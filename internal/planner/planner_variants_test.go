// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package planner

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	"github.com/temporalio/temporal-worker-controller/internal/testhelpers/testlogr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func variantDeployment(name, buildID, variant string, replicas int32) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:   name,
			Labels: map[string]string{k8s.BuildIDLabel: buildID, k8s.VariantLabel: variant},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
				"temporal.io/deployment-name": "w",
				k8s.BuildIDLabel:              buildID,
				k8s.VariantLabel:              variant,
			}},
		},
	}
}

func stateWithVariant(base *appsv1.Deployment, buildID string, variants map[string]*appsv1.Deployment) *k8s.DeploymentState {
	st := &k8s.DeploymentState{
		Deployments:           map[string]*appsv1.Deployment{},
		DeploymentRefs:        map[string]*corev1.ObjectReference{},
		VariantDeployments:    map[string]map[string]*appsv1.Deployment{},
		VariantDeploymentRefs: map[string]map[string]*corev1.ObjectReference{},
	}
	if base != nil {
		st.Deployments[buildID] = base
		st.DeploymentRefs[buildID] = k8s.NewObjectRef(base)
	}
	if len(variants) > 0 {
		st.VariantDeployments[buildID] = variants
		st.VariantDeploymentRefs[buildID] = map[string]*corev1.ObjectReference{}
		for n, d := range variants {
			st.VariantDeploymentRefs[buildID][n] = k8s.NewObjectRef(d)
		}
	}
	return st
}

func specWithVariants(names ...string) *temporaliov1alpha1.TemporalWorkerDeploymentSpec {
	spec := &temporaliov1alpha1.TemporalWorkerDeploymentSpec{
		WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{TaskQueue: "q"},
	}
	for _, n := range names {
		spec.Variants = append(spec.Variants, temporaliov1alpha1.WorkerVariant{Name: n, TaskQueueSuffix: "-" + n})
	}
	return spec
}

func TestGetCreateVariants(t *testing.T) {
	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: "v1"},
		},
	}
	baseVariantMode := variantDeployment("w-v1", "v1", k8s.BaseVariantName, 1)
	baseLegacy := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "w-v1", Labels: map[string]string{k8s.BuildIDLabel: "v1"}},
		Spec: appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{
			"temporal.io/deployment-name": "w", k8s.BuildIDLabel: "v1",
		}}},
	}

	t.Run("no variants declared", func(t *testing.T) {
		assert.Nil(t, getCreateVariants(stateWithVariant(baseVariantMode, "v1", nil), status, &temporaliov1alpha1.TemporalWorkerDeploymentSpec{}, false))
	})
	t.Run("base being created this cycle -> create all variants", func(t *testing.T) {
		got := getCreateVariants(stateWithVariant(nil, "v1", nil), status, specWithVariants("od"), true)
		assert.Equal(t, []string{"od"}, got)
	})
	t.Run("variant-mode base exists, variant missing -> create", func(t *testing.T) {
		got := getCreateVariants(stateWithVariant(baseVariantMode, "v1", nil), status, specWithVariants("od"), false)
		assert.Equal(t, []string{"od"}, got)
	})
	t.Run("variant already exists -> nothing", func(t *testing.T) {
		st := stateWithVariant(baseVariantMode, "v1", map[string]*appsv1.Deployment{"od": variantDeployment("w-od-v1", "v1", "od", 1)})
		assert.Nil(t, getCreateVariants(st, status, specWithVariants("od"), false))
	})
	t.Run("legacy base (immutable selector) -> skip until next rollout", func(t *testing.T) {
		assert.Nil(t, getCreateVariants(stateWithVariant(baseLegacy, "v1", nil), status, specWithVariants("od"), false))
	})
	t.Run("no base and not creating -> nothing", func(t *testing.T) {
		assert.Nil(t, getCreateVariants(stateWithVariant(nil, "v1", nil), status, specWithVariants("od"), false))
	})
}

func TestGetDeleteDeploymentsCascadesAndOrphans(t *testing.T) {
	drainedAt := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	scaledown := metav1.Duration{Duration: time.Minute}
	deleteDelay := metav1.Duration{Duration: time.Minute}
	spec := specWithVariants("od")
	spec.SunsetStrategy = temporaliov1alpha1.SunsetStrategy{ScaledownDelay: &scaledown, DeleteDelay: &deleteDelay}

	base := variantDeployment("w-v0", "v0", k8s.BaseVariantName, 0)
	od := variantDeployment("w-od-v0", "v0", "od", 0)
	st := stateWithVariant(base, "v0", map[string]*appsv1.Deployment{"od": od})

	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: "v1"},
		},
		DeprecatedVersions: []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "v0",
				Status:     temporaliov1alpha1.VersionStatusDrained,
				Deployment: k8s.NewObjectRef(base),
			},
			DrainedSince: &drainedAt,
			// Required by the drained-delete gate since the sunset prune change:
			// drained AND no active base replicas.
			EligibleForDeletion: true,
		}},
	}

	t.Run("sunset cascades to the version's variants", func(t *testing.T) {
		got := getDeleteDeployments(st, status, spec, true, "ns/w")
		names := map[string]bool{}
		for _, d := range got {
			names[d.Name] = true
		}
		assert.True(t, names["w-v0"], "base must be deleted")
		assert.True(t, names["w-od-v0"], "variant must be deleted with its version")
	})

	t.Run("variant removed from spec is orphan-swept even on a live version", func(t *testing.T) {
		liveBase := variantDeployment("w-v1", "v1", k8s.BaseVariantName, 1)
		gpu := variantDeployment("w-gpu-v1", "v1", "gpu", 1)
		stLive := stateWithVariant(liveBase, "v1", map[string]*appsv1.Deployment{"gpu": gpu})
		liveStatus := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
			TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: "v1"},
			},
		}
		got := getDeleteDeployments(stLive, liveStatus, specWithVariants("od"), true, "ns/w")
		require.Len(t, got, 1)
		assert.Equal(t, "w-gpu-v1", got[0].Name)
	})

	t.Run("variant whose base is gone is orphan-swept", func(t *testing.T) {
		orphan := variantDeployment("w-od-vold", "vold", "od", 0)
		stOrphan := stateWithVariant(nil, "", nil)
		stOrphan.VariantDeployments["vold"] = map[string]*appsv1.Deployment{"od": orphan}
		got := getDeleteDeployments(stOrphan, &temporaliov1alpha1.TemporalWorkerDeploymentStatus{}, specWithVariants("od"), true, "ns/w")
		require.Len(t, got, 1)
		assert.Equal(t, "w-od-vold", got[0].Name)
	})
}

func TestGetScaleDeploymentsVariantsToZero(t *testing.T) {
	drainedAt := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	scaledown := metav1.Duration{Duration: time.Minute}
	deleteDelay := metav1.Duration{Duration: 24 * time.Hour}
	spec := specWithVariants("od")
	spec.SunsetStrategy = temporaliov1alpha1.SunsetStrategy{ScaledownDelay: &scaledown, DeleteDelay: &deleteDelay}

	base := variantDeployment("w-v0", "v0", k8s.BaseVariantName, 1)
	od := variantDeployment("w-od-v0", "v0", "od", 1)
	kedaOd := variantDeployment("w-keda-v0", "v0", "keda", 1)
	kedaOd.Labels[kedaManagedLabel] = "true"
	st := stateWithVariant(base, "v0", map[string]*appsv1.Deployment{"od": od, "keda": kedaOd})

	status := &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: "v1"},
		},
		DeprecatedVersions: []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "v0",
				Status:     temporaliov1alpha1.VersionStatusDrained,
				Deployment: k8s.NewObjectRef(base),
			},
			DrainedSince: &drainedAt,
		}},
	}

	got := getScaleDeployments(st, status, spec)
	scaledNames := map[string]uint32{}
	for ref, n := range got {
		scaledNames[ref.Name] = n
	}
	assert.Equal(t, uint32(0), scaledNames["w-v0"], "base scaled to zero")
	assert.Equal(t, uint32(0), scaledNames["w-od-v0"], "variant scaled to zero with its version")
	_, kedaScaled := scaledNames["w-keda-v0"]
	assert.False(t, kedaScaled, "KEDA-managed variant left to its ScaledObject")
}

func TestCheckAndUpdateVariantPodTemplateSpec(t *testing.T) {
	variant := &temporaliov1alpha1.WorkerVariant{
		Name:            "od",
		TaskQueueSuffix: "-od",
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1Gi")},
		},
	}
	spec := &temporaliov1alpha1.TemporalWorkerDeploymentSpec{
		WorkerOptions: temporaliov1alpha1.WorkerOptions{UnsafeCustomBuildID: "main-abc", TemporalNamespace: "default"},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "w", Image: "app:v1"}}},
		},
		Variants: []temporaliov1alpha1.WorkerVariant{*variant},
	}
	conn := temporaliov1alpha1.TemporalConnectionSpec{HostPort: "temporal:7233"}

	current := variantDeployment("w-od-main-abc", "main-abc", "od", 1)
	current.Spec.Template.Annotations = map[string]string{
		k8s.PodTemplateSpecHashAnnotation: k8s.ComputePodTemplateSpecHash(spec.Template),
		k8s.VariantSpecHashAnnotation:     k8s.ComputeVariantSpecHash(variant),
		k8s.ConnectionSpecHashAnnotation:  k8s.ComputeConnectionSpecHash(conn),
	}
	current.Spec.Template.Spec = *spec.Template.Spec.DeepCopy()

	t.Run("no drift -> nil", func(t *testing.T) {
		assert.Nil(t, checkAndUpdateVariantPodTemplateSpec(current.DeepCopy(), spec, conn, variant))
	})

	t.Run("variant delta drift -> rebuild applies new delta", func(t *testing.T) {
		changed := variant.DeepCopy()
		changed.Resources = &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("2Gi")},
		}
		d := current.DeepCopy()
		got := checkAndUpdateVariantPodTemplateSpec(d, spec, conn, changed)
		require.NotNil(t, got)
		assert.Equal(t, "2Gi", got.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String())
		assert.Equal(t, k8s.ComputeVariantSpecHash(changed), got.Spec.Template.Annotations[k8s.VariantSpecHashAnnotation])
	})

	t.Run("shared template drift -> rebuild", func(t *testing.T) {
		spec2 := spec.DeepCopy()
		spec2.Template.Spec.Containers[0].Image = "app:v2"
		d := current.DeepCopy()
		got := checkAndUpdateVariantPodTemplateSpec(d, spec2, conn, variant)
		require.NotNil(t, got)
		assert.Equal(t, "app:v2", got.Spec.Template.Spec.Containers[0].Image)
	})

	t.Run("auto build ID -> no drift path", func(t *testing.T) {
		autoSpec := spec.DeepCopy()
		autoSpec.WorkerOptions.UnsafeCustomBuildID = ""
		assert.Nil(t, checkAndUpdateVariantPodTemplateSpec(current.DeepCopy(), autoSpec, conn, variant))
	})
}

func TestGateWaitsForExpectedVariantQueues(t *testing.T) {
	gate := temporaliov1alpha1.RolloutStrategy{
		Strategy: temporaliov1alpha1.UpdateAllAtOnce,
		Gate:     &temporaliov1alpha1.GateWorkflowConfig{WorkflowType: "GateWf"},
	}
	healthy := metav1.Now()
	completedWf := func(q string) temporaliov1alpha1.WorkflowExecution {
		return temporaliov1alpha1.WorkflowExecution{
			WorkflowID: "gate-" + q, RunID: "r", TaskQueue: q,
			Status: temporaliov1alpha1.WorkflowExecutionStatusCompleted,
		}
	}
	mkStatus := func(queues []string, wfs []temporaliov1alpha1.WorkflowExecution) *temporaliov1alpha1.TemporalWorkerDeploymentStatus {
		var tqs []temporaliov1alpha1.TaskQueue
		for _, q := range queues {
			tqs = append(tqs, temporaliov1alpha1.TaskQueue{Name: q})
		}
		return &temporaliov1alpha1.TemporalWorkerDeploymentStatus{
			TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID: "v2", Status: temporaliov1alpha1.VersionStatusInactive,
					HealthySince: &healthy, TaskQueues: tqs,
				},
				TestWorkflows: wfs,
			},
			CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID: "v1", Status: temporaliov1alpha1.VersionStatusCurrent,
				},
			},
		}
	}
	state := &temporal.TemporalWorkerState{Versions: map[string]*temporal.VersionInfo{}}
	cfg := &Config{RolloutStrategy: gate, ExpectedGateQueues: []string{"q", "q-od"}}

	t.Run("od queue not yet registered -> gate blocks even with base gate wf done", func(t *testing.T) {
		st := mkStatus([]string{"q"}, []temporaliov1alpha1.WorkflowExecution{completedWf("q")})
		assert.Nil(t, getVersionConfigDiff(testlogr.New(t), st, state, cfg, "ns/w"))
	})
	t.Run("both queues registered, od gate wf missing -> blocks", func(t *testing.T) {
		st := mkStatus([]string{"q", "q-od"}, []temporaliov1alpha1.WorkflowExecution{completedWf("q")})
		assert.Nil(t, getVersionConfigDiff(testlogr.New(t), st, state, cfg, "ns/w"))
	})
	t.Run("both queues + both gate wfs completed -> promotes", func(t *testing.T) {
		st := mkStatus([]string{"q", "q-od"}, []temporaliov1alpha1.WorkflowExecution{completedWf("q"), completedWf("q-od")})
		vcfg := getVersionConfigDiff(testlogr.New(t), st, state, cfg, "ns/w")
		require.NotNil(t, vcfg)
		assert.True(t, vcfg.SetCurrent)
	})
	t.Run("no expected queues configured -> legacy behavior", func(t *testing.T) {
		legacy := &Config{RolloutStrategy: gate}
		st := mkStatus([]string{"q"}, []temporaliov1alpha1.WorkflowExecution{completedWf("q")})
		assert.NotNil(t, getVersionConfigDiff(testlogr.New(t), st, state, legacy, "ns/w"))
	})
}

func TestExpectedGateQueues(t *testing.T) {
	assert.Nil(t, ExpectedGateQueues(&temporaliov1alpha1.TemporalWorkerDeploymentSpec{}))
	spec := specWithVariants("od")
	spec.Variants = append(spec.Variants, temporaliov1alpha1.WorkerVariant{Name: "standby"}) // empty suffix: same queue
	assert.Equal(t, []string{"q", "q-od"}, ExpectedGateQueues(spec))
}
