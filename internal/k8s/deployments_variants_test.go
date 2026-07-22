// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package k8s_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/testhelpers"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func ownedDeployment(name, buildID, variant string) *appsv1.Deployment {
	labels := map[string]string{k8s.BuildIDLabel: buildID}
	if variant != "" {
		labels[k8s.VariantLabel] = variant
	}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "default",
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "temporal.io/v1alpha1",
				Kind:       "TemporalWorkerDeployment",
				Name:       "test-worker",
				UID:        types.UID("test-owner-uid"),
				Controller: func() *bool { b := true; return &b }(),
			}},
		},
	}
}

// TestGetDeploymentStateVariants is the central no-collision test: a base and a
// variant Deployment of the SAME buildID must land in separate maps instead of
// overwriting each other in the buildID-keyed base map.
func TestGetDeploymentStateVariants(t *testing.T) {
	ctx := context.Background()
	owner := &temporaliov1alpha1.TemporalWorkerDeployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "temporal.io/v1alpha1", Kind: "TemporalWorkerDeployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "test-worker", Namespace: "default", UID: types.UID("test-owner-uid")},
	}
	base := ownedDeployment("worker-v1", "v1", k8s.BaseVariantName)
	od := ownedDeployment("worker-od-v1", "v1", "od")
	legacyBase := ownedDeployment("worker-v0", "v0", "") // pre-variant Deployment, no variant label

	scheme := runtime.NewScheme()
	_ = appsv1.AddToScheme(scheme)
	_ = corev1.AddToScheme(scheme)
	_ = temporaliov1alpha1.AddToScheme(scheme)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(owner, base, od, legacyBase).
		WithIndex(&appsv1.Deployment{}, k8s.DeployOwnerKey, func(rawObj client.Object) []string {
			deploy := rawObj.(*appsv1.Deployment)
			ownerRef := metav1.GetControllerOf(deploy)
			if ownerRef == nil {
				return nil
			}
			return []string{ownerRef.Name}
		}).
		Build()

	state, err := k8s.GetDeploymentState(ctx, fakeClient, "default", "test-worker", "test-worker")
	require.NoError(t, err)

	// Base map holds exactly the base + legacy deployments, no collision with the od variant.
	assert.Equal(t, 2, len(state.Deployments))
	assert.Equal(t, "worker-v1", state.Deployments["v1"].Name)
	assert.Equal(t, "worker-v0", state.Deployments["v0"].Name)

	// Variant map holds the od twin under (buildID, variant).
	require.NotNil(t, state.VariantDeployments["v1"])
	assert.Equal(t, "worker-od-v1", state.VariantDeployments["v1"]["od"].Name)
	assert.Equal(t, "worker-od-v1", state.VariantDeploymentRefs["v1"]["od"].Name)
	assert.Empty(t, state.VariantDeployments["v0"])
}

func variantSpecTWD() *temporaliov1alpha1.TemporalWorkerDeployment {
	pod := testhelpers.MakePodSpec([]corev1.Container{{
		Name:  "worker",
		Image: "example/app:v1",
		Env: []corev1.EnvVar{
			{Name: "ATLAN_DEPLOYMENT_NAME", Value: "production"},
			{Name: "OTHER", Value: "keep"},
		},
	}}, map[string]string{"app.kubernetes.io/component": "worker"}, "")
	twd := testhelpers.MakeTWD("my-app-worker-twd", "my-app", 1, pod, nil, nil, nil)
	twd.Spec.WorkerScaling = &temporaliov1alpha1.WorkerScalingConfig{TaskQueue: "atlan-my-app-production"}
	twd.Spec.Variants = []temporaliov1alpha1.WorkerVariant{{
		Name:             "od",
		TaskQueueSuffix:  "-od",
		EnvValueSuffixes: []string{"ATLAN_DEPLOYMENT_NAME"},
		NodeSelector:     map[string]string{"purpose": "workflows"},
		Tolerations:      []corev1.Toleration{{Key: "purpose", Operator: corev1.TolerationOpEqual, Value: "workflows", Effect: corev1.TaintEffectNoSchedule}},
		Resources: &corev1.ResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")},
		},
	}}
	return twd
}

// TestVariantDeploymentGolden compares the base and od-variant Deployments of a
// variant-declaring TWD field by field.
func TestVariantDeploymentGolden(t *testing.T) {
	twd := variantSpecTWD()
	conn := temporaliov1alpha1.TemporalConnectionSpec{HostPort: "temporal:7233"}
	wdn := k8s.ComputeWorkerDeploymentName(twd)

	base := k8s.NewDeploymentWithOwnerRef(&twd.TypeMeta, &twd.ObjectMeta, &twd.Spec, wdn, "bid1", conn)
	od := k8s.NewVariantDeploymentWithOwnerRef(&twd.TypeMeta, &twd.ObjectMeta, &twd.Spec, wdn, "bid1", conn, &twd.Spec.Variants[0])

	// Disjoint selectors, shared version-independent deployment-name label.
	assert.Equal(t, k8s.BaseVariantName, base.Spec.Selector.MatchLabels[k8s.VariantLabel])
	assert.Equal(t, "od", od.Spec.Selector.MatchLabels[k8s.VariantLabel])
	assert.Equal(t, base.Spec.Selector.MatchLabels["temporal.io/deployment-name"],
		od.Spec.Selector.MatchLabels["temporal.io/deployment-name"])
	assert.Equal(t, "bid1", od.Spec.Selector.MatchLabels[k8s.BuildIDLabel])
	assert.NotEqual(t, base.Name, od.Name)
	assert.Contains(t, od.Name, "-od-")

	// Delta applied: nodeSelector/tolerations/resources replaced on the variant only.
	assert.Empty(t, base.Spec.Template.Spec.NodeSelector)
	assert.Equal(t, map[string]string{"purpose": "workflows"}, od.Spec.Template.Spec.NodeSelector)
	assert.Len(t, od.Spec.Template.Spec.Tolerations, 1)
	assert.Equal(t, "512Mi", od.Spec.Template.Spec.Containers[0].Resources.Requests.Memory().String())

	// Env value suffix applied to the named env only.
	envByName := func(d *appsv1.Deployment) map[string]string {
		out := map[string]string{}
		for _, e := range d.Spec.Template.Spec.Containers[0].Env {
			out[e.Name] = e.Value
		}
		return out
	}
	baseEnv, odEnv := envByName(base), envByName(od)
	assert.Equal(t, "production", baseEnv["ATLAN_DEPLOYMENT_NAME"])
	assert.Equal(t, "production-od", odEnv["ATLAN_DEPLOYMENT_NAME"])
	assert.Equal(t, "keep", odEnv["OTHER"])

	// Identical version registration: same TEMPORAL_DEPLOYMENT_NAME + build ID env.
	assert.Equal(t, baseEnv[k8s.TemporalDeploymentNameEnvVar], odEnv[k8s.TemporalDeploymentNameEnvVar])
	assert.Equal(t, baseEnv["TEMPORAL_WORKER_BUILD_ID"], odEnv["TEMPORAL_WORKER_BUILD_ID"])
	assert.Equal(t, "bid1", odEnv["TEMPORAL_WORKER_BUILD_ID"])

	// Variant drift hash recorded; base carries none.
	assert.NotEmpty(t, od.Spec.Template.Annotations[k8s.VariantSpecHashAnnotation])
	assert.Empty(t, base.Spec.Template.Annotations[k8s.VariantSpecHashAnnotation])
	// Shared user-template hash (drift on the shared template rolls both).
	assert.Equal(t, base.Spec.Template.Annotations[k8s.PodTemplateSpecHashAnnotation],
		od.Spec.Template.Annotations[k8s.PodTemplateSpecHashAnnotation])
}

// TestNoVariantsSelectorUnchanged is the flag-off regression gate at the builder
// level: without spec.variants the selector must be exactly the historical pair.
func TestNoVariantsSelectorUnchanged(t *testing.T) {
	twd := testhelpers.MakeTWD("plain", "default", 1, testhelpers.MakePodSpec([]corev1.Container{{Name: "w"}}, nil, ""), nil, nil, nil)
	conn := temporaliov1alpha1.TemporalConnectionSpec{HostPort: "temporal:7233"}
	d := k8s.NewDeploymentWithOwnerRef(&twd.TypeMeta, &twd.ObjectMeta, &twd.Spec, "default/plain", "bid1", conn)
	assert.Equal(t, map[string]string{
		"temporal.io/deployment-name": "plain",
		k8s.BuildIDLabel:              "bid1",
	}, d.Spec.Selector.MatchLabels)
	_, hasVariant := d.Spec.Selector.MatchLabels[k8s.VariantLabel]
	assert.False(t, hasVariant)
}
