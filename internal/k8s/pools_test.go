// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package k8s_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

func envValue(c corev1.Container, name string) (string, bool) {
	for _, e := range c.Env {
		if e.Name == name {
			return e.Value, true
		}
	}
	return "", false
}

func baseTWD(pools []temporaliov1alpha1.PoolSpec) *temporaliov1alpha1.TemporalWorkerDeployment {
	replicas := int32(2)
	return &temporaliov1alpha1.TemporalWorkerDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "temporal.io/v1alpha1",
			Kind:       "TemporalWorkerDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "worker",
			Namespace: "default",
		},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			Replicas: &replicas,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{
						Name:  "worker",
						Image: "example/worker:abc",
					}},
				},
			},
			WorkerOptions: temporaliov1alpha1.WorkerOptions{
				TemporalNamespace: "ns",
			},
			Pools: pools,
		},
	}
}

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(s))
	require.NoError(t, appsv1.AddToScheme(s))
	return s
}

// (a) Multi-pool produces one Deployment per pool, all sharing the SAME worker
// deployment name and build ID, with per-pool task queue + node placement.
func TestNewPoolDeployments_MultiPool(t *testing.T) {
	scheme := newScheme(t)
	onDemandReplicas := int32(1)
	cpu := resource.MustParse("2")
	w := baseTWD([]temporaliov1alpha1.PoolSpec{
		{
			Name:         "spot",
			TaskQueue:    "default-tq",
			NodeSelector: map[string]string{"pool": "spot"},
		},
		{
			Name:         "metastore",
			TaskQueue:    "metastore-tq",
			Replicas:     &onDemandReplicas,
			NodeSelector: map[string]string{"pool": "on-demand"},
			Resources: &corev1.ResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceCPU: cpu},
			},
		},
	})

	buildID := k8s.ComputeBuildID(w)
	wdName := k8s.ComputeWorkerDeploymentName(w)
	conn := temporaliov1alpha1.TemporalConnectionSpec{HostPort: "localhost:7233"}

	deployments, err := k8s.NewPoolDeploymentsWithControllerRef(w, buildID, conn, scheme)
	require.NoError(t, err)
	require.Len(t, deployments, 2, "expected one Deployment per pool")

	byPool := map[string]*appsv1.Deployment{}
	for _, d := range deployments {
		byPool[d.Labels[k8s.PoolLabel]] = d
	}
	require.Contains(t, byPool, "spot")
	require.Contains(t, byPool, "metastore")

	// All pools share the SAME build ID label and SAME deployment name env.
	for poolName, d := range byPool {
		assert.Equal(t, buildID, d.Labels[k8s.BuildIDLabel], "pool %s build ID label", poolName)
		c := d.Spec.Template.Spec.Containers[0]
		gotWDName, _ := envValue(c, "TEMPORAL_DEPLOYMENT_NAME")
		assert.Equal(t, wdName, gotWDName, "pool %s shares deployment name", poolName)
		gotBuild, _ := envValue(c, "TEMPORAL_WORKER_BUILD_ID")
		assert.Equal(t, buildID, gotBuild, "pool %s shares build ID env", poolName)
	}

	// Deployment names are distinct per pool.
	assert.NotEqual(t, byPool["spot"].Name, byPool["metastore"].Name)

	// Per-pool task queue is injected.
	spotTQ, ok := envValue(byPool["spot"].Spec.Template.Spec.Containers[0], "TEMPORAL_TASK_QUEUE")
	assert.True(t, ok)
	assert.Equal(t, "default-tq", spotTQ)
	metaTQ, _ := envValue(byPool["metastore"].Spec.Template.Spec.Containers[0], "TEMPORAL_TASK_QUEUE")
	assert.Equal(t, "metastore-tq", metaTQ)

	// Per-pool node placement + replicas + resources.
	assert.Equal(t, map[string]string{"pool": "spot"}, byPool["spot"].Spec.Template.Spec.NodeSelector)
	assert.Equal(t, map[string]string{"pool": "on-demand"}, byPool["metastore"].Spec.Template.Spec.NodeSelector)
	assert.Equal(t, int32(2), *byPool["spot"].Spec.Replicas, "spot inherits spec.replicas")
	assert.Equal(t, int32(1), *byPool["metastore"].Spec.Replicas, "metastore overrides replicas")
	assert.True(t, cpu.Equal(byPool["metastore"].Spec.Template.Spec.Containers[0].Resources.Requests[corev1.ResourceCPU]))
}

// (c) No-pools path is unchanged: a single Deployment, no pool label, no
// TEMPORAL_TASK_QUEUE env var, and a name identical to the pre-pools helper.
func TestNewPoolDeployments_NoPoolsBackwardCompatible(t *testing.T) {
	scheme := newScheme(t)
	w := baseTWD(nil)
	buildID := k8s.ComputeBuildID(w)
	conn := temporaliov1alpha1.TemporalConnectionSpec{HostPort: "localhost:7233"}

	deployments, err := k8s.NewPoolDeploymentsWithControllerRef(w, buildID, conn, scheme)
	require.NoError(t, err)
	require.Len(t, deployments, 1, "no pools => single Deployment")

	d := deployments[0]
	_, hasPoolLabel := d.Labels[k8s.PoolLabel]
	assert.False(t, hasPoolLabel, "single-pool Deployment must not carry a pool label")

	_, hasTQ := envValue(d.Spec.Template.Spec.Containers[0], "TEMPORAL_TASK_QUEUE")
	assert.False(t, hasTQ, "single-pool Deployment must not inject TEMPORAL_TASK_QUEUE")

	// Name and replicas identical to the legacy single-pool constructor.
	legacy, err := k8s.NewDeploymentWithControllerRef(w, buildID, conn, scheme)
	require.NoError(t, err)
	assert.Equal(t, legacy.Name, d.Name)
	assert.Equal(t, *w.Spec.Replicas, *d.Spec.Replicas)
}

// EffectivePools returns one implicit pool when none are configured.
func TestEffectivePools(t *testing.T) {
	noPools := baseTWD(nil)
	implicit := noPools.Spec.EffectivePools()
	require.Len(t, implicit, 1)
	assert.Equal(t, temporaliov1alpha1.ImplicitPoolName, implicit[0].Name)

	withPools := baseTWD([]temporaliov1alpha1.PoolSpec{{Name: "a"}, {Name: "b"}})
	assert.Len(t, withPools.Spec.EffectivePools(), 2)
}
