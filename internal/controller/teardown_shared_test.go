// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func sharedTWD(name, deploymentName string, deleting bool) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app-ns", UID: types.UID(name + "-uid")},
	}
	twd.Spec.WorkerOptions.TemporalNamespace = "default"
	twd.Spec.WorkerOptions.WorkerDeploymentName = deploymentName
	if deleting {
		deleted := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		twd.DeletionTimestamp = &deleted
		twd.Finalizers = []string{finalizerName}
	}
	return twd
}

func sharedTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&temporaliov1alpha1.TemporalWorkerDeployment{}).
		WithIndex(&appsv1.Deployment{}, deployOwnerKey, func(rawObj client.Object) []string {
			if owner := metav1.GetControllerOf(rawObj.(*appsv1.Deployment)); owner != nil {
				return []string{owner.Name}
			}
			return nil
		}).
		Build()
}

func TestSharesWorkerDeployment(t *testing.T) {
	cases := []struct {
		name    string
		sibling *temporaliov1alpha1.TemporalWorkerDeployment
		want    bool
	}{
		{"live sibling on the same deployment", sharedTWD("app-heavy-twd", "app", false), true},
		{"sibling that is also being deleted", sharedTWD("app-heavy-twd", "app", true), false},
		{"sibling on another deployment", sharedTWD("other-worker-twd", "other", false), false},
		{"sibling on its own default deployment", sharedTWD("app-heavy-twd", "", false), false},
		{"sibling in another Temporal namespace", func() *temporaliov1alpha1.TemporalWorkerDeployment {
			twd := sharedTWD("app-heavy-twd", "app", false)
			twd.Spec.WorkerOptions.TemporalNamespace = "other"
			return twd
		}(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deleting := sharedTWD("app-size-s-twd", "app", true)
			r := &TemporalWorkerDeploymentReconciler{Client: sharedTestClient(t, deleting, tc.sibling)}
			got, err := r.sharesWorkerDeployment(context.Background(), deleting)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

func ownedWorkers(twd *temporaliov1alpha1.TemporalWorkerDeployment, name, buildID, registeredAs string) *appsv1.Deployment {
	ctrl := true
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app-ns", Labels: map[string]string{k8s.BuildIDLabel: buildID},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: temporaliov1alpha1.GroupVersion.String(), Kind: "TemporalWorkerDeployment",
			Name: twd.Name, UID: twd.UID, Controller: &ctrl,
		}},
	}}
	container := corev1.Container{Name: "worker"}
	if registeredAs != "" {
		container.Env = []corev1.EnvVar{{Name: k8s.TemporalDeploymentNameEnvVar, Value: registeredAs}}
	}
	d.Spec.Template.Spec.Containers = []corev1.Container{container}
	return d
}

func exists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var d appsv1.Deployment
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: name}, &d)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func sharedReconciler(c client.Client) *TemporalWorkerDeploymentReconciler {
	return &TemporalWorkerDeploymentReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}
}

func release(t *testing.T, r *TemporalWorkerDeploymentReconciler, q pinnedExecutionQuerier, twd *temporaliov1alpha1.TemporalWorkerDeployment) error {
	t.Helper()
	versions, err := r.ownVersions(context.Background(), twd)
	require.NoError(t, err)
	return r.releaseSharedDeployment(context.Background(), logr.Discard(), q, twd, versions)
}

// Siblings on one release share the build, so the leaving TWD's version is also theirs. Any open
// execution pinned to it may still schedule work on this TWD's queue, so the TWD holds for it.
func TestReleaseSharedDeployment_HoldsOnOwnBuild(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	sibling := sharedTWD("app-worker-twd", "app", false)
	c := sharedTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"), ownedWorkers(sibling, "app-worker-twd-v1", "v1", "app"))
	q := &fakePinnedQuerier{count: 1}

	err := release(t, sharedReconciler(c), q, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	require.Len(t, q.countQueries, 1)
	assert.Equal(t, pinnedExecutionQuery("app", "v1"), q.countQueries[0])
	assert.True(t, exists(t, c, "app-size-s-twd-v1"), "workers stay up while their pinned work runs")
}

func TestReleaseSharedDeployment_ReleasesOnlyOwnWorkers(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	sibling := sharedTWD("app-worker-twd", "app", false)
	c := sharedTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"), ownedWorkers(sibling, "app-worker-twd-new", "new", "app"))

	require.NoError(t, release(t, sharedReconciler(c), &fakePinnedQuerier{}, leaving))

	assert.False(t, exists(t, c, "app-size-s-twd-old"), "the leaving TWD's workers must go")
	assert.True(t, exists(t, c, "app-worker-twd-new"), "the sibling's workers must stay")
}

// Pinned work is counted under the name the workers registered with, so workers left over
// from a workerDeploymentName change are not released while their pinned work still runs.
// A base Deployment and its variant on the same build are one version and one query.
func TestReleaseSharedDeployment_CountsEachPolledVersionOnce(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	c := sharedTestClient(t, leaving, sharedTWD("app-worker-twd", "app", false),
		ownedWorkers(leaving, "app-size-s-twd-new", "new", ""),
		ownedWorkers(leaving, "app-size-s-twd-od-new", "new", "app"),
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "old-app"))
	q := &fakePinnedQuerier{}

	require.NoError(t, release(t, sharedReconciler(c), q, leaving))

	assert.ElementsMatch(t, []string{pinnedExecutionQuery("app", "new"), pinnedExecutionQuery("old-app", "old")}, q.countQueries)
}

// Right after the deletion, an execution may not be pinned or indexed yet, so zero is not trusted.
func TestReleaseSharedDeployment_ZeroRightAfterDeletionHolds(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	just := metav1.NewTime(time.Now().Add(-5 * time.Second))
	leaving.DeletionTimestamp = &just
	c := sharedTestClient(t, leaving, sharedTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))

	err := release(t, sharedReconciler(c), &fakePinnedQuerier{}, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	assert.True(t, exists(t, c, "app-size-s-twd-v1"))
}

func TestReleaseSharedDeployment_CountFailureHolds(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	c := sharedTestClient(t, leaving, sharedTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"))

	err := release(t, sharedReconciler(c), &fakePinnedQuerier{countErr: errors.New("visibility unavailable")}, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	assert.True(t, exists(t, c, "app-size-s-twd-old"))
}

// A shared TWD whose workers are already gone has nothing to wait for, so it is released without
// a Temporal client: an unreachable server must not hold it for the whole drainage budget.
func TestHandleDeletion_SharedWithoutWorkersNeedsNoTemporal(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 72 * time.Hour}
	c := sharedTestClient(t, leaving, sharedTWD("app-worker-twd", "app", false))

	require.NoError(t, sharedReconciler(c).handleDeletion(context.Background(), logr.Discard(), leaving))
}

// Past the drainage budget a shared TWD only deletes its own workers. It needs no Temporal
// client for that, and it must not terminate executions pinned to builds its siblings also run.
func TestHandleDeletion_SharedPastBudgetNeedsNoTemporal(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: time.Minute}
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	leaving.DeletionTimestamp = &past
	sibling := sharedTWD("app-worker-twd", "app", false)
	c := sharedTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"), ownedWorkers(sibling, "app-worker-twd-new", "new", "app"))
	r := sharedReconciler(c) // no TemporalClientPool: any Temporal call would panic

	require.NoError(t, r.handleDeletion(context.Background(), logr.Discard(), leaving))

	assert.False(t, exists(t, c, "app-size-s-twd-old"))
	assert.True(t, exists(t, c, "app-worker-twd-new"))
}
