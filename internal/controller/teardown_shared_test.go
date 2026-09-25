// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
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
		now := metav1.NewTime(time.Now())
		twd.DeletionTimestamp = &now
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

func ownedWorkers(twd *temporaliov1alpha1.TemporalWorkerDeployment, name, buildID string) *appsv1.Deployment {
	ctrl := true
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app-ns", Labels: map[string]string{k8s.BuildIDLabel: buildID},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: temporaliov1alpha1.GroupVersion.String(), Kind: "TemporalWorkerDeployment",
			Name: twd.Name, UID: twd.UID, Controller: &ctrl,
		}},
	}}
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

// A TWD leaving a shared Worker Deployment holds while its own build still has pinned
// work, and counts nothing but its own build: the siblings' builds stay live.
func TestReleaseSharedDeployment_HoldsOnOwnBuildOnly(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	sibling := sharedTWD("app-worker-twd", "app", false)
	c := sharedTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old"), ownedWorkers(sibling, "app-worker-twd-new", "new"))
	r := &TemporalWorkerDeploymentReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}
	q := &fakePinnedQuerier{count: 1}

	err := r.releaseSharedDeployment(context.Background(), logr.Discard(), q, leaving, "app", versions("old", "new"), false)

	require.ErrorIs(t, err, errTeardownWaiting)
	require.Len(t, q.countQueries, 1, "only this TWD's own build may be counted")
	assert.Contains(t, q.countQueries[0], `"app:old"`)
	assert.True(t, exists(t, c, "app-size-s-twd-old"), "workers stay up while their pinned work runs")
}

func TestReleaseSharedDeployment_ReleasesOnlyOwnWorkers(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	sibling := sharedTWD("app-worker-twd", "app", false)
	c := sharedTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old"), ownedWorkers(sibling, "app-worker-twd-new", "new"))
	r := &TemporalWorkerDeploymentReconciler{Client: c}

	require.NoError(t, r.releaseSharedDeployment(context.Background(), logr.Discard(), &fakePinnedQuerier{}, leaving, "app", versions("old", "new"), false))

	assert.False(t, exists(t, c, "app-size-s-twd-old"), "the leaving TWD's workers must go")
	assert.True(t, exists(t, c, "app-worker-twd-new"), "the sibling's workers must stay")
}

func TestReleaseSharedDeployment_ForcedSkipsCount(t *testing.T) {
	leaving := sharedTWD("app-size-s-twd", "app", true)
	c := sharedTestClient(t, leaving, sharedTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-old", "old"))
	r := &TemporalWorkerDeploymentReconciler{Client: c}
	q := &fakePinnedQuerier{count: 5}

	require.NoError(t, r.releaseSharedDeployment(context.Background(), logr.Discard(), q, leaving, "app", versions("old"), true))

	assert.Empty(t, q.countQueries)
	assert.False(t, exists(t, c, "app-size-s-twd-old"))
}
