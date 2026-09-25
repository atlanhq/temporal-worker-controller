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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

// A TWD leaving a shared Worker Deployment must release its own workers without
// touching Temporal: the reconciler has no Temporal client here, so any server
// call would panic.
func TestHandleDeletion_SharedDeploymentReleasesOnlyOwnWorkers(t *testing.T) {
	deleting := sharedTWD("app-size-s-twd", "app", true)
	sibling := sharedTWD("app-worker-twd", "app", false)
	ctrl := true
	owned := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "app-size-s-twd-v1", Namespace: "app-ns",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: temporaliov1alpha1.GroupVersion.String(), Kind: "TemporalWorkerDeployment",
			Name: deleting.Name, UID: deleting.UID, Controller: &ctrl,
		}},
	}}
	siblings := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: "app-worker-twd-v1", Namespace: "app-ns",
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: temporaliov1alpha1.GroupVersion.String(), Kind: "TemporalWorkerDeployment",
			Name: sibling.Name, UID: sibling.UID, Controller: &ctrl,
		}},
	}}
	r := &TemporalWorkerDeploymentReconciler{Client: sharedTestClient(t, deleting, sibling, owned, siblings)}

	require.NoError(t, r.handleDeletion(context.Background(), logr.Discard(), deleting))

	var d appsv1.Deployment
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: owned.Name}, &d)
	assert.True(t, apierrors.IsNotFound(err), "the deleted TWD's own workers must go")
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: siblings.Name}, &d),
		"the sibling's workers must stay")
}
