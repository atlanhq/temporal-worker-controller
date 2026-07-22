// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestTeardownChildren: deletion must remove the TWD's owned ScaledObjects and
// child Deployments (the deadlock fix: children at >0 replicas keep polling, the
// server refuses version deletion with "active pollers", and GC cannot remove
// the children because the delete-protection finalizer blocks their owner).
// Non-owned resources must be untouched, and the call must be idempotent.
func TestTeardownChildren(t *testing.T) {
	soGVK := schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"}
	soListGVK := schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObjectList"}

	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-worker-twd",
			Namespace: "app-ns",
			UID:       types.UID("twd-uid"),
		},
	}
	ownedDeploy := func(name string) *appsv1.Deployment {
		ctrl := true
		return &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "app-ns",
				Labels:    map[string]string{k8s.BuildIDLabel: "v1"},
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: temporaliov1alpha1.GroupVersion.String(),
					Kind:       "TemporalWorkerDeployment",
					Name:       twd.Name,
					UID:        twd.UID,
					Controller: &ctrl,
				}},
			},
		}
	}
	bystander := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: "app-ns"},
	}
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(soGVK)
	so.SetName("app-worker-twd-v1-scale")
	so.SetNamespace("app-ns")
	so.SetLabels(map[string]string{OwnerTWDLabel: twd.Name, BuildIDLabel: "v1"})
	so.SetOwnerReferences([]metav1.OwnerReference{
		*metav1.NewControllerRef(twd, temporaliov1alpha1.GroupVersion.WithKind("TemporalWorkerDeployment")),
	})

	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(soGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(soListGVK, &unstructured.UnstructuredList{})

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(twd, ownedDeploy("app-worker-twd-v1"), ownedDeploy("app-worker-twd-od-v1"), bystander, so).
		WithIndex(&appsv1.Deployment{}, deployOwnerKey, func(rawObj client.Object) []string {
			deploy := rawObj.(*appsv1.Deployment)
			owner := metav1.GetControllerOf(deploy)
			if owner == nil {
				return nil
			}
			return []string{owner.Name}
		}).
		Build()
	r := &TemporalWorkerDeploymentReconciler{Client: fakeClient, Scheme: scheme}

	require.NoError(t, r.teardownChildren(context.Background(), logr.Discard(), twd))

	// Owned children gone.
	var d appsv1.Deployment
	err := r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: "app-worker-twd-v1"}, &d)
	assert.True(t, apierrors.IsNotFound(err), "owned base deployment must be deleted")
	err = r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: "app-worker-twd-od-v1"}, &d)
	assert.True(t, apierrors.IsNotFound(err), "owned variant deployment must be deleted")

	// Bystander survives.
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: "unrelated"}, &d))

	// Owned SO gone.
	got := &unstructured.Unstructured{}
	got.SetGroupVersionKind(soGVK)
	err = r.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: "app-worker-twd-v1-scale"}, got)
	assert.True(t, apierrors.IsNotFound(err), "owned ScaledObject must be deleted")

	// Idempotent on retry (empty lists).
	require.NoError(t, r.teardownChildren(context.Background(), logr.Discard(), twd))
}
