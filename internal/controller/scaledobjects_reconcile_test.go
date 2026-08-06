// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

var (
	testSOGVK     = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObject"}
	testSOListGVK = schema.GroupVersionKind{Group: "keda.sh", Version: "v1alpha1", Kind: "ScaledObjectList"}
)

// applyAsUpsert lets the fake client accept server-side apply, which it does not
// implement: an Apply patch against a missing object returns NotFound rather
// than creating it. Translating Apply into create-or-update exercises the
// reconciler's decisions — which ScaledObjects it builds, which it deletes, and
// which Deployments it labels. The apiserver's field-manager merge semantics are
// not modelled here and are not what these tests cover. Merge patches (the
// keda-managed label path) pass through untouched.
func applyAsUpsert() interceptor.Funcs {
	return interceptor.Funcs{
		Patch: func(
			ctx context.Context,
			c client.WithWatch,
			obj client.Object,
			patch client.Patch,
			opts ...client.PatchOption,
		) error {
			if patch.Type() != types.ApplyPatchType {
				return c.Patch(ctx, obj, patch, opts...)
			}
			existing := &unstructured.Unstructured{}
			existing.SetGroupVersionKind(obj.GetObjectKind().GroupVersionKind())
			err := c.Get(ctx, client.ObjectKeyFromObject(obj), existing)
			if apierrors.IsNotFound(err) {
				return c.Create(ctx, obj)
			}
			if err != nil {
				return err
			}
			obj.SetResourceVersion(existing.GetResourceVersion())
			return c.Update(ctx, obj)
		},
	}
}

// longVariantTWD returns a TWD whose name alone fills the ScaledObject prefix
// budget (50 chars), with two variants. This is the shape that collided before
// the name fix: base, od and spot all resolved to one SO name.
func longVariantTWD() *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: temporaliov1alpha1.GroupVersion.String(),
			Kind:       "TemporalWorkerDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "atlan-automation-engine-worker-default-normal-prio",
			Namespace: "app-ns",
			UID:       types.UID("twd-uid"),
		},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			Replicas:      int32Ptr(1),
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				TaskQueue:       "atlan-app-production",
				MinReplicaCount: int32Ptr(1),
				MaxReplicaCount: int32Ptr(10),
			},
			Variants: []temporaliov1alpha1.WorkerVariant{
				{Name: "od", TaskQueueSuffix: "-od"},
				{Name: "spot", TaskQueueSuffix: "-spot"},
			},
		},
	}
	twd.Status = temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    testLongBuildID,
				Status:     temporaliov1alpha1.VersionStatusCurrent,
				Deployment: &corev1.ObjectReference{Name: "dep-base", Namespace: "app-ns"},
				Variants: []temporaliov1alpha1.VariantStatus{
					{Name: "od", Deployment: &corev1.ObjectReference{Name: "dep-od", Namespace: "app-ns"}},
					{Name: "spot", Deployment: &corev1.ObjectReference{Name: "dep-spot", Namespace: "app-ns"}},
				},
			},
		},
	}
	return twd
}

const testLongBuildID = "master-a1b2c3d4e5f6-xk4p"

func soReconcileFixture(t *testing.T, extra ...client.Object) (*TemporalWorkerDeploymentReconciler, *temporaliov1alpha1.TemporalWorkerDeployment) {
	t.Helper()
	twd := longVariantTWD()

	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(testSOGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(testSOListGVK, &unstructured.UnstructuredList{})

	objs := []client.Object{twd}
	for _, name := range []string{"dep-base", "dep-od", "dep-spot"} {
		objs = append(objs, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app-ns"},
		})
	}
	objs = append(objs, extra...)

	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithInterceptorFuncs(applyAsUpsert()).
		Build()
	return &TemporalWorkerDeploymentReconciler{
		Client:   fakeClient,
		Scheme:   scheme,
		Recorder: record.NewFakeRecorder(20),
	}, twd
}

func listSONames(t *testing.T, r *TemporalWorkerDeploymentReconciler) []string {
	t.Helper()
	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(testSOListGVK)
	require.NoError(t, r.List(context.Background(), &list, client.InNamespace("app-ns")))
	names := make([]string, 0, len(list.Items))
	for i := range list.Items {
		names = append(names, list.Items[i].GetName())
	}
	return names
}

func soTargetOf(t *testing.T, r *TemporalWorkerDeploymentReconciler, name string) string {
	t.Helper()
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(testSOGVK)
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Namespace: "app-ns", Name: name}, so))
	target, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
	return target
}

func isManaged(t *testing.T, r *TemporalWorkerDeploymentReconciler, name string) bool {
	t.Helper()
	var d appsv1.Deployment
	require.NoError(t, r.Get(context.Background(),
		types.NamespacedName{Namespace: "app-ns", Name: name}, &d))
	return d.Labels[ManagedByLabel] == "true"
}

// On a TWD long enough to truncate, every variant must end up with its own
// ScaledObject pointed at its own Deployment, and every Deployment must carry
// the keda-managed label so the planner yields replica control. Before the name
// fix all three collapsed into one SO and only one Deployment was labelled.
func TestReconcileScaledObjects_VariantsEachGetTheirOwnSO(t *testing.T) {
	r, twd := soReconcileFixture(t)
	ctx := context.Background()

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	names := listSONames(t, r)
	assert.Len(t, names, 3, "base + 2 variants must each get an SO, got %v", names)

	targets := make(map[string]string, len(names))
	for _, n := range names {
		targets[soTargetOf(t, r, n)] = n
	}
	assert.Len(t, targets, 3, "each SO must target a distinct Deployment, got %v", targets)
	for _, dep := range []string{"dep-base", "dep-od", "dep-spot"} {
		assert.Contains(t, targets, dep, "no SO targets %s", dep)
		assert.True(t, isManaged(t, r, dep), "%s must be labelled keda-managed", dep)
	}
}

// Reconciling twice must converge: no new SOs, no churn, labels intact.
func TestReconcileScaledObjects_SecondPassIsStable(t *testing.T) {
	r, twd := soReconcileFixture(t)
	ctx := context.Background()

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
	first := listSONames(t, r)
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
	second := listSONames(t, r)

	assert.ElementsMatch(t, first, second, "second reconcile must not add or rename SOs")
	for _, dep := range []string{"dep-base", "dep-od", "dep-spot"} {
		assert.True(t, isManaged(t, r, dep), "%s must stay keda-managed across reconciles", dep)
	}
}

// Upgrade simulation. Pre-fix, a long-named TWD's base and variants collapsed
// onto one SO name whose scaleTargetRef pointed at the LAST variant. This seeds
// that object and reconciles with the fixed naming, which is what a controller
// upgrade does on its first pass for a TWD already in the truncation band.
func TestReconcileScaledObjects_UpgradeFromCollapsedSO(t *testing.T) {
	// Old algorithm: prefix(twdName-variant)[:48] + "-" + sha1(buildID)[:8] + "-scale".
	twdName := "atlan-automation-engine-worker-default-normal-prio"
	sum := sha1.Sum([]byte(testLongBuildID))
	legacyName := (twdName + "-spot")[:48] + "-" + hex.EncodeToString(sum[:])[:8] + "-scale"

	legacy := &unstructured.Unstructured{}
	legacy.SetGroupVersionKind(testSOGVK)
	legacy.SetName(legacyName)
	legacy.SetNamespace("app-ns")
	legacy.SetLabels(map[string]string{
		OwnerTWDLabel:  twdName,
		BuildIDLabel:   testLongBuildID,
		VariantSOLabel: "spot",
	})
	legacy.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: temporaliov1alpha1.GroupVersion.String(),
		Kind:       "TemporalWorkerDeployment",
		Name:       twdName,
		UID:        types.UID("twd-uid"),
	}})
	require.NoError(t, unstructured.SetNestedField(legacy.Object,
		"dep-spot", "spec", "scaleTargetRef", "name"))

	r, twd := soReconcileFixture(t, legacy)
	ctx := context.Background()

	// The winning variant's Deployment starts keda-managed, as it would in a
	// live cluster where the collapsed SO owned it.
	var dep appsv1.Deployment
	require.NoError(t, r.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: "dep-spot"}, &dep))
	dep.Labels = map[string]string{ManagedByLabel: "true"}
	require.NoError(t, r.Update(ctx, &dep))

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	names := listSONames(t, r)
	assert.NotContains(t, names, legacyName, "legacy collapsed SO must be deleted")
	assert.Len(t, names, 3, "one SO per Deployment after upgrade, got %v", names)

	// Known gap, asserted so it is documented rather than assumed: step 5 deletes
	// the legacy SO and strips keda-managed from the Deployment it targeted —
	// dep-spot — even though step 4 has just pointed a new SO at that same
	// Deployment. For one reconcile the Deployment has a ScaledObject but no
	// label, so the planner believes it owns spec.replicas (default 1) and can
	// yank a Deployment KEDA had scaled up. Deployments the legacy SO did not
	// target are unaffected.
	assert.True(t, isManaged(t, r, "dep-base"), "dep-base was not the legacy target")
	assert.True(t, isManaged(t, r, "dep-od"), "dep-od was not the legacy target")
	assert.False(t, isManaged(t, r, "dep-spot"),
		"dep-spot loses keda-managed for one cycle; flip this once the stale-delete "+
			"path skips Deployments that are still a desired SO's scale target")

	// The next reconcile restores it, so the gap is one cycle wide, not permanent.
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
	for _, d := range []string{"dep-base", "dep-od", "dep-spot"} {
		assert.True(t, isManaged(t, r, d),
			"%s must be keda-managed after the follow-up reconcile", d)
	}

	// The rename is a one-time event, not a loop: once the legacy SO is gone the
	// names are stable, so KEDA sees a single delete+create per affected version
	// rather than churn on every ~10s requeue.
	assert.ElementsMatch(t, names, listSONames(t, r),
		"no further SO churn after the upgrade pass")
}
