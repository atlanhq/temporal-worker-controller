// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// A buildID is cleaned for label-value syntax, which permits upper case, "_"
// and "." — none of them legal in an object name. Interpolated straight into
// the name, any of them made the apiserver reject the apply, so that version
// got no ScaledObject at all and its Deployment ran unautoscaled.
func TestScaledObjectNameIsAlwaysAValidObjectName(t *testing.T) {
	for _, tc := range []struct{ name, twd, variant, buildID string }{
		{"plain", "app-worker-twd", "", "main-319dd01"},
		{"dotted tag", "app-worker-twd", "", "v1.2.3-319dd01"},
		{"underscored branch", "app-worker-twd", "", "feature_x-319dd01"},
		{"capitalised", "app-worker-twd", "", "Main-319dd01"},
		{"both", "app-worker-twd", "od", "Release_2.0-319dd01"},
		{"truncated", "a-very-long-application-name-indeed-worker-twd-xx", "od", "Release_2.0-0123456789012345678901234567890"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ScaledObjectName(tc.twd, tc.variant, tc.buildID)
			assert.Empty(t, validation.IsDNS1123Label(got), "%q is not a valid object name", got)
			assert.LessOrEqual(t, len(got), scaledObjectMaxNameLen)
			assert.Equal(t, got, ScaledObjectName(tc.twd, tc.variant, tc.buildID), "must be deterministic")
		})
	}
}

// The readable form joins variant and buildID with "-", and both may contain
// "-", so the boundary is ambiguous: an "od" variant of main-319dd01 and a base
// built from a branch named "od-main" compose the same name. The name function
// cannot break the tie on its own — this test pins the ambiguity so the
// reconciler's duty to resolve it is not optimised away.
func TestScaledObjectNameIsNotInjective(t *testing.T) {
	base := ScaledObjectName("app-worker-twd", "", "od-main-319dd01")
	variant := ScaledObjectName("app-worker-twd", "od", "main-319dd01")
	require.Equal(t, base, variant, "the delimiter ambiguity is the premise of this test")

	// Hashing the whole triple separates them again.
	hb := disambiguateScaledObjectName("app-worker-twd", "", "od-main-319dd01")
	hv := disambiguateScaledObjectName("app-worker-twd", "od", "main-319dd01")
	assert.NotEqual(t, hb, hv, "hashed names must distinguish the triples")
	assert.Empty(t, validation.IsDNS1123Label(hb))
	assert.Empty(t, validation.IsDNS1123Label(hv))
	assert.LessOrEqual(t, len(hb), scaledObjectMaxNameLen)
	assert.Equal(t, hb, disambiguateScaledObjectName("app-worker-twd", "", "od-main-319dd01"))
}

// collidingTWD has a short name (so names stay in the readable path) and two
// versions whose build IDs differ by exactly the "od-" prefix, which is what
// makes the Current version's od variant and the Target version's base compose
// one name.
func collidingTWD() *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: temporaliov1alpha1.GroupVersion.String(),
			Kind:       "TemporalWorkerDeployment",
		},
		ObjectMeta: metav1.ObjectMeta{Name: "app-worker-twd", Namespace: "app-ns", UID: types.UID("twd-uid")},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			Replicas:      int32Ptr(1),
			WorkerOptions: temporaliov1alpha1.WorkerOptions{TemporalNamespace: "default"},
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{
				TaskQueue:       "atlan-app-production",
				MinReplicaCount: int32Ptr(1),
				MaxReplicaCount: int32Ptr(10),
			},
			Variants: []temporaliov1alpha1.WorkerVariant{{Name: "od", TaskQueueSuffix: "-od"}},
		},
	}
	twd.Status = temporaliov1alpha1.TemporalWorkerDeploymentStatus{
		CurrentVersion: &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "main-319dd01",
				Status:     temporaliov1alpha1.VersionStatusCurrent,
				Deployment: &corev1.ObjectReference{Name: "dep-cur", Namespace: "app-ns"},
				Variants: []temporaliov1alpha1.VariantStatus{
					{Name: "od", Deployment: &corev1.ObjectReference{Name: "dep-cur-od", Namespace: "app-ns"}},
				},
			},
		},
		TargetVersion: temporaliov1alpha1.TargetWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
				BuildID:    "od-main-319dd01",
				Status:     temporaliov1alpha1.VersionStatusRamping,
				Deployment: &corev1.ObjectReference{Name: "dep-tgt", Namespace: "app-ns"},
				Variants: []temporaliov1alpha1.VariantStatus{
					{Name: "od", Deployment: &corev1.ObjectReference{Name: "dep-tgt-od", Namespace: "app-ns"}},
				},
			},
		},
	}
	return twd
}

func collisionFixture(t *testing.T) (*TemporalWorkerDeploymentReconciler, *temporaliov1alpha1.TemporalWorkerDeployment, *record.FakeRecorder) {
	t.Helper()
	twd := collidingTWD()

	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(testSOGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(testSOListGVK, &unstructured.UnstructuredList{})

	objs := []client.Object{twd}
	for _, n := range collisionDeployments {
		objs = append(objs, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: "app-ns"}})
	}
	rec := record.NewFakeRecorder(20)
	return &TemporalWorkerDeploymentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithInterceptorFuncs(applyAsUpsert()).Build(),
		Scheme:   scheme,
		Recorder: rec,
	}, twd, rec
}

var collisionDeployments = []string{"dep-cur", "dep-cur-od", "dep-tgt", "dep-tgt-od"}

// Every version keeps its own scaler even when two of them compose one readable
// name. Before the fix the later entry overwrote the earlier one in the desired
// map: one Deployment was left without an SO and without the keda-managed
// label, so the planner held it at static replicas and it never scaled on
// backlog.
func TestReconcileScaledObjects_CollidingNamesStillEachGetAnSO(t *testing.T) {
	r, twd, rec := collisionFixture(t)
	ctx := context.Background()

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	names := listSONames(t, r)
	assert.Len(t, names, 4, "2 versions x (base + od) must each get an SO, got %v", names)

	targets := map[string]string{}
	for _, n := range names {
		targets[soTargetOf(t, r, n)] = n
	}
	assert.Len(t, targets, 4, "each SO must target a distinct Deployment, got %v", targets)
	for _, dep := range collisionDeployments {
		assert.Contains(t, targets, dep, "no SO targets %s", dep)
		assert.True(t, isManaged(t, r, dep), "%s must be labelled keda-managed", dep)
	}

	// The collision is resolved, not hidden: operators still get an event.
	var sawEvent bool
	for len(rec.Events) > 0 {
		if ev := <-rec.Events; assert.NotEmpty(t, ev) && strings.Contains(ev, ReasonScaledObjectNameCollision) {
			sawEvent = true
		}
	}
	assert.True(t, sawEvent, "a collision must still raise %s", ReasonScaledObjectNameCollision)
}

// Resolution must be order-independent and idempotent: a second reconcile
// neither renames nor adds anything.
func TestReconcileScaledObjects_CollisionResolutionIsStable(t *testing.T) {
	r, twd, _ := collisionFixture(t)
	ctx := context.Background()

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
	first := listSONames(t, r)

	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
	second := listSONames(t, r)

	assert.ElementsMatch(t, first, second, "second pass must not churn names")
	for _, dep := range collisionDeployments {
		assert.True(t, isManaged(t, r, dep), "%s lost its keda-managed label", dep)
	}
}

// Base and variant SOs of one version must be distinguishable by label alone.
// Owner+buildID matches both, so without a discriminator on the base the only
// way to select it is a negated selector.
func TestScaledObjectLabels_BaseIsSelectable(t *testing.T) {
	r, twd, _ := collisionFixture(t)
	ctx := context.Background()
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(testSOListGVK)
	require.NoError(t, r.List(ctx, &list, client.InNamespace("app-ns"),
		client.MatchingLabels{OwnerTWDLabel: "app-worker-twd", BuildIDLabel: "main-319dd01"}))
	assert.Len(t, list.Items, 2, "owner+buildID alone matches the base and its variant")

	for _, sel := range []struct {
		variant string
		target  string
	}{{"base", "dep-cur"}, {"od", "dep-cur-od"}} {
		var one unstructured.UnstructuredList
		one.SetGroupVersionKind(testSOListGVK)
		require.NoError(t, r.List(ctx, &one, client.InNamespace("app-ns"),
			client.MatchingLabels{
				OwnerTWDLabel:  "app-worker-twd",
				BuildIDLabel:   "main-319dd01",
				VariantSOLabel: sel.variant,
			}))
		require.Len(t, one.Items, 1, "variant=%q must select exactly one SO", sel.variant)
		got, _, _ := unstructured.NestedString(one.Items[0].Object, "spec", "scaleTargetRef", "name")
		assert.Equal(t, sel.target, got)
	}
}
