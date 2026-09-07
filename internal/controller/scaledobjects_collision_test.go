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
	hb := disambiguateScaledObjectName("app-worker-twd", "", "od-main-319dd01", 0)
	hv := disambiguateScaledObjectName("app-worker-twd", "od", "main-319dd01", 0)
	assert.NotEqual(t, hb, hv, "hashed names must distinguish the triples")
	assert.Empty(t, validation.IsDNS1123Label(hb))
	assert.Empty(t, validation.IsDNS1123Label(hv))
	assert.LessOrEqual(t, len(hb), scaledObjectMaxNameLen)
	assert.Equal(t, hb, disambiguateScaledObjectName("app-worker-twd", "", "od-main-319dd01", 0))
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
	r, rec := soFixtureFor(t, twd, collisionDeployments...)
	return r, twd, rec
}

func soFixtureFor(
	t *testing.T,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	depNames ...string,
) (*TemporalWorkerDeploymentReconciler, *record.FakeRecorder) {
	t.Helper()

	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	scheme.AddKnownTypeWithName(testSOGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(testSOListGVK, &unstructured.UnstructuredList{})

	objs := []client.Object{twd}
	for _, n := range depNames {
		objs = append(objs, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: "app-ns"}})
	}
	rec := record.NewFakeRecorder(200)
	return &TemporalWorkerDeploymentReconciler{
		Client: fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).
			WithInterceptorFuncs(applyAsUpsert()).Build(),
		Scheme:   scheme,
		Recorder: rec,
	}, rec
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

// --- Defects found in the second sweep ---------------------------------------

// Normalising the name to a DNS label is itself a collision source: two image
// tags differing only by case or by "." vs "_" now compose one readable name
// where before they composed two (one of them invalid). This needs no variant
// at all, so it is more reachable than the variant/buildID boundary case.
func TestScaledObjectNameNormalisationFoldsTags(t *testing.T) {
	for _, pair := range [][2]string{
		{"v1_0-319dd01", "v1.0-319dd01"},
		{"Main-319dd01", "main-319dd01"},
		{"feature_x-319dd01", "feature.x-319dd01"},
	} {
		t.Run(pair[0]+" vs "+pair[1], func(t *testing.T) {
			a := ScaledObjectName("app-worker-twd", "", pair[0])
			b := ScaledObjectName("app-worker-twd", "", pair[1])
			require.Equal(t, a, b, "premise: normalisation folds these two tags")

			// The fallback must still separate them, or one version loses its
			// scaler. It hashes the raw triple, so the fold does not reach it.
			assert.NotEqual(t,
				disambiguateScaledObjectName("app-worker-twd", "", pair[0], 0),
				disambiguateScaledObjectName("app-worker-twd", "", pair[1], 0))
		})
	}
}

// The fallback name must never equal the readable name it is replacing.
// Unsalted, a base version's hashed name was byte-identical to its own
// truncated readable name — both budget 48 chars of prefix over the same digest
// input — so "fall back to the hash" was a no-op for exactly the versions long
// enough to truncate, and the allocator could not break their tie.
func TestDisambiguateNeverReturnsTheReadableName(t *testing.T) {
	longBuildID := "main-0123456789012345678901234567890123456789"
	for _, twdName := range []string{
		"app-worker-twd",
		"a-very-long-application-name-indeed-worker-twd-xx",
		"a-very-long-application-name-indeed-worker-twd-xxyyzz",
	} {
		for _, variant := range []string{"", "od"} {
			readable := ScaledObjectName(twdName, variant, longBuildID)
			require.Greater(t, len(twdName+"-"+longBuildID+"-scale"), scaledObjectMaxNameLen,
				"premise: this input truncates")
			assert.NotEqual(t, readable,
				disambiguateScaledObjectName(twdName, variant, longBuildID, 0),
				"fallback for twd=%q variant=%q must differ from the readable name", twdName, variant)
		}
	}
}

// KEDA copies the ScaledObject name into the label value
// `scaledobject.keda.sh/name`, and a label value is capped at 63 characters.
// Our 63-char name budget therefore fits with exactly zero headroom: raising it
// would silently break KEDA's own labelling and its HPA lookups.
func TestScaledObjectMaxNameLenFitsAKedaLabelValue(t *testing.T) {
	assert.LessOrEqual(t, scaledObjectMaxNameLen, 63,
		"KEDA writes the SO name into a label value, which cannot exceed 63 chars")
}

// twdWithVersions builds a short-named TWD (so names stay in the readable path)
// carrying one `od` variant, with a Current version and optionally a Target.
func twdWithVersions(currentBuildID, targetBuildID string) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := collidingTWD()
	twd.Status.CurrentVersion.BuildID = currentBuildID
	if targetBuildID == "" {
		twd.Status.TargetVersion = temporaliov1alpha1.TargetWorkerDeploymentVersion{}
		return twd
	}
	twd.Status.TargetVersion.BuildID = targetBuildID
	return twd
}

// A converged, serving tier must not be renamed just because an unrelated new
// version collided with its name. Renaming both contenders was deterministic
// but cost the incumbent a full reconcile with no HPA, mid-rollout. The
// incumbent keeps the name; only the newcomer moves.
func TestReconcileScaledObjects_IncumbentKeepsItsName(t *testing.T) {
	ctx := context.Background()

	// Phase 1: only the Current version exists. Its od variant converges on the
	// readable name that the Target version's base will later also want.
	twd := twdWithVersions("main-319dd01", "")
	r, _ := soFixtureFor(t, twd, "dep-cur", "dep-cur-od")
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	incumbent := ScaledObjectName("app-worker-twd", "od", "main-319dd01")
	require.Equal(t, incumbent, soNameTargeting(t, r, "dep-cur-od"),
		"premise: the od variant holds the contested readable name")

	// Phase 2: the colliding Target arrives. Re-list so the reconciler sees the
	// live SOs, exactly as it would on the next requeue.
	twd.Status.TargetVersion = collidingTWD().Status.TargetVersion
	for _, n := range []string{"dep-tgt", "dep-tgt-od"} {
		require.NoError(t, r.Create(ctx, &appsv1.Deployment{
			ObjectMeta: metav1.ObjectMeta{Name: n, Namespace: "app-ns"}}))
	}
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	assert.Equal(t, incumbent, soNameTargeting(t, r, "dep-cur-od"),
		"the incumbent must keep its name and its scaler through the collision")
	assert.True(t, isManaged(t, r, "dep-cur-od"), "incumbent must stay keda-managed")
}

// The invariant that makes a permanent freeze impossible: every enumerated
// version ends up with exactly one ScaledObject aimed at its own Deployment,
// and every such Deployment is keda-managed. Skipping a version left its
// Deployment labelled keda-managed with no scaler, so the planner yielded
// replica control to an HPA that did not exist and replicas froze forever.
func TestReconcileScaledObjects_EveryVersionAlwaysGetsAnSO(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct{ name, current, target string }{
		{"no collision", "main-319dd01", "main-b07509f"},
		{"variant boundary", "main-319dd01", "od-main-319dd01"},
		{"normalisation fold", "v1_0-319dd01", "v1.0-319dd01"},
		{"case fold", "Main-319dd01", "main-319dd01"},
		{"target equals a hash shape", "main-319dd01", "4180e1f2"},
		{"long names truncate", "main-0123456789012345678901234567890123456789", "od-main-0123456789012345678901234567890123456789"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twd := twdWithVersions(tc.current, tc.target)
			r, _ := soFixtureFor(t, twd, collisionDeployments...)
			require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

			names := listSONames(t, r)
			assert.Len(t, names, len(collisionDeployments),
				"one SO per version, got %v", names)

			targets := map[string]string{}
			for _, n := range names {
				targets[soTargetOf(t, r, n)] = n
			}
			for _, dep := range collisionDeployments {
				assert.Contains(t, targets, dep, "no SO targets %s", dep)
				assert.True(t, isManaged(t, r, dep), "%s must be keda-managed", dep)
			}
		})
	}
}

// A collision that has converged must stop raising events. The condition is
// stable and already resolved, so re-raising it every ~10s requeue is pure
// apiserver write load — the exact cost ARUN-894 was about.
func TestReconcileScaledObjects_ConvergedCollisionStopsEventing(t *testing.T) {
	ctx := context.Background()
	twd := collidingTWD()
	r, rec := soFixtureFor(t, twd, collisionDeployments...)

	counts := make([]int, 0, 4)
	for pass := 0; pass < 4; pass++ {
		require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))
		counts = append(counts, drainCollisionEvents(rec))
	}

	assert.Positive(t, counts[0], "the first pass must surface the collision")
	assert.Zero(t, counts[len(counts)-1],
		"a converged collision must stop eventing, got per-pass counts %v", counts)
}

// soNameTargeting returns the name of the single SO whose scaleTargetRef points
// at deployName, or "" when none does.
func soNameTargeting(t *testing.T, r *TemporalWorkerDeploymentReconciler, deployName string) string {
	t.Helper()
	for _, n := range listSONames(t, r) {
		if soTargetOf(t, r, n) == deployName {
			return n
		}
	}
	return ""
}

func drainCollisionEvents(rec *record.FakeRecorder) int {
	n := 0
	for {
		select {
		case ev := <-rec.Events:
			if strings.Contains(ev, ReasonScaledObjectNameCollision) {
				n++
			}
		default:
			return n
		}
	}
}

// The permanent-freeze case, constructed deterministically. A version can hold
// the readable name that ANOTHER version's fallback name resolves to, because
// contention is detected over readable names while the final name space also
// contains fallback names. Without a retry the fallback silently overwrote the
// innocent holder in the desired map: its Deployment kept the keda-managed
// label with no ScaledObject behind it, so the planner yielded replica control
// to an HPA that did not exist and replicas froze indefinitely.
//
// The colliding buildID is derived from the implementation rather than
// hardcoded, so the test keeps testing this shape if the digest ever changes.
func TestReconcileScaledObjects_FallbackNameNeverStealsAHeldName(t *testing.T) {
	ctx := context.Background()
	const twdName = "app-worker-twd"

	// The Target's base collides with the Current version's od variant, so the
	// Target must fall back. Take the 8-hex body of that fallback name and hand
	// it to a third version as its buildID, so that version's READABLE name is
	// exactly the Target's fallback name.
	fallback := disambiguateScaledObjectName(twdName, "", "od-main-319dd01", 0)
	held := strings.TrimSuffix(strings.TrimPrefix(fallback, twdName+"-"), scaledObjectSuffix)
	require.Len(t, held, 8, "expected an 8-hex fallback body, got %q from %q", held, fallback)
	require.Equal(t, fallback, ScaledObjectName(twdName, "", held),
		"premise: a version with buildID %q composes the Target's fallback name", held)

	twd := collidingTWD()
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{{
		BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
			BuildID:    held,
			Status:     temporaliov1alpha1.VersionStatusInactive,
			Deployment: &corev1.ObjectReference{Name: "dep-held", Namespace: "app-ns"},
		},
	}}

	deps := append([]string{"dep-held"}, collisionDeployments...)
	r, _ := soFixtureFor(t, twd, deps...)
	require.NoError(t, r.reconcileScaledObjects(ctx, logr.Discard(), twd, "temporal:7233"))

	names := listSONames(t, r)
	assert.Len(t, names, len(deps), "every version needs its own SO, got %v", names)
	for _, dep := range deps {
		assert.NotEmpty(t, soNameTargeting(t, r, dep), "no SO targets %s", dep)
		assert.True(t, isManaged(t, r, dep), "%s must be keda-managed", dep)
	}
}
