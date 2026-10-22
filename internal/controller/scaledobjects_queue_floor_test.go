// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// fakeRouting serves describeQueueRouting from a per-queue table and counts calls.
type fakeRouting struct {
	routes map[string][]queueRouting
	err    error
	calls  int
}

func (f *fakeRouting) describe(_ context.Context, _ *temporaliov1alpha1.TemporalWorkerDeployment, queue string) ([]queueRouting, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	return f.routes[queue], nil
}

// both returns the same routing for the workflow and the activity type.
func both(deployment, buildID string) []queueRouting {
	return []queueRouting{{deployment, buildID}, {deployment, buildID}}
}

// soMinByTarget returns each ScaledObject's minReplicaCount keyed by its target Deployment.
func soMinByTarget(t *testing.T, r *TemporalWorkerDeploymentReconciler) map[string]int64 {
	t.Helper()
	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(testSOListGVK)
	require.NoError(t, r.List(context.Background(), &list, client.InNamespace("app-ns")))
	out := map[string]int64{}
	for i := range list.Items {
		target, _, _ := unstructured.NestedString(list.Items[i].Object, "spec", "scaleTargetRef", "name")
		minR, found, _ := unstructured.NestedInt64(list.Items[i].Object, "spec", "minReplicaCount")
		if !found {
			minR = -1
		}
		out[target] = minR
	}
	return out
}

func TestResolveMinReplicas_CurrentWhoseQueueRoutesElsewhere(t *testing.T) {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
		WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MinReplicaCount: int32Ptr(0)},
	}}
	cases := []struct {
		name    string
		v       versionRef
		min     int32
		wantMin int64
	}{
		{"current receiving its queue runs at the configured min", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent}, 0, 0},
		{"current whose queue routes elsewhere keeps one worker", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueRoutedElsewhere: true}, 0, 1},
		{"a higher configured min still wins", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueRoutedElsewhere: true}, 3, 3},
		{"draining is never floored", versionRef{Status: temporaliov1alpha1.VersionStatusDraining, QueueRoutedElsewhere: true}, 0, 0},
		{"a variant whose queue routes elsewhere keeps one worker", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueRoutedElsewhere: true,
			Variant: &temporaliov1alpha1.WorkerVariant{Name: "od", TaskQueueSuffix: "-od"}}, 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			twd.Spec.WorkerScaling.MinReplicaCount = int32Ptr(tc.min)
			got, set := resolveMinReplicas(tc.v, twd)
			require.True(t, set)
			assert.Equal(t, tc.wantMin, got)
		})
	}
}

// The incident: after a workerDeploymentName change the Current version was
// promoted before its base worker polled, and matching still routes the base
// queue to the old worker deployment's version. The base keeps one worker so
// it polls and takes the queue over; once routing reaches the Current version
// the floor lifts. Variants whose queues route to the Current version, or to
// no version at all, scale from zero as before.
func TestReconcileScaledObjects_BaseQueueRoutedToOldDeploymentKeepsOneWorker(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Spec.WorkerScaling.MinReplicaCount = int32Ptr(0)
	dep := resolveWorkerDeployment(twd)
	fake := &fakeRouting{routes: map[string][]queueRouting{
		"atlan-app-production":      both("app-ns/old-name", "old-build"),
		"atlan-app-production-od":   both(dep, testLongBuildID),
		"atlan-app-production-spot": both("", ""),
	}}
	r.describeQueueRouting = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	mins := soMinByTarget(t, r)
	assert.Equal(t, int64(1), mins["dep-base"], "base must hold one worker while its queue routes to the old deployment")
	assert.Equal(t, int64(0), mins["dep-od"])
	assert.Equal(t, int64(0), mins["dep-spot"], "unversioned routing needs no floor: selectUnversioned counts it")

	// The worker polled: routing moved to the Current version and the floor lifts.
	fake.routes["atlan-app-production"] = both(dep, testLongBuildID)
	k := queueRoutingKey{twd: twd.UID, deployment: dep, buildID: testLongBuildID, queue: "atlan-app-production"}
	r.queueRouting.get(k).checkedAt = time.Now().Add(-2 * queueRoutingRecheckInterval)
	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	assert.Equal(t, int64(0), soMinByTarget(t, r)["dep-base"])
}

// An activity-only pool registers only the activity type, so routing of either
// type to another version counts.
func TestReconcileScaledObjects_ActivityTypeRoutedElsewhereKeepsOneWorker(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Spec.WorkerScaling.MinReplicaCount = int32Ptr(0)
	dep := resolveWorkerDeployment(twd)
	fake := &fakeRouting{routes: map[string][]queueRouting{
		"atlan-app-production":      both(dep, testLongBuildID),
		"atlan-app-production-od":   {{"", ""}, {"app-ns/old-name", "old-build"}},
		"atlan-app-production-spot": both(dep, testLongBuildID),
	}}
	r.describeQueueRouting = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	mins := soMinByTarget(t, r)
	assert.Equal(t, int64(0), mins["dep-base"])
	assert.Equal(t, int64(1), mins["dep-od"])
	assert.Equal(t, int64(0), mins["dep-spot"])
}

// Only the Current version is checked: a Draining version gets no new work.
func TestReconcileScaledObjects_OnlyCurrentIsDescribed(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Status.CurrentVersion.Status = temporaliov1alpha1.VersionStatusDraining
	fake := &fakeRouting{}
	r.describeQueueRouting = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	assert.Zero(t, fake.calls)
}

func newRoutingTestTWD() *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{}
	twd.UID = types.UID("twd-1")
	twd.Namespace, twd.Name = "ns", "twd"
	return twd
}

func TestQueueRoutedElsewhere_CachesSettledAndRechecksOthers(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	twd := newRoutingTestTWD()
	dep := resolveWorkerDeployment(twd)
	fake := &fakeRouting{routes: map[string][]queueRouting{"q": both("other/dep", "b0")}}
	r.describeQueueRouting = fake.describe
	ctx, l := context.Background(), logr.Discard()

	assert.True(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"))
	assert.True(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 1, fake.calls, "not re-described within the recheck interval")

	k := queueRoutingKey{twd: twd.UID, deployment: dep, buildID: "b1", queue: "q"}
	r.queueRouting.get(k).checkedAt = time.Now().Add(-2 * queueRoutingRecheckInterval)
	fake.routes["q"] = both(dep, "b1")
	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 2, fake.calls)

	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 2, fake.calls, "routing to this version is final for the version")

	// Unversioned routing is rechecked: another worker deployment could still take the queue.
	fake.routes["u"] = both("", "")
	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "u"))
	ku := queueRoutingKey{twd: twd.UID, deployment: dep, buildID: "b1", queue: "u"}
	assert.False(t, r.queueRouting.get(ku).checkedAt.IsZero())
	calls := fake.calls
	r.queueRouting.get(ku).checkedAt = time.Now().Add(-2 * queueRoutingRecheckInterval)
	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "u"))
	assert.Equal(t, calls, fake.calls, "unversioned routing is rechecked on the longer interval")
	r.queueRouting.get(ku).checkedAt = time.Now().Add(-2 * queueRoutingIdleRecheckInterval)
	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "u"))
	assert.Equal(t, calls+1, fake.calls)

	// A new Current drops the old version's entries.
	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b2", "u"))
	assert.Nil(t, r.queueRouting.get(k))

	r.queueRouting.forget(twd.UID)
	assert.Empty(t, r.queueRouting.entries)
}

// A failed describe never holds a worker up on its own, but keeps what was
// last observed.
func TestQueueRoutedElsewhere_DescribeFailure(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	twd := newRoutingTestTWD()
	fake := &fakeRouting{err: errors.New("unavailable")}
	r.describeQueueRouting = fake.describe
	ctx, l := context.Background(), logr.Discard()

	assert.False(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"), "nothing known: do not hold a worker")

	k := queueRoutingKey{twd: twd.UID, deployment: resolveWorkerDeployment(twd), buildID: "b1", queue: "q"}
	r.queueRouting.put(k, &queueRoutingEntry{elsewhere: true, checkedAt: time.Now().Add(-time.Hour)})
	assert.True(t, r.queueRoutedElsewhere(ctx, l, twd, "b1", "q"), "keep the last observed routing")
}

// Without a Temporal client (as in the other controller tests) nothing is floored.
func TestQueueRoutedElsewhere_NoClientPool(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	assert.False(t, r.queueRoutedElsewhere(context.Background(), logr.Discard(), newRoutingTestTWD(), "b1", "q"))
}
