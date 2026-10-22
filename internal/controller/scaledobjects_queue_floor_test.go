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

// fakeVersionQueues serves describeVersionQueues from a fixed set and counts calls.
type fakeVersionQueues struct {
	queues map[string]bool
	err    error
	calls  int
}

func (f *fakeVersionQueues) describe(context.Context, *temporaliov1alpha1.TemporalWorkerDeployment, string, string) (map[string]bool, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	out := make(map[string]bool, len(f.queues))
	for q, ok := range f.queues {
		out[q] = ok
	}
	return out, nil
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

func TestResolveMinReplicas_CurrentWithoutItsQueue(t *testing.T) {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
		WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MinReplicaCount: int32Ptr(0)},
	}}
	cases := []struct {
		name    string
		v       versionRef
		min     int32
		wantMin int64
	}{
		{"current holding its queue runs at the configured min", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent}, 0, 0},
		{"current missing its queue keeps one worker", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueNotOnVersion: true}, 0, 1},
		{"a higher configured min still wins", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueNotOnVersion: true}, 3, 3},
		{"draining is never floored", versionRef{Status: temporaliov1alpha1.VersionStatusDraining, QueueNotOnVersion: true}, 0, 0},
		{"a variant missing its queue keeps one worker", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, QueueNotOnVersion: true,
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

// The incident: the Current version holds the variant's queue but not the
// base queue. The base must keep one worker so it polls and registers its
// queue; the variant, whose queue is on the version, scales from zero as before.
func TestReconcileScaledObjects_CurrentMissingBaseQueueKeepsOneWorker(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Spec.WorkerScaling.MinReplicaCount = int32Ptr(0)
	fake := &fakeVersionQueues{queues: map[string]bool{"atlan-app-production-od": true, "atlan-app-production-spot": true}}
	r.describeVersionQueues = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	mins := soMinByTarget(t, r)
	assert.Equal(t, int64(1), mins["dep-base"], "base must hold one worker until its queue is on the version")
	assert.Equal(t, int64(0), mins["dep-od"])
	assert.Equal(t, int64(0), mins["dep-spot"])

	// The worker polled: the queue is now on the version and the floor lifts.
	fake.queues["atlan-app-production"] = true
	r.versionQueues.put(versionQueuesKey{twd: twd.UID, deployment: resolveWorkerDeployment(twd), buildID: testLongBuildID},
		&versionQueuesEntry{queues: map[string]bool{}, checkedAt: time.Now().Add(-time.Hour)})
	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	assert.Equal(t, int64(0), soMinByTarget(t, r)["dep-base"])
}

func TestReconcileScaledObjects_VariantMissingItsQueueKeepsOneWorker(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Spec.WorkerScaling.MinReplicaCount = int32Ptr(0)
	fake := &fakeVersionQueues{queues: map[string]bool{"atlan-app-production": true, "atlan-app-production-spot": true}}
	r.describeVersionQueues = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	mins := soMinByTarget(t, r)
	assert.Equal(t, int64(0), mins["dep-base"])
	assert.Equal(t, int64(1), mins["dep-od"])
	assert.Equal(t, int64(0), mins["dep-spot"])
}

// Only the Current version is checked: a Draining version gets no share of new
// work, so its queue being absent says nothing about routing.
func TestReconcileScaledObjects_OnlyCurrentIsDescribed(t *testing.T) {
	r, twd := soReconcileFixture(t)
	twd.Status.CurrentVersion.Status = temporaliov1alpha1.VersionStatusDraining
	fake := &fakeVersionQueues{queues: map[string]bool{}}
	r.describeVersionQueues = fake.describe

	require.NoError(t, r.reconcileScaledObjects(context.Background(), logr.Discard(), twd, "temporal:7233"))
	assert.Zero(t, fake.calls)
}

func TestVersionHoldsQueue_CachesHitsAndRechecksMisses(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{}
	twd.UID = types.UID("twd-1")
	twd.Namespace, twd.Name = "ns", "twd"
	fake := &fakeVersionQueues{queues: map[string]bool{}}
	r.describeVersionQueues = fake.describe
	ctx, l := context.Background(), logr.Discard()

	assert.False(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"))
	assert.False(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 1, fake.calls, "a miss is not re-described within the recheck interval")

	k := versionQueuesKey{twd: twd.UID, deployment: resolveWorkerDeployment(twd), buildID: "b1"}
	r.versionQueues.get(k).checkedAt = time.Now().Add(-2 * queueRecheckInterval)
	fake.queues["q"] = true
	assert.True(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 2, fake.calls)

	assert.True(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"))
	assert.Equal(t, 2, fake.calls, "a hit is kept without rechecking")

	// A new Current replaces the old version's entry.
	assert.True(t, r.versionHoldsQueue(ctx, l, twd, "b2", "q"))
	assert.Nil(t, r.versionQueues.get(k))

	r.versionQueues.forget(twd.UID)
	assert.Empty(t, r.versionQueues.entries)
}

// A failed describe never holds a worker up on its own, but does not discard
// a miss already observed.
func TestVersionHoldsQueue_DescribeFailure(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{}
	twd.UID = types.UID("twd-1")
	fake := &fakeVersionQueues{err: errors.New("unavailable")}
	r.describeVersionQueues = fake.describe
	ctx, l := context.Background(), logr.Discard()

	assert.True(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"), "nothing known: do not hold a worker")

	k := versionQueuesKey{twd: twd.UID, deployment: resolveWorkerDeployment(twd), buildID: "b1"}
	r.versionQueues.put(k, &versionQueuesEntry{queues: map[string]bool{}, checkedAt: time.Now().Add(-time.Hour)})
	assert.False(t, r.versionHoldsQueue(ctx, l, twd, "b1", "q"), "keep the last observed miss")
}

// Without a Temporal client (as in the other controller tests) the lookup
// answers "held", so no ScaledObject changes.
func TestVersionHoldsQueue_NoClientPool(t *testing.T) {
	r := &TemporalWorkerDeploymentReconciler{}
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{}
	assert.True(t, r.versionHoldsQueue(context.Background(), logr.Discard(), twd, "b1", "q"))
}
