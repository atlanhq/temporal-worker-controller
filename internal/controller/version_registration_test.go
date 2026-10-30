// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package controller

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var (
	wf  = enumspb.TASK_QUEUE_TYPE_WORKFLOW
	act = enumspb.TASK_QUEUE_TYPE_ACTIVITY
)

// fakeTemporalVersions serves describeVersionQueues from per-build queue lists, and registers a
// queue on a build when pollVersionQueue is called for it, as matching does at poll start.
type fakeTemporalVersions struct {
	mu       sync.Mutex
	versions map[string][]versionQueue
	polls    []versionQueue
	pollErr  error
	noAdd    bool
}

func (f *fakeTemporalVersions) describe(_ context.Context, _ *temporaliov1alpha1.TemporalWorkerDeployment, _, buildID string) ([]versionQueue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	qs, ok := f.versions[buildID]
	if !ok {
		return nil, serviceerror.NewNotFound("version not found")
	}
	return append([]versionQueue(nil), qs...), nil
}

func (f *fakeTemporalVersions) poll(ctx context.Context, _ *temporaliov1alpha1.TemporalWorkerDeployment, _, buildID string, q versionQueue) error {
	f.mu.Lock()
	f.polls = append(f.polls, q)
	if !f.noAdd {
		f.versions[buildID] = append(f.versions[buildID], q)
	}
	err := f.pollErr
	f.mu.Unlock()
	if err != nil {
		return err
	}
	<-ctx.Done() // a poll that registered waits until it is cancelled or times out
	return nil
}

// registrationFixture is an opted-in TWD with a base pool on "q" and an "od" variant on "q-od",
// Current build "b1" (both pools at zero workers) and target build "b2" not yet registered.
func registrationFixture(t *testing.T, extra ...client.Object) (*TemporalWorkerDeploymentReconciler, *record.FakeRecorder, *temporaliov1alpha1.TemporalWorkerDeployment, *fakeTemporalVersions) {
	t.Helper()
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns", UID: types.UID("twd-uid"),
			Annotations: map[string]string{RegistrationAnnotation: "true"}},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{TaskQueue: "q", MinReplicaCount: int32Ptr(0)},
			Variants:      []temporaliov1alpha1.WorkerVariant{{Name: "od", TaskQueueSuffix: "-od"}},
		},
	}
	twd.Spec.WorkerOptions.TemporalNamespace = "default"
	ref := func(name string) *corev1.ObjectReference { return &corev1.ObjectReference{Namespace: "ns", Name: name} }
	twd.Status.CurrentVersion = &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
		BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
			BuildID: "b1", Status: temporaliov1alpha1.VersionStatusCurrent, Deployment: ref("app-b1"),
			Variants: []temporaliov1alpha1.VariantStatus{{Name: "od", Deployment: ref("app-od-b1")}},
		},
	}
	twd.Status.TargetVersion.BuildID = "b2"
	twd.Status.TargetVersion.Status = temporaliov1alpha1.VersionStatusNotRegistered

	deploy := func(name string, replicas int32) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
			Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
	}
	objs := append([]client.Object{twd, deploy("app-b1", 0), deploy("app-od-b1", 0)}, extra...)
	r, rec := newTestReconciler(objs)
	rec.Events = make(chan string, 50)
	f := &fakeTemporalVersions{versions: map[string][]versionQueue{
		"b1": {{"q", wf}, {"q", act}, {"q-od", act}},
	}}
	r.describeVersionQueues = f.describe
	r.pollVersionQueue = f.poll
	return r, rec, twd, f
}

func hasEvent(events []string, reason string) bool {
	for _, e := range events {
		if strings.Contains(e, reason) {
			return true
		}
	}
	return false
}

func TestResolveMinReplicas_ControllerRegisteredTargetHasNoFloor(t *testing.T) {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
		WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{MinReplicaCount: int32Ptr(0)},
	}}
	cases := []struct {
		name    string
		v       versionRef
		min     int32
		wantMin int64
	}{
		{"registering target is floored without the controller", versionRef{IsTarget: true, Status: temporaliov1alpha1.VersionStatusNotRegistered}, 0, 1},
		{"not-registered target the controller registers starts at zero", versionRef{IsTarget: true, ControllerRegisters: true, Status: temporaliov1alpha1.VersionStatusNotRegistered}, 0, 0},
		{"inactive target the controller registers starts at zero", versionRef{IsTarget: true, ControllerRegisters: true, Status: temporaliov1alpha1.VersionStatusInactive}, 0, 0},
		{"a configured min still applies", versionRef{IsTarget: true, ControllerRegisters: true, Status: temporaliov1alpha1.VersionStatusInactive}, 2, 2},
		{"a ramping version keeps its floor", versionRef{IsTarget: true, ControllerRegisters: true, Status: temporaliov1alpha1.VersionStatusRamping}, 0, 1},
		{"the queue-routing floor is unchanged", versionRef{Status: temporaliov1alpha1.VersionStatusCurrent, ControllerRegisters: true, QueueRoutedElsewhere: true}, 0, 1},
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

// The controller polls each queue/type the Current version holds, once, with the target build, and
// covers both pools; promotion is not held once every queue registered.
func TestRegisterTargetVersion_RegistersEveryCurrentQueue(t *testing.T) {
	r, rec, twd, f := registrationFixture(t)

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.ElementsMatch(t, []versionQueue{{"q", wf}, {"q", act}, {"q-od", act}}, f.polls)
	assert.ElementsMatch(t, f.versions["b1"], f.versions["b2"])
	assert.True(t, r.registration.registers(twd.UID, "b2", ""))
	assert.True(t, r.registration.registers(twd.UID, "b2", "od"))
	assert.False(t, r.registration.registers(twd.UID, "b1", ""), "only the target build is covered")
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
	assert.True(t, hasEvent(drainEvents(rec), ReasonVersionRegistered))

	// Registered: the next reconcile makes no Temporal calls.
	f.polls = nil
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.Empty(t, f.polls)
}

// Only queues that are still missing are polled.
func TestRegisterTargetVersion_PollsOnlyMissingQueues(t *testing.T) {
	r, _, twd, f := registrationFixture(t)
	f.versions["b2"] = []versionQueue{{"q", wf}, {"q", act}}

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.Equal(t, []versionQueue{{"q-od", act}}, f.polls)
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
}

// A pool whose Current Deployment has workers keeps its warm worker: the new worker will have work.
func TestRegisterTargetVersion_BusyPoolKeepsItsWorker(t *testing.T) {
	r, _, twd, f := registrationFixture(t)
	var busy appsv1.Deployment
	require.NoError(t, r.Get(context.Background(), types.NamespacedName{Namespace: "ns", Name: "app-b1"}, &busy))
	busy.Spec.Replicas = int32Ptr(2)
	require.NoError(t, r.Update(context.Background(), &busy))
	f.noAdd = true // nothing but the busy pool's worker would register its queue

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.Equal(t, []versionQueue{{"q-od", act}}, f.polls)
	assert.False(t, r.registration.registers(twd.UID, "b2", ""))
	assert.True(t, r.registration.registers(twd.UID, "b2", "od"))
	assert.True(t, r.registration.holdPromotion(twd.UID, "b2"), "promotion waits for the base worker to register its queue")
}

// A pool whose queue the Current version doesn't hold (a new pool) keeps its warm worker, and the
// controller does not hold promotion for a queue the Current version never had.
func TestRegisterTargetVersion_NewPoolKeepsItsWorker(t *testing.T) {
	r, _, twd, f := registrationFixture(t)
	f.versions["b1"] = []versionQueue{{"q", wf}, {"q", act}}

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.True(t, r.registration.registers(twd.UID, "b2", ""))
	assert.False(t, r.registration.registers(twd.UID, "b2", "od"))
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
}

// A sibling TWD on the same worker deployment targeting the same build holds promotion until its
// own queue registers; the server alone would promote while that queue is idle.
func TestRegisterTargetVersion_HoldsForSiblingQueues(t *testing.T) {
	sibling := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "heavy", Namespace: "ns", UID: types.UID("sibling-uid")},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			WorkerScaling: &temporaliov1alpha1.WorkerScalingConfig{TaskQueue: "q-heavy"},
		},
	}
	sibling.Spec.WorkerOptions.TemporalNamespace = "default"
	sibling.Status.TargetVersion.BuildID = "b2"
	r, _, twd, f := registrationFixture(t, sibling)
	shared := "ns/shared"
	twd.Spec.WorkerOptions.WorkerDeploymentName = shared
	require.NoError(t, r.Update(context.Background(), twd))
	twd.Status.TargetVersion.BuildID = "b2"
	twd.Status.TargetVersion.Status = temporaliov1alpha1.VersionStatusNotRegistered
	twd.Status.CurrentVersion.BuildID = "b1"
	sibling.Spec.WorkerOptions.WorkerDeploymentName = shared
	require.NoError(t, r.Update(context.Background(), sibling))
	require.NoError(t, r.Status().Update(context.Background(), sibling))
	f.versions["b1"] = append(f.versions["b1"], versionQueue{"q-heavy", act})

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.NotContains(t, f.polls, versionQueue{"q-heavy", act}, "a TWD polls only its own queues")
	assert.True(t, r.registration.holdPromotion(twd.UID, "b2"))

	f.versions["b2"] = append(f.versions["b2"], versionQueue{"q-heavy", act}) // the sibling registered
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
}

// A task handed to a registration poll is reported; the pool stays covered.
func TestRegisterTargetVersion_TaskReceivedIsReported(t *testing.T) {
	r, rec, twd, f := registrationFixture(t)
	f.pollErr = errRegistrationTaskReceived

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.True(t, hasEvent(drainEvents(rec), ReasonRegistrationTaskReceived))
}

// A registration that does not complete within the grace hands each pool back to its warm worker,
// keeps holding promotion until the workers register, and stops polling.
func TestRegisterTargetVersion_FallsBackAfterGrace(t *testing.T) {
	r, rec, twd, f := registrationFixture(t)
	f.noAdd = true
	f.pollErr = errors.New("matching unavailable")

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	require.True(t, r.registration.registers(twd.UID, "b2", ""), "inside the grace the pools stay at zero")
	assert.True(t, r.registration.holdPromotion(twd.UID, "b2"))

	e := r.registration.get(twd.UID)
	e.since = time.Now().Add(-2 * registrationGrace)
	e.checkedAt = time.Now().Add(-2 * registrationRetryInterval)
	r.registration.put(twd.UID, e)
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)

	assert.False(t, r.registration.registers(twd.UID, "b2", ""))
	assert.False(t, r.registration.registers(twd.UID, "b2", "od"))
	assert.True(t, r.registration.holdPromotion(twd.UID, "b2"))
	assert.True(t, hasEvent(drainEvents(rec), ReasonRegistrationFellBack))

	f.polls = nil
	f.versions["b2"] = f.versions["b1"] // the workers registered
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.Empty(t, f.polls, "no polling after the fallback")
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
}

// A failed attempt is retried on the retry interval, not every reconcile.
func TestRegisterTargetVersion_RetriesOnInterval(t *testing.T) {
	r, _, twd, f := registrationFixture(t)
	f.noAdd = true
	f.pollErr = errors.New("matching unavailable")

	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	n := len(f.polls)
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.Equal(t, n, len(f.polls))
}

func TestRegisterTargetVersion_SkippedCases(t *testing.T) {
	cases := map[string]func(*temporaliov1alpha1.TemporalWorkerDeployment){
		"not opted in": func(twd *temporaliov1alpha1.TemporalWorkerDeployment) {
			delete(twd.Annotations, RegistrationAnnotation)
		},
		"no KEDA scaling":        func(twd *temporaliov1alpha1.TemporalWorkerDeployment) { twd.Spec.WorkerScaling = nil },
		"no current version":     func(twd *temporaliov1alpha1.TemporalWorkerDeployment) { twd.Status.CurrentVersion = nil },
		"target already current": func(twd *temporaliov1alpha1.TemporalWorkerDeployment) { twd.Status.TargetVersion.BuildID = "b1" },
		"target ramping": func(twd *temporaliov1alpha1.TemporalWorkerDeployment) {
			twd.Status.TargetVersion.Status = temporaliov1alpha1.VersionStatusRamping
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			r, _, twd, f := registrationFixture(t)
			mutate(twd)
			r.registerTargetVersion(context.Background(), logr.Discard(), twd)
			assert.Empty(t, f.polls)
			assert.Nil(t, r.registration.get(twd.UID))
		})
	}
}

// A describe failure before any pools were chosen leaves every pool on its warm worker.
func TestRegisterTargetVersion_DescribeFailureKeepsWorkers(t *testing.T) {
	r, _, twd, _ := registrationFixture(t)
	r.describeVersionQueues = func(context.Context, *temporaliov1alpha1.TemporalWorkerDeployment, string, string) ([]versionQueue, error) {
		return nil, errors.New("unavailable")
	}
	r.registerTargetVersion(context.Background(), logr.Discard(), twd)
	assert.False(t, r.registration.registers(twd.UID, "b2", ""))
	assert.False(t, r.registration.holdPromotion(twd.UID, "b2"))
}

// The constructor shares one label map between the Deployment and its selector; marking the
// Deployment KEDA-managed must leave the selector, which has to match the pod labels, as it was.
func TestStartAtZero(t *testing.T) {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "ns"},
		Spec: temporaliov1alpha1.TemporalWorkerDeploymentSpec{
			Variants: []temporaliov1alpha1.WorkerVariant{{Name: "od", TaskQueueSuffix: "-od"}},
		},
	}
	for _, variant := range []*temporaliov1alpha1.WorkerVariant{nil, &twd.Spec.Variants[0]} {
		d := k8s.NewVariantDeploymentWithOwnerRef(&twd.TypeMeta, &twd.ObjectMeta, &twd.Spec, "ns/app", "b2",
			temporaliov1alpha1.TemporalConnectionSpec{}, variant)
		if variant == nil {
			d = k8s.NewDeploymentWithOwnerRef(&twd.TypeMeta, &twd.ObjectMeta, &twd.Spec, "ns/app", "b2",
				temporaliov1alpha1.TemporalConnectionSpec{})
		}
		before := map[string]string{}
		for k, v := range d.Spec.Selector.MatchLabels {
			before[k] = v
		}

		startAtZero(d)

		require.NotNil(t, d.Spec.Replicas)
		assert.Equal(t, int32(0), *d.Spec.Replicas)
		assert.Equal(t, "true", d.Labels[ManagedByLabel])
		assert.Equal(t, before, d.Spec.Selector.MatchLabels, "the selector must not change")
		for k, v := range d.Spec.Selector.MatchLabels {
			assert.Equal(t, v, d.Spec.Template.Labels[k], "pod labels must still match the selector")
		}
	}
}

func TestPollEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	boom := errors.New("boom")
	assert.Equal(t, boom, pollEnd(ctx, boom))
	cancel()
	assert.NoError(t, pollEnd(ctx, boom), "a poll ended by its own cancel or deadline is finished, not failed")
}
