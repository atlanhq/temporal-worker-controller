// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// deletingTWD returns a TWD deleted deletedAgo in the past with the given drainage
// budget. A nil budget leaves spec.sunset.teardownDrainageTimeout unset.
func deletingTWD(deletedAgo time.Duration, budget *time.Duration) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := makeTWD("app-worker", "app-ns", "conn")
	deletedAt := metav1.NewTime(time.Now().Add(-deletedAgo))
	twd.DeletionTimestamp = &deletedAt
	if budget != nil {
		twd.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: *budget}
	}
	return twd
}

func durPtr(d time.Duration) *time.Duration { return &d }

func versions(buildIDs ...string) []sdkclient.WorkerDeploymentVersionSummary {
	out := make([]sdkclient.WorkerDeploymentVersionSummary, 0, len(buildIDs))
	for _, id := range buildIDs {
		out = append(out, sdkclient.WorkerDeploymentVersionSummary{
			Version: sdkworker.WorkerDeploymentVersion{BuildID: id},
		})
	}
	return out
}

// TestTeardownBudgetExpired: the budget decides whether a teardown may wait at all.
// It is derived from deletionTimestamp rather than tracked in status, so every
// answer has to hold with no state carried between reconciles.
func TestTeardownBudgetExpired(t *testing.T) {
	for _, tc := range []struct {
		name string
		twd  *temporaliov1alpha1.TemporalWorkerDeployment
		want bool
	}{
		{
			name: "unset budget does not wait",
			twd:  deletingTWD(time.Minute, nil),
			want: true,
		},
		{
			name: "zero budget opts out of waiting",
			twd:  deletingTWD(time.Minute, durPtr(0)),
			want: true,
		},
		{
			name: "within budget waits",
			twd:  deletingTWD(time.Minute, durPtr(15*time.Minute)),
			want: false,
		},
		{
			name: "past budget stops waiting",
			twd:  deletingTWD(20*time.Minute, durPtr(15*time.Minute)),
			want: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, teardownBudgetExpired(tc.twd))
		})
	}
}

// TestTeardownBudgetExpired_NotDeleting: with no deletionTimestamp there is no
// deadline to measure against, so the wait must not be entered at all.
func TestTeardownBudgetExpired_NotDeleting(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	twd.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 15 * time.Minute}

	assert.True(t, teardownBudgetExpired(twd))
}

// TestCountPinnedExecutions_SumsAcrossVersions: the gate protects every version the
// deployment still has, not just the one that was current, so the count spans all of
// them and each is queried by its own build ID.
func TestCountPinnedExecutions_SumsAcrossVersions(t *testing.T) {
	twd := deletingTWD(time.Minute, durPtr(15*time.Minute))
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{count: 2}

	total, err := r.countPinnedExecutions(context.Background(), logr.Discard(), q, twd,
		"automation-engine", versions("main-9e3e1ad", "main-3f94482"))

	require.NoError(t, err)
	assert.Equal(t, int64(4), total)
	require.Len(t, q.countQueries, 2)
	assert.Contains(t, q.countQueries[0], `"automation-engine:main-9e3e1ad"`)
	assert.Contains(t, q.countQueries[1], `"automation-engine:main-3f94482"`)
	assert.Empty(t, drainEvents(recorder))
}

// TestCountPinnedExecutions_NoneOpen: the common case. Nothing pinned means the
// teardown proceeds at once, so this must report a clean zero and no error.
func TestCountPinnedExecutions_NoneOpen(t *testing.T) {
	twd := deletingTWD(time.Minute, durPtr(15*time.Minute))
	r, _ := newTestReconciler(nil)
	q := &fakePinnedQuerier{count: 0}

	total, err := r.countPinnedExecutions(context.Background(), logr.Discard(), q, twd,
		"automation-engine", versions("main-9e3e1ad"))

	require.NoError(t, err)
	assert.Zero(t, total)
}

// TestCountPinnedExecutions_ErrorIsNotZero: a visibility store that is down must
// never read as "nothing is pinned". That would be the silent-strand bug again, with
// an outage as the trigger instead of a Helm timeout.
func TestCountPinnedExecutions_ErrorIsNotZero(t *testing.T) {
	twd := deletingTWD(time.Minute, durPtr(15*time.Minute))
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{countErr: errors.New("visibility store unavailable")}

	_, err := r.countPinnedExecutions(context.Background(), logr.Discard(), q, twd,
		"automation-engine", versions("main-9e3e1ad"))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "main-9e3e1ad")
	assertEventEmitted(t, drainEvents(recorder), ReasonPinnedExecutionCheckFailed)
}

// TestRecordTeardownHeld: a TWD sitting in Terminating with no explanation reads as
// a wedged finalizer. The condition says why it is waiting, and the event fires on
// the transition only so a long wait does not flood the stream.
func TestRecordTeardownHeld(t *testing.T) {
	twd := deletingTWD(time.Minute, durPtr(15*time.Minute))
	r, recorder := newTestReconciler(nil)

	r.recordTeardownHeld(context.Background(), logr.Discard(), twd, 3)

	cond := meta.FindStatusCondition(twd.Status.Conditions, temporaliov1alpha1.ConditionReady)
	require.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionFalse, cond.Status)
	assert.Equal(t, ReasonTeardownDrainingPinnedExecutions, cond.Reason)
	assert.Contains(t, cond.Message, "3 open pinned execution")

	events := drainEvents(recorder)
	assertEventEmitted(t, events, ReasonTeardownDrainingPinnedExecutions)
	assert.Contains(t, strings.Join(events, "\n"), "3 open pinned execution")

	// Same count on the next requeue: condition already says this, so no new event.
	r.recordTeardownHeld(context.Background(), logr.Discard(), twd, 3)
	assert.Empty(t, drainEvents(recorder))

	// A changed count is new information and must be surfaced.
	r.recordTeardownHeld(context.Background(), logr.Discard(), twd, 1)
	assertEventEmitted(t, drainEvents(recorder), ReasonTeardownDrainingPinnedExecutions)
}

// TestErrTeardownWaiting_IsDistinguishable: Reconcile requeues quietly on a held
// teardown and logs an error on a real failure. Both paths return an error, so the
// sentinel has to survive the wrapping that carries the reason.
func TestErrTeardownWaiting_IsDistinguishable(t *testing.T) {
	held := errors.New("boom")
	assert.False(t, errors.Is(held, errTeardownWaiting))

	// Must match how handleDeletion wraps it, reason and all.
	wrapped := fmt.Errorf("%w: %d open pinned execution(s)", errTeardownWaiting, 2)
	assert.True(t, errors.Is(wrapped, errTeardownWaiting))
	assert.Contains(t, wrapped.Error(), "2 open pinned execution(s)")
}
