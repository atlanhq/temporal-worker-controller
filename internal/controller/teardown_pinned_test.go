// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// fakePinnedQuerier is a pinnedExecutionQuerier that returns canned responses and
// records the queries it was asked to run.
type fakePinnedQuerier struct {
	count      int64
	countErr   error
	workflowID []string
	listErr    error

	countQueries []string
	listQueries  []string
}

func (f *fakePinnedQuerier) CountWorkflow(
	_ context.Context,
	req *workflowservice.CountWorkflowExecutionsRequest,
) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	f.countQueries = append(f.countQueries, req.GetQuery())
	if f.countErr != nil {
		return nil, f.countErr
	}
	return &workflowservice.CountWorkflowExecutionsResponse{Count: f.count}, nil
}

func (f *fakePinnedQuerier) ListWorkflow(
	_ context.Context,
	req *workflowservice.ListWorkflowExecutionsRequest,
) (*workflowservice.ListWorkflowExecutionsResponse, error) {
	f.listQueries = append(f.listQueries, req.GetQuery())
	if f.listErr != nil {
		return nil, f.listErr
	}
	execs := make([]*workflowpb.WorkflowExecutionInfo, 0, len(f.workflowID))
	for _, id := range f.workflowID {
		execs = append(execs, &workflowpb.WorkflowExecutionInfo{
			Execution: &commonpb.WorkflowExecution{WorkflowId: id, RunId: "run-" + id},
		})
	}
	return &workflowservice.ListWorkflowExecutionsResponse{Executions: execs}, nil
}

// TestRecordPinnedExecutions_OpenPinned: force-deleting a version that still has
// open pinned executions strands them permanently, so the count and a sample of
// workflow IDs must be recorded before the delete proceeds.
func TestRecordPinnedExecutions_OpenPinned(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{count: 3, workflowID: []string{"wf-a", "wf-b", "wf-c"}}

	r.recordPinnedExecutions(context.Background(), logr.Discard(), q, twd, "automation-engine", "main-9e3e1ad")

	events := drainEvents(recorder)
	assertEventEmitted(t, events, ReasonPinnedExecutionsStranded)

	joined := strings.Join(events, "\n")
	assert.Contains(t, joined, "main-9e3e1ad")
	for _, id := range q.workflowID {
		assert.Contains(t, joined, id, "event must name the stranded workflow so an operator can recover it")
	}

	require.Len(t, q.countQueries, 1)
	assert.Equal(t,
		`ExecutionStatus="Running" AND TemporalWorkerDeploymentVersion="automation-engine:main-9e3e1ad"`,
		q.countQueries[0])
	assert.Equal(t, q.countQueries, q.listQueries, "both calls must use the same query")
}

// TestRecordPinnedExecutions_NoneOpen: the common case is a version with nothing
// pinned to it. That must stay silent and must not pay for the list call.
func TestRecordPinnedExecutions_NoneOpen(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{count: 0}

	r.recordPinnedExecutions(context.Background(), logr.Discard(), q, twd, "automation-engine", "main-3f94482")

	assert.Empty(t, drainEvents(recorder))
	assert.Empty(t, q.listQueries, "no executions to sample, so no list call")
}

// TestRecordPinnedExecutions_CountFails: a visibility failure must not block
// teardown. It reports that the check could not run and returns so the caller
// proceeds with the delete.
func TestRecordPinnedExecutions_CountFails(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{countErr: errors.New("visibility store unavailable")}

	r.recordPinnedExecutions(context.Background(), logr.Discard(), q, twd, "automation-engine", "main-9e3e1ad")

	events := drainEvents(recorder)
	assertEventEmitted(t, events, ReasonPinnedExecutionCheckFailed)
	assert.Contains(t, strings.Join(events, "\n"), "main-9e3e1ad")
	assert.Empty(t, q.listQueries)
}

// TestRecordPinnedExecutions_ListFails: the count is the load-bearing signal. If
// only the ID sample fails, the count must still be reported.
func TestRecordPinnedExecutions_ListFails(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	r, recorder := newTestReconciler(nil)
	q := &fakePinnedQuerier{count: 7, listErr: errors.New("page token expired")}

	r.recordPinnedExecutions(context.Background(), logr.Discard(), q, twd, "automation-engine", "main-9e3e1ad")

	events := drainEvents(recorder)
	assertEventEmitted(t, events, ReasonPinnedExecutionsStranded)
	assert.Contains(t, strings.Join(events, "\n"), "7 open pinned execution")
}
