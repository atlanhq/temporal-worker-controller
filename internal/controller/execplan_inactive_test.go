// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// perBuildPinnedQuerier answers CountWorkflow by the version named in the query.
type perBuildPinnedQuerier struct {
	pinnedExecutionQuerier
	count   map[string]int64
	err     map[string]error
	queried []string
}

func (q *perBuildPinnedQuerier) CountWorkflow(_ context.Context, req *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	for buildID := range q.count {
		if req.GetQuery() == pinnedExecutionQuery("w", buildID) {
			q.queried = append(q.queried, buildID)
			return &workflowservice.CountWorkflowExecutionsResponse{Count: q.count[buildID]}, q.err[buildID]
		}
	}
	return nil, errors.New("unexpected query " + req.GetQuery())
}

// An Inactive version at zero replicas has no pollers, so the server would delete it even
// with workflows pinned to it. Its Deployments are held while any are running or while the
// count is unknown, and a base and its variant are always held or deleted together.
func TestHoldPinnedInactiveVersions(t *testing.T) {
	deploy := func(name, buildID string) *appsv1.Deployment {
		return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{k8s.BuildIDLabel: buildID}}}
	}
	version := func(buildID string, s temporaliov1alpha1.VersionStatus) *temporaliov1alpha1.DeprecatedWorkerDeploymentVersion {
		return &temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
			BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: buildID, Status: s},
		}
	}

	twd := makeTWD("w", "ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		version("idle", temporaliov1alpha1.VersionStatusInactive),
		version("pinned", temporaliov1alpha1.VersionStatusInactive),
		version("unknown", temporaliov1alpha1.VersionStatusInactive),
		version("drained", temporaliov1alpha1.VersionStatusDrained),
	}
	q := &perBuildPinnedQuerier{
		count: map[string]int64{"idle": 0, "pinned": 2, "unknown": 0},
		err:   map[string]error{"unknown": errors.New("visibility unavailable")},
	}
	p := &plan{
		WorkerDeploymentName: "w",
		DeleteDeployments: []*appsv1.Deployment{
			deploy("w-idle", "idle"), deploy("w-od-idle", "idle"),
			deploy("w-pinned", "pinned"), deploy("w-od-pinned", "pinned"),
			deploy("w-unknown", "unknown"),
			deploy("w-drained", "drained"),
		},
	}

	r, _ := newTestReconciler(nil)
	r.holdPinnedInactiveVersions(context.Background(), logr.Discard(), q, twd, p)

	var kept []string
	for _, d := range p.DeleteDeployments {
		kept = append(kept, d.Name)
	}
	assert.ElementsMatch(t, []string{"w-idle", "w-od-idle", "w-drained"}, kept)
	assert.ElementsMatch(t, []string{"idle", "pinned", "unknown"}, q.queried, "one query per Inactive build, none for Drained")
}

// deleteRecordingHandle records the versions the controller asks the server to delete.
type deleteRecordingHandle struct {
	stubWDHandle
	deleted []string
}

func (h *deleteRecordingHandle) DeleteVersion(_ context.Context, o sdkclient.WorkerDeploymentDeleteVersionOptions) (sdkclient.WorkerDeploymentDeleteVersionResponse, error) {
	h.deleted = append(h.deleted, o.BuildID)
	return sdkclient.WorkerDeploymentDeleteVersionResponse{}, nil
}

// executePlan checks pinned workflows before any delete, so a held Inactive version keeps
// both its Kubernetes Deployment and its server-side record.
func TestExecutePlan_InactiveVersionDeletedOnlyWithoutPinnedWorkflows(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pinned      int64
		wantDeleted bool
	}{
		{"no pinned workflows, deleted", 0, true},
		{"pinned workflow running, kept", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twd := makeTWD("app-worker", "app-ns", "conn")
			twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{
					BuildID: "b", Status: temporaliov1alpha1.VersionStatusInactive,
				},
			}}
			d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
				Name: "app-worker-b", Namespace: "app-ns", Labels: map[string]string{k8s.BuildIDLabel: "b"},
			}}
			r, _ := newTestReconciler([]client.Object{d.DeepCopy()})
			handle := &deleteRecordingHandle{}
			temporalClient := &countingTemporalClient{
				stubTemporalClient: &stubTemporalClient{wdClient: &stubWDClient{handle: handle}},
				pinned:             fakePinnedQuerier{count: tc.pinned},
			}
			p := &plan{WorkerDeploymentName: "app", DeleteDeployments: []*appsv1.Deployment{d}}

			require.NoError(t, r.executePlan(context.Background(), logr.Discard(), twd, temporalClient, p))

			assert.Equal(t, !tc.wantDeleted, exists(t, r.Client, "app-worker-b"))
			if tc.wantDeleted {
				assert.Equal(t, []string{"b"}, handle.deleted)
			} else {
				assert.Empty(t, handle.deleted)
			}
			assert.Equal(t, []string{pinnedExecutionQuery("app", "b")}, temporalClient.pinned.countQueries)
		})
	}
}
