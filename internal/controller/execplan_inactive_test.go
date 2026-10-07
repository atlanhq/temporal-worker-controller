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
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// perBuildPinnedQuerier answers CountWorkflow by the version named in the query.
type perBuildPinnedQuerier struct {
	pinnedExecutionQuerier
	deploymentName string
	count          map[string]int64
	err            map[string]error
	queried        []string
}

func (q *perBuildPinnedQuerier) CountWorkflow(_ context.Context, req *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	for buildID := range q.count {
		if req.GetQuery() == pinnedExecutionQuery(q.deploymentName, buildID) {
			q.queried = append(q.queried, buildID)
			if err := q.err[buildID]; err != nil {
				return nil, err
			}
			return &workflowservice.CountWorkflowExecutionsResponse{Count: q.count[buildID]}, nil
		}
	}
	return nil, errors.New("unexpected query " + req.GetQuery())
}

// versionDeletingHandle deletes server-side versions, refusing those in refuse. A version it
// has deleted, or never knew, answers NotFound.
type versionDeletingHandle struct {
	stubWDHandle
	known   map[string]bool
	refuse  map[string]error
	deleted []string
}

func (h *versionDeletingHandle) DeleteVersion(_ context.Context, o sdkclient.WorkerDeploymentDeleteVersionOptions) (sdkclient.WorkerDeploymentDeleteVersionResponse, error) {
	if err := h.refuse[o.BuildID]; err != nil {
		return sdkclient.WorkerDeploymentDeleteVersionResponse{}, err
	}
	if !h.known[o.BuildID] {
		return sdkclient.WorkerDeploymentDeleteVersionResponse{}, serviceerror.NewNotFound("version not found")
	}
	h.known[o.BuildID] = false
	h.deleted = append(h.deleted, o.BuildID)
	return sdkclient.WorkerDeploymentDeleteVersionResponse{}, nil
}

func buildDeployment(name, buildID, variant string) *appsv1.Deployment {
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app-ns", Labels: map[string]string{k8s.BuildIDLabel: buildID, k8s.VariantLabel: variant},
	}}
}

func deprecatedVersion(buildID string, s temporaliov1alpha1.VersionStatus) *temporaliov1alpha1.DeprecatedWorkerDeploymentVersion {
	return &temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: buildID, Status: s},
	}
}

// An Inactive version is deleted from the server before its Deployments, and only when no
// workflow is pinned to it. Whatever stops that (pinned workflows, an unreadable count, the
// server refusing) keeps the version's Deployments and rendered resources for a retry, and a
// base and its variant always share the outcome. A variant removed from the spec on its own
// leaves its version alone.
func TestDeleteInactiveVersions(t *testing.T) {
	twd := makeTWD("w", "app-ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		deprecatedVersion("idle", temporaliov1alpha1.VersionStatusInactive),
		deprecatedVersion("gone", temporaliov1alpha1.VersionStatusInactive),
		deprecatedVersion("pinned", temporaliov1alpha1.VersionStatusInactive),
		deprecatedVersion("unknown", temporaliov1alpha1.VersionStatusInactive),
		deprecatedVersion("refused", temporaliov1alpha1.VersionStatusInactive),
		deprecatedVersion("drained", temporaliov1alpha1.VersionStatusDrained),
		deprecatedVersion("live", temporaliov1alpha1.VersionStatusInactive),
	}
	q := &perBuildPinnedQuerier{
		deploymentName: "w",
		count:          map[string]int64{"idle": 0, "gone": 0, "pinned": 2, "unknown": 0, "refused": 0, "live": 0},
		err:            map[string]error{"unknown": errors.New("visibility unavailable")},
	}
	h := &versionDeletingHandle{
		known:  map[string]bool{"idle": true, "pinned": true, "unknown": true, "refused": true, "drained": true, "live": true},
		refuse: map[string]error{"refused": serviceerror.NewFailedPrecondition("version has active pollers")},
	}
	var deployments []*appsv1.Deployment
	var resources []planner.WorkerResourceRef
	for _, b := range []string{"idle", "gone", "pinned", "unknown", "refused", "drained"} {
		deployments = append(deployments, buildDeployment("w-"+b, b, k8s.BaseVariantName), buildDeployment("w-od-"+b, b, "od"))
		resources = append(resources, planner.WorkerResourceRef{Name: "res-" + b, BuildID: b})
	}
	// Only the removed variant of version "live" is being deleted.
	deployments = append(deployments, buildDeployment("w-od-live", "live", "od"))
	p := &plan{WorkerDeploymentName: "w", DeleteDeployments: deployments, DeleteWorkerResources: resources}

	r, _ := newTestReconciler(nil)
	r.deleteInactiveVersions(context.Background(), logr.Discard(), q, h, twd, p)

	var keptDeployments, keptResources []string
	for _, d := range p.DeleteDeployments {
		keptDeployments = append(keptDeployments, d.Name)
	}
	for _, res := range p.DeleteWorkerResources {
		keptResources = append(keptResources, res.Name)
	}
	assert.ElementsMatch(t, []string{"w-idle", "w-od-idle", "w-gone", "w-od-gone", "w-drained", "w-od-drained", "w-od-live"}, keptDeployments)
	assert.ElementsMatch(t, []string{"res-idle", "res-gone", "res-drained"}, keptResources)
	assert.ElementsMatch(t, []string{"idle", "gone", "pinned", "unknown", "refused"}, q.queried, "one query per nominated Inactive version")
	assert.Equal(t, []string{"idle"}, h.deleted, "only versions with no pinned workflows are deleted, and Drained is left to deleteDrainedVersions")
}

// executePlan deletes an Inactive version's server record before its Deployment, so a version
// that is kept for any reason still has both, and the next reconcile can retry.
func TestExecutePlan_InactiveVersionDeletedServerFirst(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pinned      int64
		refuse      error
		wantDeleted bool
	}{
		{"no pinned workflows, deleted", 0, nil, true},
		{"pinned workflow running, kept", 1, nil, false},
		{"server still sees pollers, kept", 0, serviceerror.NewFailedPrecondition("version has active pollers"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twd := makeTWD("app-worker", "app-ns", "conn")
			twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
				deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
			}
			d := buildDeployment("app-worker-b", "b", k8s.BaseVariantName)
			r, _ := newTestReconciler([]client.Object{d.DeepCopy()})
			handle := &versionDeletingHandle{known: map[string]bool{"b": true}, refuse: map[string]error{"b": tc.refuse}}
			temporalClient := &countingTemporalClient{
				stubTemporalClient: &stubTemporalClient{wdClient: &stubWDClient{handle: handle}},
				pinned:             fakePinnedQuerier{count: tc.pinned},
			}
			p := &plan{WorkerDeploymentName: "app", DeleteDeployments: []*appsv1.Deployment{d}}

			require.NoError(t, r.executePlan(context.Background(), logr.Discard(), twd, temporalClient, p))

			assert.Equal(t, !tc.wantDeleted, exists(t, r.Client, "app-worker-b"), "Deployment exists")
			assert.Equal(t, !tc.wantDeleted, handle.known["b"], "server-side version exists")
		})
	}
}

// When the Deployment delete fails after the server record is gone, the Deployment stays and
// the version reads as NotRegistered on the next reconcile, which the planner deletes (see
// TestGetDeleteDeployments_InactiveRecordGoneDeploymentLeft).
func TestExecutePlan_InactiveVersionDeploymentDeleteFails(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
	}
	d := buildDeployment("app-worker-b", "b", k8s.BaseVariantName)
	r, _ := newTestReconcilerWithInterceptors([]client.Object{d.DeepCopy()}, interceptor.Funcs{
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			return errors.New("apiserver unavailable")
		},
	})
	handle := &versionDeletingHandle{known: map[string]bool{"b": true}}
	temporalClient := &countingTemporalClient{
		stubTemporalClient: &stubTemporalClient{wdClient: &stubWDClient{handle: handle}},
	}
	p := &plan{WorkerDeploymentName: "app", DeleteDeployments: []*appsv1.Deployment{d}}

	require.Error(t, r.executePlan(context.Background(), logr.Discard(), twd, temporalClient, p))
	assert.True(t, exists(t, r.Client, "app-worker-b"))
	assert.False(t, handle.known["b"])
}

// Removing a variant from spec.variants deletes only that Deployment. Its version's server
// record stays, so the pinned check is not needed and nothing is asked of the server.
func TestExecutePlan_RemovedVariantKeepsVersion(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
	}
	v := buildDeployment("app-worker-od-b", "b", "od")
	r, _ := newTestReconciler([]client.Object{v.DeepCopy()})
	handle := &versionDeletingHandle{known: map[string]bool{"b": true}}
	temporalClient := &countingTemporalClient{
		stubTemporalClient: &stubTemporalClient{wdClient: &stubWDClient{handle: handle}},
		pinned:             fakePinnedQuerier{count: 1},
	}
	p := &plan{WorkerDeploymentName: "app", DeleteDeployments: []*appsv1.Deployment{v}}

	require.NoError(t, r.executePlan(context.Background(), logr.Discard(), twd, temporalClient, p))

	assert.False(t, exists(t, r.Client, "app-worker-od-b"))
	assert.True(t, handle.known["b"])
	assert.Empty(t, temporalClient.pinned.countQueries)
}
