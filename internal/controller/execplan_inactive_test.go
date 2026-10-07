// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

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
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/flowcontrol"
	clocktesting "k8s.io/utils/clock/testing"
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
	known    map[string]bool
	refuse   map[string]error
	deleted  []string
	attempts int
}

func (h *versionDeletingHandle) DeleteVersion(_ context.Context, o sdkclient.WorkerDeploymentDeleteVersionOptions) (sdkclient.WorkerDeploymentDeleteVersionResponse, error) {
	h.attempts++
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

// A version whose variant delete fails keeps its base, and so its status entry and its server
// record, for the next reconcile to retry as a whole.
func TestExecutePlan_VariantDeleteFailureKeepsVersion(t *testing.T) {
	twd := makeTWD("app-worker", "app-ns", "conn")
	base := buildDeployment("app-worker-b", "b", k8s.BaseVariantName)
	variant := buildDeployment("app-worker-od-b", "b", "od")
	r, _ := newTestReconcilerWithInterceptors([]client.Object{base.DeepCopy(), variant.DeepCopy()}, interceptor.Funcs{
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if obj.GetName() == variant.Name {
				return errors.New("apiserver unavailable")
			}
			return c.Delete(ctx, obj, opts...)
		},
	})
	handle := &versionDeletingHandle{known: map[string]bool{"b": true}}
	temporalClient := &countingTemporalClient{stubTemporalClient: &stubTemporalClient{wdClient: &stubWDClient{handle: handle}}}
	p := &plan{WorkerDeploymentName: "app", DeleteDeployments: []*appsv1.Deployment{variant, base}}

	require.Error(t, r.executePlan(context.Background(), logr.Discard(), twd, temporalClient, p))
	assert.True(t, exists(t, r.Client, "app-worker-b"))
	assert.True(t, handle.known["b"])
}

// A refused delete is not retried until the version's backoff expires, so a server that keeps
// failing is not called on every reconcile, and a later success resets the backoff.
func TestDeleteInactiveVersions_BacksOffAfterRefusal(t *testing.T) {
	twd := makeTWD("w", "app-ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
	}
	q := &perBuildPinnedQuerier{deploymentName: "w", count: map[string]int64{"b": 0}}
	h := &versionDeletingHandle{known: map[string]bool{"b": true}, refuse: map[string]error{"b": errors.New("context deadline exceeded")}}
	r, _ := newTestReconciler(nil)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	r.deleteBackoff = flowcontrol.NewFakeBackOff(10*time.Second, 30*time.Minute, fakeClock)
	attempt := func() []string {
		p := &plan{WorkerDeploymentName: "w", DeleteDeployments: []*appsv1.Deployment{buildDeployment("w-b", "b", k8s.BaseVariantName)}}
		r.deleteInactiveVersions(context.Background(), logr.Discard(), q, h, twd, p)
		var kept []string
		for _, d := range p.DeleteDeployments {
			kept = append(kept, d.Name)
		}
		return kept
	}

	assert.Empty(t, attempt())
	assert.Equal(t, 1, h.attempts)

	fakeClock.Step(5 * time.Second)
	assert.Empty(t, attempt())
	assert.Equal(t, 1, h.attempts, "no retry inside the backoff window")

	fakeClock.Step(6 * time.Second)
	assert.Empty(t, attempt())
	assert.Equal(t, 2, h.attempts, "retried once the window expired")

	fakeClock.Step(15 * time.Second)
	assert.Empty(t, attempt())
	assert.Equal(t, 2, h.attempts, "the window doubled after the second refusal")

	h.refuse = nil
	fakeClock.Step(10 * time.Second)
	assert.Equal(t, []string{"w-b"}, attempt())
	assert.Equal(t, 3, h.attempts)
}

// The version record is shared by every TWD on the Worker Deployment, so it is deleted only once
// no sibling targets it, runs it as current, or has a pod for it.
func TestDeleteInactiveVersions_WaitsForSiblings(t *testing.T) {
	sibling := func(target, current string) *temporaliov1alpha1.TemporalWorkerDeployment {
		s := makeTWD("w-size-s", "app-ns", "conn")
		s.UID = "sibling-uid"
		s.Spec.WorkerOptions.WorkerDeploymentName = "w"
		s.Status.TargetVersion.BuildID = target
		if current != "" {
			s.Status.CurrentVersion = &temporaliov1alpha1.CurrentWorkerDeploymentVersion{
				BaseWorkerDeploymentVersion: temporaliov1alpha1.BaseWorkerDeploymentVersion{BuildID: current},
			}
		}
		return s
	}
	// Deployments rendered by the controller record their Worker Deployment name; older ones may not.
	siblingDeploymentRecording := func(replicas int32, recorded string) *appsv1.Deployment {
		d := buildDeployment("w-size-s-b", "b", k8s.BaseVariantName)
		d.Spec.Replicas = &replicas
		d.Status.Replicas = replicas
		if recorded != "" {
			d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "worker", Env: []corev1.EnvVar{{Name: k8s.TemporalDeploymentNameEnvVar, Value: recorded}}}}
		}
		return d
	}
	sharedName := k8s.ComputeWorkerDeploymentName(func() *temporaliov1alpha1.TemporalWorkerDeployment {
		t := makeTWD("w-size-m", "app-ns", "conn")
		t.Spec.WorkerOptions.WorkerDeploymentName = "w"
		return t
	}())
	siblingDeployment := func(replicas int32) *appsv1.Deployment { return siblingDeploymentRecording(replicas, sharedName) }
	otherAppDeployment := func() *appsv1.Deployment {
		d := siblingDeployment(1)
		d.Name = "other-app-b"
		d.Spec.Template.Spec.Containers = []corev1.Container{{Name: "worker", Env: []corev1.EnvVar{{Name: k8s.TemporalDeploymentNameEnvVar, Value: "other-app"}}}}
		return d
	}
	for _, tc := range []struct {
		name        string
		objs        []client.Object
		wantDeleted bool
	}{
		{"sibling moved on and scaled down, deleted", []client.Object{sibling("c", "c"), siblingDeployment(0)}, true},
		{"sibling still targets the version, kept", []client.Object{sibling("b", "a"), siblingDeployment(0)}, false},
		{"sibling runs the version as current, kept", []client.Object{sibling("c", "b"), siblingDeployment(0)}, false},
		{"sibling still has a pod for the version, kept", []client.Object{sibling("c", "c"), siblingDeployment(1)}, false},
		{"sibling Deployment without a recorded name still has a pod, kept", []client.Object{sibling("c", "c"), siblingDeploymentRecording(1, "")}, false},
		{"another app runs the same build ID, deleted", []client.Object{sibling("c", "c"), otherAppDeployment()}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twd := makeTWD("w-size-m", "app-ns", "conn")
			twd.Spec.WorkerOptions.WorkerDeploymentName = "w"
			twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
				deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
			}
			r, _ := newTestReconciler(append(tc.objs, twd))
			name := k8s.ComputeWorkerDeploymentName(twd)
			q := &perBuildPinnedQuerier{deploymentName: name, count: map[string]int64{"b": 0}}
			h := &versionDeletingHandle{known: map[string]bool{"b": true}}
			p := &plan{WorkerDeploymentName: name, DeleteDeployments: []*appsv1.Deployment{buildDeployment("w-size-m-b", "b", k8s.BaseVariantName)}}

			r.deleteInactiveVersions(context.Background(), logr.Discard(), q, h, twd, p)

			assert.Equal(t, tc.wantDeleted, len(p.DeleteDeployments) == 1)
			assert.Equal(t, !tc.wantDeleted, h.known["b"], "server-side version exists")
		})
	}
}

// A failed pinned count backs off like a refused delete, so a degraded visibility store is not
// queried on every reconcile.
func TestDeleteInactiveVersions_BacksOffAfterCountError(t *testing.T) {
	twd := makeTWD("w", "app-ns", "conn")
	twd.Status.DeprecatedVersions = []*temporaliov1alpha1.DeprecatedWorkerDeploymentVersion{
		deprecatedVersion("b", temporaliov1alpha1.VersionStatusInactive),
	}
	q := &perBuildPinnedQuerier{deploymentName: "w", count: map[string]int64{"b": 0}, err: map[string]error{"b": errors.New("visibility unavailable")}}
	h := &versionDeletingHandle{known: map[string]bool{"b": true}}
	r, _ := newTestReconciler(nil)
	fakeClock := clocktesting.NewFakeClock(time.Now())
	r.deleteBackoff = flowcontrol.NewFakeBackOff(10*time.Second, 30*time.Minute, fakeClock)
	attempt := func() {
		p := &plan{WorkerDeploymentName: "w", DeleteDeployments: []*appsv1.Deployment{buildDeployment("w-b", "b", k8s.BaseVariantName)}}
		r.deleteInactiveVersions(context.Background(), logr.Discard(), q, h, twd, p)
		assert.Len(t, p.DeleteDeployments, 0)
	}

	attempt()
	fakeClock.Step(5 * time.Second)
	attempt()
	assert.Len(t, q.queried, 1, "no query inside the backoff window")
	fakeClock.Step(6 * time.Second)
	attempt()
	assert.Len(t, q.queried, 2)
	assert.Zero(t, h.attempts, "never deleted without a count")
}
