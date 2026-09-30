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
	"github.com/temporalio/temporal-worker-controller/internal/controller/clientpool"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func leavingTWD(name, deploymentName string, deleting bool) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := &temporaliov1alpha1.TemporalWorkerDeployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app-ns", UID: types.UID(name + "-uid")},
	}
	twd.Spec.WorkerOptions.TemporalNamespace = "default"
	twd.Spec.WorkerOptions.WorkerDeploymentName = deploymentName
	if deleting {
		deleted := metav1.NewTime(time.Now().Add(-10 * time.Minute))
		twd.DeletionTimestamp = &deleted
		twd.Finalizers = []string{finalizerName}
	}
	return twd
}

func releaseTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, temporaliov1alpha1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objs...).
		WithStatusSubresource(&temporaliov1alpha1.TemporalWorkerDeployment{}).
		WithIndex(&appsv1.Deployment{}, deployOwnerKey, func(rawObj client.Object) []string {
			if owner := metav1.GetControllerOf(rawObj.(*appsv1.Deployment)); owner != nil {
				return []string{owner.Name}
			}
			return nil
		}).
		Build()
}

func TestSharesWorkerDeployment(t *testing.T) {
	cases := []struct {
		name    string
		sibling *temporaliov1alpha1.TemporalWorkerDeployment
		want    bool
	}{
		{"live sibling on the same deployment", leavingTWD("app-heavy-twd", "app", false), true},
		{"sibling that is also being deleted", leavingTWD("app-heavy-twd", "app", true), false},
		{"sibling on another deployment", leavingTWD("other-worker-twd", "other", false), false},
		{"sibling on its own default deployment", leavingTWD("app-heavy-twd", "", false), false},
		{"sibling in another Temporal namespace", func() *temporaliov1alpha1.TemporalWorkerDeployment {
			twd := leavingTWD("app-heavy-twd", "app", false)
			twd.Spec.WorkerOptions.TemporalNamespace = "other"
			return twd
		}(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deleting := leavingTWD("app-size-s-twd", "app", true)
			r := &TemporalWorkerDeploymentReconciler{Client: releaseTestClient(t, deleting, tc.sibling)}
			siblings, err := r.siblingTWDs(context.Background(), deleting)
			require.NoError(t, err)
			assert.Equal(t, tc.want, hasLiveSibling(siblings))
		})
	}
}

func ownedWorkers(twd *temporaliov1alpha1.TemporalWorkerDeployment, name, buildID, registeredAs string) *appsv1.Deployment {
	ctrl := true
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app-ns", Labels: map[string]string{k8s.BuildIDLabel: buildID},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: temporaliov1alpha1.GroupVersion.String(), Kind: "TemporalWorkerDeployment",
			Name: twd.Name, UID: twd.UID, Controller: &ctrl,
		}},
	}}
	container := corev1.Container{Name: "worker"}
	if registeredAs != "" {
		container.Env = []corev1.EnvVar{{Name: k8s.TemporalDeploymentNameEnvVar, Value: registeredAs}}
	}
	d.Spec.Template.Spec.Containers = []corev1.Container{container}
	return d
}

func exists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var d appsv1.Deployment
	err := c.Get(context.Background(), types.NamespacedName{Namespace: "app-ns", Name: name}, &d)
	if apierrors.IsNotFound(err) {
		return false
	}
	require.NoError(t, err)
	return true
}

func releaseReconciler(c client.Client) *TemporalWorkerDeploymentReconciler {
	return &TemporalWorkerDeploymentReconciler{Client: c, Recorder: record.NewFakeRecorder(10)}
}

func release(t *testing.T, r *TemporalWorkerDeploymentReconciler, q pinnedExecutionQuerier, twd *temporaliov1alpha1.TemporalWorkerDeployment) error {
	t.Helper()
	return releaseWith(t, r, q, twd, func(context.Context) (sharedRouting, error) { return sharedRouting{}, nil })
}

// releaseWith runs releaseOwnWorkers for twd with its real siblings and own versions, and the given
// routing lookup.
func releaseWith(t *testing.T, r *TemporalWorkerDeploymentReconciler, q pinnedExecutionQuerier, twd *temporaliov1alpha1.TemporalWorkerDeployment, readRouting func(context.Context) (sharedRouting, error)) error {
	t.Helper()
	siblings, err := r.siblingTWDs(context.Background(), twd)
	require.NoError(t, err)
	versions, err := r.ownVersions(context.Background(), twd)
	require.NoError(t, err)
	return r.releaseOwnWorkers(context.Background(), logr.Discard(), q, twd, siblings, versions, readRouting)
}

// Siblings on one release share the build, so the leaving TWD's version is also theirs. Any open
// execution pinned to it may still schedule work on this TWD's queue, so the TWD holds for it.
func TestReleaseOwnWorkers_HoldsOnOwnBuild(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	sibling := leavingTWD("app-worker-twd", "app", false)
	c := releaseTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"), ownedWorkers(sibling, "app-worker-twd-v1", "v1", "app"))
	q := &fakePinnedQuerier{count: 1}

	err := release(t, releaseReconciler(c), q, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	require.Len(t, q.countQueries, 1)
	assert.Equal(t, pinnedExecutionQuery("app", "v1"), q.countQueries[0])
	assert.True(t, exists(t, c, "app-size-s-twd-v1"), "workers stay up while their pinned work runs")
}

func TestReleaseOwnWorkers_ReleasesOnlyOwnWorkers(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	sibling := leavingTWD("app-worker-twd", "app", false)
	c := releaseTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"), ownedWorkers(sibling, "app-worker-twd-new", "new", "app"))

	require.NoError(t, release(t, releaseReconciler(c), &fakePinnedQuerier{}, leaving))

	assert.False(t, exists(t, c, "app-size-s-twd-old"), "the leaving TWD's workers must go")
	assert.True(t, exists(t, c, "app-worker-twd-new"), "the sibling's workers must stay")
}

// Pinned work is counted under the name the workers registered with, so workers left over
// from a workerDeploymentName change are not released while their pinned work still runs.
// A base Deployment and its variant on the same build are one version and one query.
func TestReleaseOwnWorkers_CountsEachPolledVersionOnce(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	c := releaseTestClient(t, leaving, leavingTWD("app-worker-twd", "app", false),
		ownedWorkers(leaving, "app-size-s-twd-new", "new", ""),
		ownedWorkers(leaving, "app-size-s-twd-od-new", "new", "app"),
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "old-app"))
	q := &fakePinnedQuerier{}

	require.NoError(t, release(t, releaseReconciler(c), q, leaving))

	assert.ElementsMatch(t, []string{pinnedExecutionQuery("app", "new"), pinnedExecutionQuery("old-app", "old")}, q.countQueries)
}

// Right after the deletion, an execution may not be pinned or indexed yet, so zero is not trusted.
func TestReleaseOwnWorkers_ZeroRightAfterDeletionHolds(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	just := metav1.NewTime(time.Now().Add(-5 * time.Second))
	leaving.DeletionTimestamp = &just
	c := releaseTestClient(t, leaving, leavingTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))

	err := release(t, releaseReconciler(c), &fakePinnedQuerier{}, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	assert.True(t, exists(t, c, "app-size-s-twd-v1"))
}

func TestReleaseOwnWorkers_CountFailureHolds(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	c := releaseTestClient(t, leaving, leavingTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"))

	err := release(t, releaseReconciler(c), &fakePinnedQuerier{countErr: errors.New("visibility unavailable")}, leaving)

	require.ErrorIs(t, err, errTeardownWaiting)
	assert.True(t, exists(t, c, "app-size-s-twd-old"))
}

// A shared TWD whose workers are already gone has nothing to wait for, so it is released without
// a Temporal client: an unreachable server must not hold it for the whole drainage budget.
func TestHandleDeletion_SharedWithoutWorkersNeedsNoTemporal(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 72 * time.Hour}
	c := releaseTestClient(t, leaving, leavingTWD("app-worker-twd", "app", false))

	require.NoError(t, releaseReconciler(c).handleDeletion(context.Background(), logr.Discard(), leaving))
}

// Past the drainage budget a shared TWD only deletes its own workers, and an unreachable Temporal
// server can't hold them: here it has no TemporalConnection at all. It must not terminate executions
// pinned to builds its siblings also run.
func TestHandleDeletion_SharedPastBudgetReleasesWhenTemporalUnreachable(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: time.Minute}
	past := metav1.NewTime(time.Now().Add(-time.Hour))
	leaving.DeletionTimestamp = &past
	sibling := leavingTWD("app-worker-twd", "app", false)
	c := releaseTestClient(t, leaving, sibling,
		ownedWorkers(leaving, "app-size-s-twd-old", "old", "app"), ownedWorkers(sibling, "app-worker-twd-new", "new", "app"))
	r := releaseReconciler(c) // no TemporalClientPool: any Temporal call would panic

	require.NoError(t, r.handleDeletion(context.Background(), logr.Discard(), leaving))

	assert.False(t, exists(t, c, "app-size-s-twd-old"))
	assert.True(t, exists(t, c, "app-worker-twd-new"))
}

// A budget of zero means no wait at all, so a shared TWD releases at once, even while Temporal still
// sends new work to its build and a live sibling is switching away from it.
func TestHandleDeletion_SharedZeroBudgetReleasesAtOnce(t *testing.T) {
	leaving := leavingTWD("app-size-s-twd", "app", true)
	leaving.Spec.WorkerOptions.TemporalConnectionRef.Name = "conn"
	leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 0}
	c := releaseTestClient(t, leaving, liveSibling("v2", false), testConnection(), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))
	r := releaseReconciler(c)
	stub := withStubTemporal(r, c, fakePinnedQuerier{count: 3})
	h := stubHandle(stub)
	h.versionExists = true
	h.describeErr = nil
	h.describeResp.Info.RoutingConfig.CurrentVersion = &sdkworker.WorkerDeploymentVersion{DeploymentName: "app", BuildID: "v1"}
	h.describeResp.Info.RoutingConfig.CurrentVersionChangedTime = time.Now().Add(-time.Hour)

	require.NoError(t, r.handleDeletion(context.Background(), logr.Discard(), leaving))

	assert.False(t, exists(t, c, "app-size-s-twd-v1"))
}

// Past its drainage budget a shared TWD stops waiting for pinned executions but still holds while
// Temporal sends new work to its build and a live sibling targets another: released then, the
// switch would be refused for good once it is gone. A NotFound while its version exists is a
// transient answer from a reachable server, so it holds too; any other routing failure releases.
func TestHandleDeletion_SharedPastBudgetHoldsWhileSiblingSwitches(t *testing.T) {
	longAgo := time.Now().Add(-2 * time.Hour)
	for _, tc := range []struct {
		name          string
		describe      error
		current       string
		versionExists bool
		wantHeld      bool
	}{
		{"its build still current, sibling moving on", nil, "v1", true, true},
		{"sibling already switched", nil, "v2", true, false},
		{"routing unreadable", errors.New("unavailable"), "", true, false},
		{"not found while its version exists", &serviceerror.NotFound{}, "", true, true},
		{"never registered", &serviceerror.NotFound{}, "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaving := leavingTWD("app-size-s-twd", "app", true)
			past := metav1.NewTime(time.Now().Add(-time.Hour))
			leaving.DeletionTimestamp = &past
			leaving.Spec.WorkerOptions.TemporalConnectionRef.Name = "conn"
			leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: time.Minute}
			c := releaseTestClient(t, leaving, liveSibling("v2", false), testConnection(), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))
			r := releaseReconciler(c)
			stub := withStubTemporal(r, c, fakePinnedQuerier{count: 3})
			h := stubHandle(stub)
			h.describeErr = tc.describe
			h.versionExists = tc.versionExists
			if tc.current != "" {
				h.describeResp.Info.RoutingConfig.CurrentVersion = &sdkworker.WorkerDeploymentVersion{DeploymentName: "app", BuildID: tc.current}
				h.describeResp.Info.RoutingConfig.CurrentVersionChangedTime = longAgo
			}

			err := r.handleDeletion(context.Background(), logr.Discard(), leaving)

			if tc.wantHeld {
				require.ErrorIs(t, err, errTeardownWaiting)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeld, exists(t, c, "app-size-s-twd-v1"))
			assert.Empty(t, stub.pinned.countQueries, "past the budget pinned executions are no longer waited for")
		})
	}
}

// countingTemporalClient answers the pinned-execution count on top of a stub whose Worker
// Deployment describe fails with NotFound.
type countingTemporalClient struct {
	*stubTemporalClient
	pinned fakePinnedQuerier
}

func (c *countingTemporalClient) CountWorkflow(ctx context.Context, req *workflowservice.CountWorkflowExecutionsRequest) (*workflowservice.CountWorkflowExecutionsResponse, error) {
	return c.pinned.CountWorkflow(ctx, req)
}

// A NotFound from the server can be transient, so a TWD on its own Worker Deployment holds while the
// server still knows one of its versions, however long ago it was deleted, and when it can't tell.
// When the server knows none, nothing can be pinned to its workers and it releases them without
// counting.
func TestHandleDeletion_NotFoundHoldsWhileItsVersionsExist(t *testing.T) {
	for _, tc := range []struct {
		name               string
		deletedAgo         time.Duration
		versionExists      bool
		describeVersionErr error
		wantHeld           bool
	}{
		{"version exists, right after deletion", time.Second, true, nil, true},
		{"version exists, long after deletion", 10 * time.Hour, true, nil, true},
		{"version lookup failed", 10 * time.Hour, false, errors.New("unavailable"), true},
		{"no version, right after deletion", time.Second, false, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			twd := leavingTWD("app-worker-twd", "", true)
			deleted := metav1.NewTime(time.Now().Add(-tc.deletedAgo))
			twd.DeletionTimestamp = &deleted
			twd.Spec.WorkerOptions.TemporalConnectionRef.Name = "conn"
			twd.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 72 * time.Hour}
			c := releaseTestClient(t, twd, testConnection(), ownedWorkers(twd, "app-worker-twd-v1", "v1", ""))
			r := releaseReconciler(c)
			stub := withStubTemporal(r, c, fakePinnedQuerier{})
			h := stubHandle(stub)
			h.versionExists = tc.versionExists
			h.describeVersionErr = tc.describeVersionErr

			err := r.handleDeletion(context.Background(), logr.Discard(), twd)

			if tc.wantHeld {
				require.ErrorIs(t, err, errTeardownWaiting)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeld, exists(t, c, "app-worker-twd-v1"))
			assert.Empty(t, stub.pinned.countQueries)
		})
	}
}

func testConnection() *temporaliov1alpha1.TemporalConnection {
	return &temporaliov1alpha1.TemporalConnection{
		ObjectMeta: metav1.ObjectMeta{Name: "conn", Namespace: "app-ns"},
		Spec:       temporaliov1alpha1.TemporalConnectionSpec{HostPort: "temporal:7233"},
	}
}

func withStubTemporal(r *TemporalWorkerDeploymentReconciler, c client.Client, pinned fakePinnedQuerier) *countingTemporalClient {
	r.TemporalClientPool = clientpool.New(nil, c)
	stub := &countingTemporalClient{stubTemporalClient: newStubTemporalClient(nil), pinned: pinned}
	r.TemporalClientPool.SetClientForTesting(noCredsPoolKey("temporal:7233", "default"), stub)
	return stub
}

func stubHandle(stub *countingTemporalClient) *stubWDHandle {
	return stub.wdClient.(*stubWDClient).handle.(*stubWDHandle)
}

// liveSibling is a live TWD on the "app" Worker Deployment whose spec targets buildID, with no
// status at all: the release must not depend on it.
func liveSibling(buildID string, deleting bool) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd := leavingTWD("app-worker-twd", "app", deleting)
	twd.Spec.WorkerOptions.UnsafeCustomBuildID = buildID
	return twd
}

// While the routing still sends new executions to the leaving TWD's build, as current or ramping,
// and a live sibling targets another build, those executions may need the leaving TWD's workers,
// so it holds. The routing comes from the server and the sibling's target from its spec.
func TestReleaseOwnWorkers_HoldsWhileRoutingSendsNewWorkToItsBuild(t *testing.T) {
	for _, tc := range []struct {
		name              string
		routed            []string
		siblingTarget     string
		siblingIsDeleting bool
		wantHeld          bool
	}{
		{"current on its build, sibling moving to another", []string{"v1"}, "v2", false, true},
		{"ramping to its build, sibling moving to another", []string{"v0", "v1"}, "v2", false, true},
		{"sibling already switched", []string{"v2"}, "v2", false, false},
		{"sibling staying on the build", []string{"v1"}, "v1", false, false},
		{"sibling also being deleted", []string{"v1"}, "v2", true, false},
		{"nothing routed", nil, "v2", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaving := leavingTWD("app-size-s-twd", "app", true)
			c := releaseTestClient(t, leaving, liveSibling(tc.siblingTarget, tc.siblingIsDeleting), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))
			err := releaseWith(t, releaseReconciler(c), &fakePinnedQuerier{}, leaving,
				func(context.Context) (sharedRouting, error) { return sharedRouting{builds: tc.routed}, nil })

			if tc.wantHeld {
				require.ErrorIs(t, err, errTeardownWaiting)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeld, exists(t, c, "app-size-s-twd-v1"))
		})
	}
}

// A switch of the current or ramping version pins executions to the new routing just before it.
// Those may not be visible to the count yet, so the settle also runs from the last routing change,
// and an unreadable routing holds.
func TestReleaseOwnWorkers_SettlesFromTheLastRoutingChange(t *testing.T) {
	for _, tc := range []struct {
		name     string
		changed  time.Time
		err      error
		wantHeld bool
	}{
		{"routing changed just now", time.Now().Add(-10 * time.Second), nil, true},
		{"routing changed long ago", time.Now().Add(-time.Hour), nil, false},
		{"routing unreadable", time.Time{}, errors.New("describe failed"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaving := leavingTWD("app-size-s-twd", "app", true)
			c := releaseTestClient(t, leaving, leavingTWD("app-worker-twd", "app", false), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))
			err := releaseWith(t, releaseReconciler(c), &fakePinnedQuerier{}, leaving,
				func(context.Context) (sharedRouting, error) { return sharedRouting{changed: tc.changed}, tc.err })

			if tc.wantHeld {
				require.ErrorIs(t, err, errTeardownWaiting)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeld, exists(t, c, "app-size-s-twd-v1"))
		})
	}
}

// The shared path reads the routing with the Worker Deployment's describe. A NotFound releases the
// TWD at once only when its own version is unknown too, so nothing was ever registered; otherwise it
// is transient and, like any other failure, holds. A sibling's status plays no part.
func TestHandleDeletion_SharedRoutingLookup(t *testing.T) {
	longAgo := time.Now().Add(-time.Hour)
	for _, tc := range []struct {
		name               string
		describe           error
		current, ramping   string
		versionExists      bool
		describeVersionErr error
		justDeleted        bool
		wantHeld           bool
	}{
		{"never registered", &serviceerror.NotFound{}, "", "", false, nil, false, false},
		{"never registered, right after deletion", &serviceerror.NotFound{}, "", "", false, nil, true, false},
		{"not found while its version exists, right after deletion", &serviceerror.NotFound{}, "", "", true, nil, true, true},
		{"not found while its version exists", &serviceerror.NotFound{}, "", "", true, nil, false, true},
		{"not found and version lookup failed", &serviceerror.NotFound{}, "", "", false, errors.New("unavailable"), false, true},
		{"lookup failed", errors.New("unavailable"), "", "", false, nil, false, true},
		{"its build still current, sibling moving on", nil, "v1", "", true, nil, false, true},
		{"its build ramping, sibling moving on", nil, "v0", "v1", true, nil, false, true},
		{"sibling's build current", nil, "v2", "", true, nil, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			leaving := leavingTWD("app-size-s-twd", "app", true)
			leaving.Spec.WorkerOptions.TemporalConnectionRef.Name = "conn"
			leaving.Spec.SunsetStrategy.TeardownDrainageTimeout = &metav1.Duration{Duration: 72 * time.Hour}
			if tc.justDeleted {
				deleted := metav1.NewTime(time.Now().Add(-time.Second))
				leaving.DeletionTimestamp = &deleted
			}
			c := releaseTestClient(t, leaving, liveSibling("v2", false), testConnection(), ownedWorkers(leaving, "app-size-s-twd-v1", "v1", "app"))
			r := releaseReconciler(c)
			stub := withStubTemporal(r, c, fakePinnedQuerier{})
			h := stubHandle(stub)
			h.describeErr = tc.describe
			h.versionExists = tc.versionExists
			h.describeVersionErr = tc.describeVersionErr
			rc := &h.describeResp.Info.RoutingConfig
			if tc.current != "" {
				rc.CurrentVersion = &sdkworker.WorkerDeploymentVersion{DeploymentName: "app", BuildID: tc.current}
				rc.CurrentVersionChangedTime = longAgo
			}
			if tc.ramping != "" {
				rc.RampingVersion = &sdkworker.WorkerDeploymentVersion{DeploymentName: "app", BuildID: tc.ramping}
				rc.RampingVersionChangedTime = longAgo
			}

			err := r.handleDeletion(context.Background(), logr.Discard(), leaving)

			if tc.wantHeld {
				require.ErrorIs(t, err, errTeardownWaiting)
			} else {
				require.NoError(t, err)
			}
			assert.Equal(t, tc.wantHeld, exists(t, c, "app-size-s-twd-v1"))
		})
	}
}
