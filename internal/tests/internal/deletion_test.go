package internal

import (
	"context"
	"errors"
	"testing"
	"time"

	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/testhelpers"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/temporaltest"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestDeletionCleanup verifies that deleting a TemporalWorkerDeployment CRD
// cleans up Temporal server-side versioning data (build IDs, current version routing).
// This prevents stale build ID routing from blocking unversioned workers.
func TestDeletionCleanup(t *testing.T) {
	cfg, k8sClient, _, _, cleanup := setupTestEnvironment(t)
	defer cleanup()

	testNamespace := createTestNamespace(t, k8sClient)
	defer cleanupTestNamespace(t, cfg, k8sClient, testNamespace)

	dc := dynamicconfig.NewMemoryClient()
	dc.OverrideValue("matching.wv.VersionDrainageStatusVisibilityGracePeriod", testDrainageVisibilityGracePeriod)
	dc.OverrideValue("matching.wv.VersionDrainageStatusRefreshInterval", testDrainageRefreshInterval)
	ts := temporaltest.NewServer(
		temporaltest.WithT(t),
		temporaltest.WithBaseServerOptions(temporal.WithDynamicConfigClient(dc)),
	)

	t.Run("deletion-sets-current-to-unversioned", func(t *testing.T) {
		testDeletionSetsCurrentToUnversioned(t, k8sClient, ts, testNamespace.Name)
	})

	t.Run("deletion-with-no-temporal-connection", func(t *testing.T) {
		testDeletionWithNoTemporalConnection(t, k8sClient, ts, testNamespace.Name)
	})
}

// testDeletionSetsCurrentToUnversioned verifies the core fix: when a TWD is deleted,
// the controller sets the current version to unversioned so tasks route to unversioned workers.
func testDeletionSetsCurrentToUnversioned(
	t *testing.T,
	k8sClient client.Client,
	ts *temporaltest.TestServer,
	namespace string,
) {
	ctx := context.Background()
	testName := "del-cleanup"

	// Build a TWD using the standard builder pattern
	tc := testhelpers.NewTestCase().
		WithInput(
			testhelpers.NewTemporalWorkerDeploymentBuilder().
				WithAllAtOnceStrategy().
				WithTargetTemplate("v1.0"),
		).
		WithExpectedStatus(
			testhelpers.NewStatusBuilder().
				WithTargetVersion("v1.0", temporaliov1alpha1.VersionStatusCurrent, -1, true, false).
				WithCurrentVersion("v1.0", true, false),
		).
		BuildWithValues(testName, namespace, ts.GetDefaultNamespace())

	twd := tc.GetTWD()

	// Create a TemporalConnection
	temporalConnection := &temporaliov1alpha1.TemporalConnection{
		ObjectMeta: metav1.ObjectMeta{
			Name:      twd.Spec.WorkerOptions.TemporalConnectionRef.Name,
			Namespace: namespace,
		},
		Spec: temporaliov1alpha1.TemporalConnectionSpec{
			HostPort: ts.GetFrontendHostPort(),
		},
	}
	if err := k8sClient.Create(ctx, temporalConnection); err != nil {
		t.Fatalf("failed to create TemporalConnection: %v", err)
	}

	// Create the TWD
	if err := k8sClient.Create(ctx, twd); err != nil {
		t.Fatalf("failed to create TWD: %v", err)
	}

	// Wait for the child deployment to be created by the controller
	workerDeploymentName := k8s.ComputeWorkerDeploymentName(twd)
	buildID := k8s.ComputeBuildID(twd)
	expectedDeploymentName := k8s.ComputeVersionedDeploymentName(twd.Name, buildID)

	eventually(t, 30*time.Second, time.Second, func() error {
		var dep appsv1.Deployment
		return k8sClient.Get(ctx, types.NamespacedName{
			Name: expectedDeploymentName, Namespace: namespace,
		}, &dep)
	})

	// Start workers so the version registers on the Temporal server
	workerStopFuncs := applyDeployment(t, ctx, k8sClient, expectedDeploymentName, namespace)
	defer handleStopFuncs(workerStopFuncs)

	// Wait until the version becomes current on the Temporal server
	deploymentHandle := ts.GetDefaultClient().WorkerDeploymentClient().GetHandle(workerDeploymentName)
	eventually(t, 60*time.Second, 2*time.Second, func() error {
		resp, err := deploymentHandle.Describe(ctx, sdkclient.WorkerDeploymentDescribeOptions{})
		if err != nil {
			return err
		}
		if resp.Info.RoutingConfig.CurrentVersion == nil {
			return errors.New("current version not set yet")
		}
		return nil
	})
	t.Log("TWD is reconciled with a current version set")

	// Delete the TWD
	t.Log("Deleting the TemporalWorkerDeployment")
	var latestTwd temporaliov1alpha1.TemporalWorkerDeployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: twd.Name, Namespace: namespace}, &latestTwd); err != nil {
		t.Fatalf("failed to re-fetch TWD: %v", err)
	}
	if err := k8sClient.Delete(ctx, &latestTwd); err != nil {
		t.Fatalf("failed to delete TWD: %v", err)
	}

	// Verify the TWD is eventually deleted (finalizer ran and was removed)
	eventually(t, 60*time.Second, 2*time.Second, func() error {
		var check temporaliov1alpha1.TemporalWorkerDeployment
		err := k8sClient.Get(ctx, types.NamespacedName{Name: twd.Name, Namespace: namespace}, &check)
		if err != nil {
			return nil // not found = deleted
		}
		return errors.New("TWD still exists, finalizer may not have completed")
	})
	t.Log("TWD deleted successfully (finalizer completed)")

	// Verify Temporal server-side state: current version should be unversioned
	resp, err := deploymentHandle.Describe(ctx, sdkclient.WorkerDeploymentDescribeOptions{})
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			t.Log("Worker Deployment was fully deleted from Temporal server")
			return
		}
		t.Fatalf("failed to describe worker deployment after deletion: %v", err)
	}

	if resp.Info.RoutingConfig.CurrentVersion != nil {
		t.Errorf("expected current version to be nil (unversioned) after TWD deletion, got buildID=%q",
			resp.Info.RoutingConfig.CurrentVersion.BuildId)
	} else {
		t.Log("Verified: current version is unversioned after TWD deletion")
	}
}

// testDeletionWithNoTemporalConnection verifies that deleting a TWD when the
// TemporalConnection has already been deleted doesn't block forever.
func testDeletionWithNoTemporalConnection(
	t *testing.T,
	k8sClient client.Client,
	ts *temporaltest.TestServer,
	namespace string,
) {
	ctx := context.Background()
	testName := "del-no-conn"

	// Build a TWD with manual strategy (simpler, no need to reach current version)
	tc := testhelpers.NewTestCase().
		WithInput(
			testhelpers.NewTemporalWorkerDeploymentBuilder().
				WithManualStrategy().
				WithTargetTemplate("v1.0"),
		).
		WithExpectedStatus(
			testhelpers.NewStatusBuilder().
				WithTargetVersion("v1.0", temporaliov1alpha1.VersionStatusInactive, -1, true, false),
		).
		BuildWithValues(testName, namespace, ts.GetDefaultNamespace())

	twd := tc.GetTWD()

	// Create a TemporalConnection
	temporalConnection := &temporaliov1alpha1.TemporalConnection{
		ObjectMeta: metav1.ObjectMeta{
			Name:      twd.Spec.WorkerOptions.TemporalConnectionRef.Name,
			Namespace: namespace,
		},
		Spec: temporaliov1alpha1.TemporalConnectionSpec{
			HostPort: ts.GetFrontendHostPort(),
		},
	}
	if err := k8sClient.Create(ctx, temporalConnection); err != nil {
		t.Fatalf("failed to create TemporalConnection: %v", err)
	}

	// Create the TWD
	if err := k8sClient.Create(ctx, twd); err != nil {
		t.Fatalf("failed to create TWD: %v", err)
	}

	// Wait for the finalizer to be added by the controller
	eventually(t, 30*time.Second, time.Second, func() error {
		var check temporaliov1alpha1.TemporalWorkerDeployment
		if err := k8sClient.Get(ctx, types.NamespacedName{Name: twd.Name, Namespace: namespace}, &check); err != nil {
			return err
		}
		for _, f := range check.Finalizers {
			if f == "temporal.io/worker-deployment-cleanup" {
				return nil
			}
		}
		return errors.New("finalizer not yet added")
	})
	t.Log("Finalizer has been added to TWD")

	// Delete the TemporalConnection FIRST (simulates the race condition)
	if err := k8sClient.Delete(ctx, temporalConnection); err != nil {
		t.Fatalf("failed to delete TemporalConnection: %v", err)
	}
	t.Log("Deleted TemporalConnection before TWD")

	// Delete the TWD
	var latestTwd temporaliov1alpha1.TemporalWorkerDeployment
	if err := k8sClient.Get(ctx, types.NamespacedName{Name: twd.Name, Namespace: namespace}, &latestTwd); err != nil {
		t.Fatalf("failed to re-fetch TWD: %v", err)
	}
	if err := k8sClient.Delete(ctx, &latestTwd); err != nil {
		t.Fatalf("failed to delete TWD: %v", err)
	}

	// Verify the TWD is eventually deleted (controller skips cleanup gracefully)
	eventually(t, 60*time.Second, 2*time.Second, func() error {
		var check temporaliov1alpha1.TemporalWorkerDeployment
		err := k8sClient.Get(ctx, types.NamespacedName{Name: twd.Name, Namespace: namespace}, &check)
		if err != nil {
			return nil // deleted
		}
		return errors.New("TWD still exists after connection was deleted")
	})
	t.Log("TWD deleted successfully even without TemporalConnection")
}
