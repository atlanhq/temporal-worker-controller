// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/planner"
	sdkclient "go.temporal.io/sdk/client"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// recordingWDHandle records the routing changes the controller asks for.
type recordingWDHandle struct {
	sdkclient.WorkerDeploymentHandle
	setCurrent []sdkclient.WorkerDeploymentSetCurrentVersionOptions
	setRamping []sdkclient.WorkerDeploymentSetRampingVersionOptions
}

func (h *recordingWDHandle) SetCurrentVersion(_ context.Context, o sdkclient.WorkerDeploymentSetCurrentVersionOptions) (sdkclient.WorkerDeploymentSetCurrentVersionResponse, error) {
	h.setCurrent = append(h.setCurrent, o)
	return sdkclient.WorkerDeploymentSetCurrentVersionResponse{}, nil
}

func (h *recordingWDHandle) SetRampingVersion(_ context.Context, o sdkclient.WorkerDeploymentSetRampingVersionOptions) (sdkclient.WorkerDeploymentSetRampingVersionResponse, error) {
	h.setRamping = append(h.setRamping, o)
	return sdkclient.WorkerDeploymentSetRampingVersionResponse{}, nil
}

func (h *recordingWDHandle) UpdateVersionMetadata(_ context.Context, _ sdkclient.WorkerDeploymentUpdateVersionMetadataOptions) (sdkclient.WorkerDeploymentUpdateVersionMetadataResponse, error) {
	return sdkclient.WorkerDeploymentUpdateVersionMetadataResponse{}, nil
}

func registered(twd *temporaliov1alpha1.TemporalWorkerDeployment, buildID string, status temporaliov1alpha1.VersionStatus) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd.Status.TargetVersion.BuildID = buildID
	twd.Status.TargetVersion.Status = status
	healthy := metav1.Now()
	twd.Status.TargetVersion.HealthySince = &healthy
	return twd
}

func unhealthy(twd *temporaliov1alpha1.TemporalWorkerDeployment) *temporaliov1alpha1.TemporalWorkerDeployment {
	twd.Status.TargetVersion.HealthySince = nil
	return twd
}

// A sibling being deleted takes its queues out of the new version on purpose, so the promotion may
// skip the server's missing-queue check for them, but only once every live sibling has registered
// the new version with its own workers for it healthy; otherwise a live pool's queue could be the
// one missing.
func TestUpdateVersionConfig_IgnoresMissingQueuesOnlyForADeletingSiblingsQueues(t *testing.T) {
	for _, tc := range []struct {
		name     string
		siblings []*temporaliov1alpha1.TemporalWorkerDeployment
		want     bool
	}{
		{"only a deleting sibling", []*temporaliov1alpha1.TemporalWorkerDeployment{leavingTWD("app-size-s-twd", "app", true)}, true},
		{"deleting sibling, live sibling registered on the new build", []*temporaliov1alpha1.TemporalWorkerDeployment{
			leavingTWD("app-size-s-twd", "app", true),
			registered(leavingTWD("app-heavy-twd", "app", false), "v2", temporaliov1alpha1.VersionStatusInactive)}, true},
		{"deleting sibling, live sibling not registered yet", []*temporaliov1alpha1.TemporalWorkerDeployment{
			leavingTWD("app-size-s-twd", "app", true),
			registered(leavingTWD("app-heavy-twd", "app", false), "v2", temporaliov1alpha1.VersionStatusNotRegistered)}, false},
		{"deleting sibling, live sibling's own workers not healthy yet", []*temporaliov1alpha1.TemporalWorkerDeployment{
			leavingTWD("app-size-s-twd", "app", true),
			unhealthy(registered(leavingTWD("app-heavy-twd", "app", false), "v2", temporaliov1alpha1.VersionStatusInactive))}, false},
		{"deleting sibling, live sibling on another build", []*temporaliov1alpha1.TemporalWorkerDeployment{
			leavingTWD("app-size-s-twd", "app", true),
			registered(leavingTWD("app-heavy-twd", "app", false), "v3", temporaliov1alpha1.VersionStatusInactive)}, false},
		{"live sibling only", []*temporaliov1alpha1.TemporalWorkerDeployment{
			registered(leavingTWD("app-heavy-twd", "app", false), "v2", temporaliov1alpha1.VersionStatusInactive)}, false},
		{"no sibling", []*temporaliov1alpha1.TemporalWorkerDeployment{leavingTWD("other-twd", "other", true)}, false},
	} {
		for _, ramp := range []bool{false, true} {
			mode := "set current"
			if ramp {
				mode = "set ramping"
			}
			t.Run(tc.name+", "+mode, func(t *testing.T) {
				promoting := leavingTWD("app-worker-twd", "app", false)
				objs := []client.Object{promoting}
				for _, s := range tc.siblings {
					objs = append(objs, s)
				}
				r := releaseReconciler(releaseTestClient(t, objs...))
				h := &recordingWDHandle{}
				vcfg := &planner.VersionConfig{BuildID: "v2", SetCurrent: !ramp, ManagerIdentity: getControllerIdentity()}
				if ramp {
					vcfg.RampPercentage = 25
				}

				require.NoError(t, r.updateVersionConfig(context.Background(), logr.Discard(), promoting, h,
					&plan{WorkerDeploymentName: "app", UpdateVersionConfig: vcfg}))

				if ramp {
					require.Len(t, h.setRamping, 1)
					assert.Equal(t, tc.want, h.setRamping[0].IgnoreMissingTaskQueues)
				} else {
					require.Len(t, h.setCurrent, 1)
					assert.Equal(t, tc.want, h.setCurrent[0].IgnoreMissingTaskQueues)
				}
			})
		}
	}
}
