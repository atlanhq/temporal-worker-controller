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

// A sibling being deleted takes its queues out of the new version on purpose, so the promotion
// must not be refused for them; otherwise the protection against dropping a queue stays on.
func TestUpdateVersionConfig_IgnoresMissingQueuesOnlyWhileASiblingIsDeleting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		sibling *temporaliov1alpha1.TemporalWorkerDeployment
		want    bool
	}{
		{"sibling being deleted", leavingTWD("app-size-s-twd", "app", true), true},
		{"live sibling", leavingTWD("app-size-s-twd", "app", false), false},
		{"no sibling", leavingTWD("other-twd", "other", true), false},
	} {
		for _, ramp := range []bool{false, true} {
			mode := "set current"
			if ramp {
				mode = "set ramping"
			}
			t.Run(tc.name+", "+mode, func(t *testing.T) {
				promoting := leavingTWD("app-worker-twd", "app", false)
				r := releaseReconciler(releaseTestClient(t, promoting, tc.sibling))
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
