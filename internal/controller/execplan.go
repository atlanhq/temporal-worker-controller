// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package controller

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *TemporalWorkerDeploymentReconciler) executePlan(ctx context.Context, l logr.Logger, temporalClient sdkclient.Client, p *plan) error {
	// Get deployment handler
	deploymentHandler := temporalClient.WorkerDeploymentClient().GetHandle(p.WorkerDeploymentName)

	// Create deployment
	if p.CreateDeployment != nil {
		l.Info("creating deployment", "deployment", p.CreateDeployment)
		if err := r.Create(ctx, p.CreateDeployment); err != nil {
			l.Error(err, "unable to create deployment", "deployment", p.CreateDeployment)
			return err
		}
	}

	// Delete deployments. Each of these is a drained, no-longer-polling version
	// (see planner.getDeleteDeployments, which now requires EligibleForDeletion
	// before adding a Drained version here). Deleting the Kubernetes object
	// alone would leave the corresponding Temporal server-side Worker
	// Deployment Version registered forever — the only other cleanup path is
	// the CRD-deletion finalizer, which never runs during normal rollout. This
	// is also the only point that can reliably do so: a deprecated version's
	// status entry only exists in status.DeprecatedVersions while its
	// Deployment still exists (see state_mapper.go), so there's no way to
	// retry this on a later reconcile once the Deployment is gone. Left
	// unpruned, these accumulate one per rollout indefinitely and eventually
	// hit the server's per-deployment version cap, at which point every future
	// rollout fails to register a new build ID (#377).
	for _, d := range p.DeleteDeployments {
		l.Info("deleting deployment", "deployment", d)
		if err := r.Delete(ctx, d); err != nil {
			l.Error(err, "unable to delete deployment", "deployment", d)
			return err
		}

		buildID, ok := d.GetLabels()[k8s.BuildIDLabel]
		if !ok {
			l.Info("deployment has no build ID label, skipping Temporal server-side version cleanup", "deployment", d.Name)
			continue
		}
		l.Info("deleting drained worker deployment version", "buildID", buildID)
		if _, err := deploymentHandler.DeleteVersion(ctx, sdkclient.WorkerDeploymentDeleteVersionOptions{
			BuildID:  buildID,
			Identity: getControllerIdentity(),
		}); err != nil {
			var notFound *serviceerror.NotFound
			if errors.As(err, &notFound) {
				continue
			}
			// Best-effort: the Kubernetes Deployment is already gone, which is
			// the primary action. A failure here (e.g. a transient RPC error,
			// or the poller-expiry race described above outlasting even
			// deleteDelay) means the Temporal-side record lingers until an
			// operator prunes it manually or the CRD itself is deleted.
			l.Info("could not delete worker deployment version, may require manual cleanup", "buildID", buildID, "error", err)
		}
	}
	// Scale deployments
	for d, replicas := range p.ScaleDeployments {
		l.Info("scaling deployment", "deployment", d, "replicas", replicas)
		dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{
			Namespace:       d.Namespace,
			Name:            d.Name,
			ResourceVersion: d.ResourceVersion,
			UID:             d.UID,
		}}

		scale := &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: int32(replicas)}}
		if err := r.Client.SubResource("scale").Update(ctx, dep, client.WithSubResourceBody(scale)); err != nil {
			l.Error(err, "unable to scale deployment", "deployment", d, "replicas", replicas)
			return fmt.Errorf("unable to scale deployment: %w", err)
		}
	}

	// Update deployments
	for _, d := range p.UpdateDeployments {
		l.Info("updating deployment", "deployment", d.Name, "namespace", d.Namespace)
		if err := r.Update(ctx, d); err != nil {
			l.Error(err, "unable to update deployment", "deployment", d)
			return fmt.Errorf("unable to update deployment: %w", err)
		}
	}

	for _, wf := range p.startTestWorkflows {
		// Log workflow start details
		if len(wf.input) > 0 {
			if wf.isInputSecret {
				// Don't log the actual input if it came from a Secret
				l.Info("starting gate workflow",
					"workflowType", wf.workflowType,
					"taskQueue", wf.taskQueue,
					"buildID", wf.buildID,
					"inputBytes", len(wf.input),
					"inputSource", "SecretRef (contents hidden)",
				)
			} else {
				// For non-secret sources, parse JSON and extract keys
				var inputKeys []string
				if len(wf.input) > 0 {
					var jsonData map[string]interface{}
					if err := json.Unmarshal(wf.input, &jsonData); err == nil {
						for key := range jsonData {
							inputKeys = append(inputKeys, key)
						}
					}
				}

				// Log the input keys for non-secret sources (inline or ConfigMap)
				l.Info("starting gate workflow",
					"workflowType", wf.workflowType,
					"taskQueue", wf.taskQueue,
					"buildID", wf.buildID,
					"inputBytes", len(wf.input),
					"inputKeys", inputKeys,
				)
			}
		} else {
			l.Info("starting gate workflow",
				"workflowType", wf.workflowType,
				"taskQueue", wf.taskQueue,
				"buildID", wf.buildID,
				"inputBytes", 0,
			)
		}
		opts := sdkclient.StartWorkflowOptions{
			ID:                       wf.workflowID,
			TaskQueue:                wf.taskQueue,
			WorkflowExecutionTimeout: time.Hour,
			WorkflowIDReusePolicy:    enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
			WorkflowIDConflictPolicy: enumspb.WORKFLOW_ID_CONFLICT_POLICY_FAIL,
			VersioningOverride: &sdkclient.PinnedVersioningOverride{
				Version: worker.WorkerDeploymentVersion{
					DeploymentName: p.WorkerDeploymentName,
					BuildId:        wf.buildID,
				},
			},
		}
		var err error
		if len(wf.input) > 0 {
			_, err = temporalClient.ExecuteWorkflow(ctx, opts, wf.workflowType, json.RawMessage(wf.input))
		} else {
			_, err = temporalClient.ExecuteWorkflow(ctx, opts, wf.workflowType)
		}
		if err != nil {
			return fmt.Errorf("unable to start test workflow execution: %w", err)
		}
	}

	// Register current version or ramps
	if vcfg := p.UpdateVersionConfig; vcfg != nil {
		if vcfg.SetCurrent {
			l.Info("registering new current version", "buildID", vcfg.BuildID)
			if _, err := deploymentHandler.SetCurrentVersion(ctx, sdkclient.WorkerDeploymentSetCurrentVersionOptions{
				BuildID:       vcfg.BuildID,
				ConflictToken: vcfg.ConflictToken,
				Identity:      getControllerIdentity(),
			}); err != nil {
				return fmt.Errorf("unable to set current deployment version: %w", err)
			}
		} else {
			if vcfg.RampPercentage > 0 {
				l.Info("applying ramp", "buildID", vcfg.BuildID, "percentage", vcfg.RampPercentage)
			} else {
				l.Info("deleting ramp", "buildID", vcfg.BuildID)
			}

			if _, err := deploymentHandler.SetRampingVersion(ctx, sdkclient.WorkerDeploymentSetRampingVersionOptions{
				BuildID:       vcfg.BuildID,
				Percentage:    float32(vcfg.RampPercentage),
				ConflictToken: vcfg.ConflictToken,
				Identity:      getControllerIdentity(),
			}); err != nil {
				return fmt.Errorf("unable to set ramping deployment version: %w", err)
			}
		}
		if _, err := deploymentHandler.UpdateVersionMetadata(ctx, sdkclient.WorkerDeploymentUpdateVersionMetadataOptions{
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: p.WorkerDeploymentName,
				BuildId:        vcfg.BuildID,
			},
			MetadataUpdate: sdkclient.WorkerDeploymentMetadataUpdate{
				UpsertEntries: map[string]interface{}{
					controllerIdentityMetadataKey: getControllerIdentity(),
					controllerVersionMetadataKey:  getControllerVersion(),
				},
			},
		}); err != nil { // would be cool to do this atomically with the update
			return fmt.Errorf("unable to update metadata after setting current deployment: %w", err)
		}
	}

	for _, buildId := range p.RemoveIgnoreLastModifierBuilds {
		if _, err := deploymentHandler.UpdateVersionMetadata(ctx, sdkclient.WorkerDeploymentUpdateVersionMetadataOptions{
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: p.WorkerDeploymentName,
				BuildId:        buildId,
			},
			MetadataUpdate: sdkclient.WorkerDeploymentMetadataUpdate{
				RemoveEntries: []string{temporal.IgnoreLastModifierKey},
			},
		}); err != nil {
			return fmt.Errorf("unable to update metadata to remove %s deployment: %w", temporal.IgnoreLastModifierKey, err)
		}
	}

	return nil
}
