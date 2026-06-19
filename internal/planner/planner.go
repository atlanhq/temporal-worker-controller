// Unless explicitly stated otherwise all files in this repository are licensed under the MIT License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/). Copyright 2024 Datadog, Inc.

package planner

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/go-logr/logr"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	"github.com/temporalio/temporal-worker-controller/internal/temporal"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Plan holds the actions to execute during reconciliation
type Plan struct {
	// Which actions to take
	DeleteDeployments []*appsv1.Deployment
	ScaleDeployments  map[*corev1.ObjectReference]uint32
	UpdateDeployments []*appsv1.Deployment
	// ShouldCreateDeployment is true when the target version is missing one or
	// more of its pool Deployments and the controller is allowed to create them.
	ShouldCreateDeployment bool
	// CreatePools lists the pools of the target version whose k8s Deployment does
	// not yet exist and must be created. For single-pool TWDs this is the single
	// implicit pool. Only meaningful when ShouldCreateDeployment is true.
	CreatePools   []temporaliov1alpha1.PoolSpec
	VersionConfig *VersionConfig
	TestWorkflows []WorkflowConfig
	// Build IDs of versions from which the controller should
	// remove IgnoreLastModifierKey from the version metadata
	RemoveIgnoreLastModifierBuilds []string
}

// VersionConfig defines version configuration for Temporal
type VersionConfig struct {
	// Token to use for conflict detection
	ConflictToken []byte
	// Build ID for the version
	BuildID string

	// One of RampPercentage OR SetCurrent must be set to a non-zero value.

	// Set this as the build ID for all new executions
	SetCurrent bool
	// Acceptable values [0,100]
	RampPercentage int32
}

// WorkflowConfig defines a workflow to be started
type WorkflowConfig struct {
	WorkflowType string
	WorkflowID   string
	BuildID      string
	TaskQueue    string
	GateInput    string
	// IsInputSecret indicates whether the GateInput came from a Secret reference
	// and should be treated as sensitive (not logged)
	IsInputSecret bool
}

// Config holds the configuration for planning
type Config struct {
	// RolloutStrategy to use
	RolloutStrategy temporaliov1alpha1.RolloutStrategy
}

// GeneratePlan creates a plan for updating the worker deployment
func GeneratePlan(
	l logr.Logger,
	k8sState *k8s.DeploymentState,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	temporalState *temporal.TemporalWorkerState,
	connection temporaliov1alpha1.TemporalConnectionSpec,
	config *Config,
	workerDeploymentName string,
	maxVersionsIneligibleForDeletion int32,
	gateInput []byte,
	isGateInputSecret bool,
) (*Plan, error) {
	plan := &Plan{
		ScaleDeployments: make(map[*corev1.ObjectReference]uint32),
	}

	// If Deployment was not found in temporal, which always happens on the first worker deployment version
	// and sometimes happens transiently thereafter, the versions list will be empty. If the deployment
	// exists and was found, there will always be at least one version in the list.
	foundDeploymentInTemporal := temporalState != nil && len(temporalState.Versions) > 0

	// Add delete/scale operations based on version status
	plan.DeleteDeployments = getDeleteDeployments(k8sState, status, spec, foundDeploymentInTemporal)
	plan.ScaleDeployments = getScaleDeployments(k8sState, status, spec)
	plan.UpdateDeployments = getUpdateDeployments(k8sState, status, spec, connection)

	// Determine which target-version pools (if any) still need a k8s Deployment.
	plan.CreatePools = getCreatePools(k8sState, status, spec, maxVersionsIneligibleForDeletion)
	plan.ShouldCreateDeployment = len(plan.CreatePools) > 0

	// Determine if we need to start any test workflows
	plan.TestWorkflows = getTestWorkflows(status, config, workerDeploymentName, gateInput, isGateInputSecret)

	// Determine version config changes
	plan.VersionConfig = getVersionConfigDiff(l, status, temporalState, config, workerDeploymentName)

	// Only remove the IgnoreLastModifier metadata after it's been used to make a version config change, which will
	// make the controller the LastModifier again
	if temporalState != nil && temporalState.IgnoreLastModifier && plan.VersionConfig != nil {
		if temporalState.RampingBuildID != "" {
			plan.RemoveIgnoreLastModifierBuilds = append(plan.RemoveIgnoreLastModifierBuilds, temporalState.RampingBuildID)
		}
		if temporalState.CurrentBuildID != "" {
			plan.RemoveIgnoreLastModifierBuilds = append(plan.RemoveIgnoreLastModifierBuilds, temporalState.CurrentBuildID)
		}
	}

	// TODO(jlegrone): generate warnings/events on the TemporalWorkerDeployment resource when buildIDs are reachable
	//                 but have no corresponding Deployment.

	return plan, nil
}

// checkAndUpdateDeploymentConnectionSpec determines whether the Deployment for the given buildID is
// out-of-date with respect to the provided TemporalConnectionSpec. If an update is required, it mutates
// the existing Deployment in-place and returns a pointer to that Deployment. If no update is needed or
// the Deployment does not exist, it returns nil.
func checkAndUpdateDeploymentConnectionSpec(
	existingDeployment *appsv1.Deployment,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) *appsv1.Deployment {
	if existingDeployment == nil {
		return nil
	}

	// If the connection spec hash has changed, update the deployment
	currentHash := k8s.ComputeConnectionSpecHash(connection)
	if currentHash != existingDeployment.Spec.Template.Annotations[k8s.ConnectionSpecHashAnnotation] {

		// Update the deployment in-place with new connection info
		updateDeploymentWithConnection(existingDeployment, connection)
		return existingDeployment // Return the modified deployment
	}

	return nil
}

// updateDeploymentWithConnection updates an existing deployment with new TemporalConnectionSpec
func updateDeploymentWithConnection(deployment *appsv1.Deployment, connection temporaliov1alpha1.TemporalConnectionSpec) {
	// Update the connection spec hash annotation
	deployment.Spec.Template.Annotations[k8s.ConnectionSpecHashAnnotation] = k8s.ComputeConnectionSpecHash(connection)

	// Update secret volume if mTLS is enabled
	if connection.MutualTLSSecretRef != nil {
		for i, volume := range deployment.Spec.Template.Spec.Volumes {
			if volume.Name == "temporal-tls" && volume.Secret != nil {
				deployment.Spec.Template.Spec.Volumes[i].Secret.SecretName = connection.MutualTLSSecretRef.Name
				break
			}
		}
	}

	// Update any environment variables that reference the connection
	for i, container := range deployment.Spec.Template.Spec.Containers {
		for j, env := range container.Env {
			if env.Name == "TEMPORAL_ADDRESS" {
				deployment.Spec.Template.Spec.Containers[i].Env[j].Value = connection.HostPort
			}
		}
	}
}

// checkAndUpdateDeploymentPodTemplateSpec determines whether the Deployment for the given buildID is
// out-of-date with respect to the user-provided pod template spec. This enables rolling updates when
// the build ID is stable (e.g., using spec.workerOptions.buildID) but the pod spec has changed.
// If an update is required, it rebuilds the deployment spec and returns a pointer to that Deployment.
// If no update is needed or the Deployment does not exist, it returns nil.
func checkAndUpdateDeploymentPodTemplateSpec(
	existingDeployment *appsv1.Deployment,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) *appsv1.Deployment {
	if existingDeployment == nil {
		return nil
	}

	// Only check for drift when UnsafeCustomBuildID is explicitly set by the user.
	// If buildID is auto-generated, any spec change would generate a new buildID anyway.
	if spec.WorkerOptions.UnsafeCustomBuildID == "" {
		return nil
	}

	// Get the stored hash from the existing deployment's pod template annotations
	storedHash := ""
	if existingDeployment.Spec.Template.Annotations != nil {
		storedHash = existingDeployment.Spec.Template.Annotations[k8s.PodTemplateSpecHashAnnotation]
	}

	// Backwards compatibility: if no hash annotation exists (legacy deployment),
	// don't trigger an update - the hash will be added on the next spec change
	if storedHash == "" {
		return nil
	}

	// Compute the hash of the current user-provided pod template spec
	currentHash := k8s.ComputePodTemplateSpecHash(spec.Template)

	// If hashes match, no drift detected
	if storedHash == currentHash {
		return nil
	}

	// Pod template has changed - rebuild the pod spec from the TWD spec
	// This applies all controller modifications (env vars, TLS mounts, etc.)
	updateDeploymentWithPodTemplateSpec(existingDeployment, spec, connection)

	return existingDeployment
}

// updateDeploymentWithPodTemplateSpec updates an existing deployment with a new pod template spec
// from the TWD spec. This applies all the controller modifications that NewDeploymentWithOwnerRef does.
func updateDeploymentWithPodTemplateSpec(
	deployment *appsv1.Deployment,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) {
	// Deep copy the user-provided pod spec to avoid mutating the original
	podSpec := spec.Template.Spec.DeepCopy()

	// Extract the build ID from the deployment's labels (with nil safety)
	var buildID string
	if deployment.Labels != nil {
		buildID = deployment.Labels[k8s.BuildIDLabel]
	}

	// Extract the worker deployment name from existing env vars
	var workerDeploymentName string
	for _, container := range deployment.Spec.Template.Spec.Containers {
		for _, env := range container.Env {
			if env.Name == "TEMPORAL_DEPLOYMENT_NAME" {
				workerDeploymentName = env.Value
				break
			}
		}
		if workerDeploymentName != "" {
			break
		}
	}

	// Resolve the pool this Deployment belongs to (empty == implicit pool) so we
	// reapply the pool's placement/sizing and task queue, not just the base template.
	poolName := ""
	if deployment.Labels != nil {
		poolName = deployment.Labels[k8s.PoolLabel]
	}
	pool := k8s.ResolvePoolByName(spec, poolName)

	// Apply controller-managed environment variables, volume mounts, pool
	// placement/sizing and task queue. Same shared helper as NewDeploymentForPool.
	k8s.ApplyPoolPodSpecModifications(podSpec, connection, spec.WorkerOptions.TemporalNamespace, workerDeploymentName, buildID, pool)

	// Build new pod annotations
	podAnnotations := make(map[string]string)
	for k, v := range spec.Template.Annotations {
		podAnnotations[k] = v
	}
	podAnnotations[k8s.ConnectionSpecHashAnnotation] = k8s.ComputeConnectionSpecHash(connection)
	// Store the new pod template spec hash
	podAnnotations[k8s.PodTemplateSpecHashAnnotation] = k8s.ComputePodTemplateSpecHash(spec.Template)

	// Preserve existing pod labels and add/update required labels
	podLabels := make(map[string]string)
	for k, v := range spec.Template.Labels {
		podLabels[k] = v
	}
	// Copy selector labels from existing deployment
	for k, v := range deployment.Spec.Selector.MatchLabels {
		podLabels[k] = v
	}

	// Update the deployment's pod template
	deployment.Spec.Template.ObjectMeta.Labels = podLabels
	deployment.Spec.Template.ObjectMeta.Annotations = podAnnotations
	deployment.Spec.Template.Spec = *podSpec

	// Update replicas if changed (honoring per-pool override)
	deployment.Spec.Replicas = poolReplicas(spec, pool)
	deployment.Spec.MinReadySeconds = spec.MinReadySeconds
}

// poolReplicas returns the effective replica count for a pool: the pool's
// override when set, otherwise spec.Replicas.
func poolReplicas(spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec, pool temporaliov1alpha1.PoolSpec) *int32 {
	if pool.Replicas != nil {
		return pool.Replicas
	}
	return spec.Replicas
}

func getUpdateDeployments(
	k8sState *k8s.DeploymentState,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	connection temporaliov1alpha1.TemporalConnectionSpec,
) []*appsv1.Deployment {
	var updateDeployments []*appsv1.Deployment
	// Track which builds we've already fully handled to avoid duplicates
	updatedBuildIDs := make(map[string]bool)

	// updatePoolsForBuild checks every pool Deployment of a build for drift /
	// stale connection spec, appending any that need updating.
	updatePoolsForBuild := func(buildID string, checkPodTemplate bool) {
		if buildID == "" || updatedBuildIDs[buildID] {
			return
		}
		updatedBuildIDs[buildID] = true
		for _, d := range sortedPoolDeployments(k8sState.PoolDeployments(buildID)) {
			// Pod template drift (only meaningful for the target version with a
			// stable custom build ID) takes precedence over a connection-only update.
			if checkPodTemplate {
				if deployment := checkAndUpdateDeploymentPodTemplateSpec(d, spec, connection); deployment != nil {
					updateDeployments = append(updateDeployments, deployment)
					continue
				}
			}
			if deployment := checkAndUpdateDeploymentConnectionSpec(d, connection); deployment != nil {
				updateDeployments = append(updateDeployments, deployment)
			}
		}
	}

	// Target version: check pod template drift + connection spec across all pools.
	updatePoolsForBuild(status.TargetVersion.BuildID, true)

	// Current version: connection spec only.
	if status.CurrentVersion != nil {
		updatePoolsForBuild(status.CurrentVersion.BuildID, false)
	}

	// Deprecated versions: connection spec only.
	for _, version := range status.DeprecatedVersions {
		updatePoolsForBuild(version.BuildID, false)
	}

	return updateDeployments
}

// getDeleteDeployments determines which deployments should be deleted
func getDeleteDeployments(
	k8sState *k8s.DeploymentState,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	foundDeploymentInTemporal bool,
) []*appsv1.Deployment {
	var deleteDeployments []*appsv1.Deployment

	for _, version := range status.DeprecatedVersions {
		if version.Deployment == nil {
			continue
		}

		// Look up all pool Deployments for this build. A version is only fully
		// deletable once every pool's Deployment is removed; here we emit a delete
		// for each pool Deployment that currently satisfies the delete criteria.
		poolDeployments := k8sState.PoolDeployments(version.BuildID)
		if len(poolDeployments) == 0 {
			continue
		}

		for _, d := range sortedPoolDeployments(poolDeployments) {
			switch version.Status {
			case temporaliov1alpha1.VersionStatusDrained:
				// Deleting a deployment is only possible when:
				// 1. The deployment has been drained for deleteDelay + scaledownDelay.
				// 2. The deployment is scaled to 0 replicas.
				if (time.Since(version.DrainedSince.Time) > spec.SunsetStrategy.DeleteDelay.Duration+spec.SunsetStrategy.ScaledownDelay.Duration) &&
					d.Spec.Replicas != nil && *d.Spec.Replicas == 0 {
					deleteDeployments = append(deleteDeployments, d)
				}
			case temporaliov1alpha1.VersionStatusNotRegistered:
				// Only delete Deployments of NotRegistered versions if temporalState was not empty
				if foundDeploymentInTemporal &&
					// NotRegistered versions are versions that the server doesn't know about.
					// Only delete if it's not the target version.
					status.TargetVersion.BuildID != version.BuildID {
					deleteDeployments = append(deleteDeployments, d)
				}
			}
		}
	}

	return deleteDeployments
}

// sortedPoolDeployments returns a build's pool Deployments in a deterministic
// (pool-name) order so that plan output is stable across reconciles.
func sortedPoolDeployments(byPool map[string]*appsv1.Deployment) []*appsv1.Deployment {
	names := make([]string, 0, len(byPool))
	for name := range byPool {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*appsv1.Deployment, 0, len(byPool))
	for _, name := range names {
		out = append(out, byPool[name])
	}
	return out
}

// getScaleDeployments determines which deployments should be scaled and to what size.
//
// It operates per (version, pool): each pool's "active" replica target is the
// pool's effective replica count (pool override or spec.replicas), and drained/
// inactive non-target pools are scaled to zero. For single-pool TWDs this is
// identical to the previous behavior.
func getScaleDeployments(
	k8sState *k8s.DeploymentState,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
) map[*corev1.ObjectReference]uint32 {
	scaleDeployments := make(map[*corev1.ObjectReference]uint32)

	// activeReplicasFor returns the effective replica count for a given pool.
	activeReplicasFor := func(poolName string) int32 {
		if r := poolReplicasByName(spec, poolName); r != nil {
			return *r
		}
		return 0
	}

	// scalePoolsTo records a scale op for each pool Deployment of buildID whose
	// current replica count differs from the desired count produced by want().
	scalePoolsTo := func(buildID string, want func(poolName string) (int32, bool)) {
		for poolName, d := range k8sState.PoolDeployments(buildID) {
			desired, ok := want(poolName)
			if !ok {
				continue
			}
			if d.Spec.Replicas == nil || *d.Spec.Replicas != desired {
				scaleDeployments[k8s.NewObjectRef(d)] = uint32(desired)
			}
		}
	}

	// Scale the current version up to its per-pool active replicas
	if status.CurrentVersion != nil && status.CurrentVersion.Deployment != nil {
		buildID := status.CurrentVersion.BuildID
		scalePoolsTo(buildID, func(poolName string) (int32, bool) {
			return activeReplicasFor(poolName), true
		})
	}

	// Scale the target version if it exists, and isn't current
	if (status.CurrentVersion == nil || status.CurrentVersion.BuildID != status.TargetVersion.BuildID) &&
		status.TargetVersion.Deployment != nil {
		buildID := status.TargetVersion.BuildID
		scalePoolsTo(buildID, func(poolName string) (int32, bool) {
			return activeReplicasFor(poolName), true
		})
	}

	// Scale other versions based on status
	for _, version := range status.DeprecatedVersions {
		if version.Deployment == nil {
			continue
		}
		if len(k8sState.PoolDeployments(version.BuildID)) == 0 {
			continue
		}

		switch version.Status {
		case temporaliov1alpha1.VersionStatusInactive:
			// Scale down inactive versions that are not the target
			if status.TargetVersion.BuildID == version.BuildID {
				scalePoolsTo(version.BuildID, func(poolName string) (int32, bool) {
					return activeReplicasFor(poolName), true
				})
			} else {
				scalePoolsTo(version.BuildID, func(poolName string) (int32, bool) {
					return 0, true
				})
			}
		case temporaliov1alpha1.VersionStatusRamping, temporaliov1alpha1.VersionStatusCurrent:
			// Scale up these deployments
			scalePoolsTo(version.BuildID, func(poolName string) (int32, bool) {
				return activeReplicasFor(poolName), true
			})
		case temporaliov1alpha1.VersionStatusDrained:
			if time.Since(version.DrainedSince.Time) > spec.SunsetStrategy.ScaledownDelay.Duration {
				// TODO(jlegrone): Compute scale based on load? Or percentage of replicas?
				// Scale down drained deployments after delay
				scalePoolsTo(version.BuildID, func(poolName string) (int32, bool) {
					return 0, true
				})
			}
		}
	}

	return scaleDeployments
}

// poolReplicasByName returns the effective replica override for a named pool,
// or spec.Replicas when the pool has no override or is unknown.
func poolReplicasByName(spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec, poolName string) *int32 {
	for _, p := range spec.EffectivePools() {
		if p.Name == poolName {
			if p.Replicas != nil {
				return p.Replicas
			}
			return spec.Replicas
		}
	}
	// Unknown pool (e.g. a pool removed from spec but still present in k8s):
	// fall back to spec.Replicas so we don't accidentally scale to zero based on
	// a missing override.
	return spec.Replicas
}

// getCreatePools determines which pools of the target version are missing their
// k8s Deployment and should be created. For single-pool TWDs this returns the
// single implicit pool when the target Deployment doesn't exist yet (matching
// the previous shouldCreateDeployment behavior), and nil otherwise.
func getCreatePools(
	k8sState *k8s.DeploymentState,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	spec *temporaliov1alpha1.TemporalWorkerDeploymentSpec,
	maxVersionsIneligibleForDeletion int32,
) []temporaliov1alpha1.PoolSpec {
	targetBuildID := status.TargetVersion.BuildID

	// Guardrail (unchanged): don't create new versions when too many versions are
	// ineligible for deletion. This is a per-version guard, so it only applies
	// when the target version has no Deployment in ANY pool yet.
	existingPools := k8sState.PoolDeployments(targetBuildID)
	if len(existingPools) == 0 {
		versionCountIneligibleForDeletion := int32(0)
		for _, v := range status.DeprecatedVersions {
			if !v.EligibleForDeletion {
				versionCountIneligibleForDeletion++
			}
		}
		if versionCountIneligibleForDeletion >= maxVersionsIneligibleForDeletion {
			return nil
		}
	}

	// Create any pool whose Deployment is missing for the target build.
	var missing []temporaliov1alpha1.PoolSpec
	for _, pool := range spec.EffectivePools() {
		if _, ok := existingPools[pool.Name]; !ok {
			missing = append(missing, pool)
		}
	}
	return missing
}

// getTestWorkflows determines which test workflows should be started
func getTestWorkflows(
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	config *Config,
	workerDeploymentName string,
	gateInput []byte,
	isGateInputSecret bool,
) []WorkflowConfig {
	var testWorkflows []WorkflowConfig

	// Skip if there's no gate workflow defined, if the target version is already the current, or if the target
	// version is not yet registered in temporal
	if config.RolloutStrategy.Gate == nil ||
		(status.CurrentVersion != nil && status.CurrentVersion.BuildID == status.TargetVersion.BuildID) ||
		status.TargetVersion.Status == temporaliov1alpha1.VersionStatusNotRegistered {
		return nil
	}

	targetVersion := status.TargetVersion

	// Create a map of task queues that already have running test workflows
	taskQueuesWithWorkflows := make(map[string]struct{})
	for _, wf := range targetVersion.TestWorkflows {
		taskQueuesWithWorkflows[wf.TaskQueue] = struct{}{}
	}

	// For each task queue without a running test workflow, create a config
	for _, tq := range targetVersion.TaskQueues {
		if _, ok := taskQueuesWithWorkflows[tq.Name]; !ok {
			testWorkflows = append(testWorkflows, WorkflowConfig{
				WorkflowType:  config.RolloutStrategy.Gate.WorkflowType,
				WorkflowID:    temporal.GetTestWorkflowID(workerDeploymentName, targetVersion.BuildID, tq.Name),
				BuildID:       targetVersion.BuildID,
				TaskQueue:     tq.Name,
				GateInput:     string(gateInput),
				IsInputSecret: isGateInputSecret,
			})
		}
	}

	return testWorkflows
}

// getVersionConfigDiff determines the version configuration based on the rollout strategy
func getVersionConfigDiff(
	l logr.Logger,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	temporalState *temporal.TemporalWorkerState,
	config *Config,
	workerDeploymentName string,
) *VersionConfig {
	strategy := config.RolloutStrategy
	conflictToken := status.VersionConflictToken

	if strategy.Strategy == temporaliov1alpha1.UpdateManual {
		return nil
	}

	// Do nothing if target version's deployment is not healthy yet, or if the version is not yet registered in temporal
	if status.TargetVersion.HealthySince == nil ||
		status.TargetVersion.Status == temporaliov1alpha1.VersionStatusNotRegistered {
		return nil
	}

	// Do nothing if the test workflows have not completed successfully
	if strategy.Gate != nil {
		if len(status.TargetVersion.TaskQueues) == 0 {
			return nil
		}
		if len(status.TargetVersion.TestWorkflows) < len(status.TargetVersion.TaskQueues) {
			return nil
		}
		for _, wf := range status.TargetVersion.TestWorkflows {
			if wf.Status != temporaliov1alpha1.WorkflowExecutionStatusCompleted {
				return nil
			}
		}
	}

	vcfg := &VersionConfig{
		ConflictToken: conflictToken,
		BuildID:       status.TargetVersion.BuildID,
	}

	// If there is no current version and presence of unversioned pollers is not confirmed for all
	// target version task queues, set the target version as the current version right away.
	if status.CurrentVersion == nil &&
		status.TargetVersion.Status == temporaliov1alpha1.VersionStatusInactive &&
		!temporalState.Versions[status.TargetVersion.BuildID].AllTaskQueuesHaveUnversionedPoller {
		vcfg.SetCurrent = true
		return vcfg
	}

	// If the current version is the target version
	if status.CurrentVersion != nil && status.CurrentVersion.BuildID == status.TargetVersion.BuildID {
		// Reset ramp if needed, this would happen if a ramp has been rolled back before completing
		if temporalState.RampingBuildID != "" {
			vcfg.BuildID = ""
			vcfg.RampPercentage = 0
			return vcfg
		}
		// Otherwise, do nothing
		return nil
	}

	switch strategy.Strategy {
	case temporaliov1alpha1.UpdateManual:
		return nil
	case temporaliov1alpha1.UpdateAllAtOnce:
		// Set new current version immediately
		vcfg.SetCurrent = true
		return vcfg
	case temporaliov1alpha1.UpdateProgressive:
		return handleProgressiveRollout(strategy.Steps, time.Now(), status.TargetVersion.RampLastModifiedAt, status.TargetVersion.RampPercentage, vcfg)
	}

	return nil
}

// handleProgressiveRollout handles the progressive rollout strategy logic
func handleProgressiveRollout(
	steps []temporaliov1alpha1.RolloutStep,
	currentTime time.Time, // avoid calling time.Now() inside function to make it easier to test
	rampLastModifiedAt *metav1.Time,
	targetRampPercentage *float32,
	vcfg *VersionConfig,
) *VersionConfig {
	// Protect against modifying the current version right away if there are no steps.
	//
	// The validating admission webhook _should_ prevent creating rollouts with 0 steps,
	// but just in case validation is skipped we should go with the more conservative
	// behavior of not updating the current version from the controller.
	if len(steps) == 0 {
		return nil
	}

	// Get the currently active step
	i := getCurrentStepIndex(steps, targetRampPercentage)
	currentStep := steps[i]

	// If this is the first step and there is no ramp percentage set, set the ramp percentage
	// to the step's ramp percentage.
	if targetRampPercentage == nil {
		vcfg.RampPercentage = int32(currentStep.RampPercentage)
		return vcfg
	}

	// If the target ramp percentage doesn't match the current step's defined ramp, the ramp
	// is reset immediately. This might be considered overly conservative, but it guarantees that
	// rollouts resume from the earliest possible step, and that at least the last step is always
	// respected (both % and duration).
	if *targetRampPercentage != float32(currentStep.RampPercentage) {
		vcfg.RampPercentage = int32(currentStep.RampPercentage)
		return vcfg
	}

	// Move to the next step if it has been long enough since the last update
	if rampLastModifiedAt != nil {
		if rampLastModifiedAt.Add(currentStep.PauseDuration.Duration).Before(currentTime) {
			if i < len(steps)-1 {
				vcfg.RampPercentage = int32(steps[i+1].RampPercentage)
				return vcfg
			} else {
				vcfg.SetCurrent = true
				return vcfg
			}
		}
	}

	// In all other cases, do nothing
	return nil
}

func getCurrentStepIndex(steps []temporaliov1alpha1.RolloutStep, targetRampPercentage *float32) int {
	if targetRampPercentage == nil {
		return 0
	}

	var result int
	for i, s := range steps {
		// Break if ramp percentage is greater than current (use last index)
		if float32(s.RampPercentage) > *targetRampPercentage {
			break
		}
		result = i
	}

	return result
}

// validateGateInputConfig validates that gate input is configured correctly
func validateGateInputConfig(gate *temporaliov1alpha1.GateWorkflowConfig) error {
	if gate == nil {
		return nil
	}
	// If both are set, return error (webhook should prevent this, but double-check)
	if gate.Input != nil && gate.InputFrom != nil {
		return errors.New("both spec.rollout.gate.input and spec.rollout.gate.inputFrom are set")
	}
	if gate.InputFrom == nil {
		return nil
	}
	// Exactly one of ConfigMapKeyRef or SecretKeyRef should be set
	cmSet := gate.InputFrom.ConfigMapKeyRef != nil
	secSet := gate.InputFrom.SecretKeyRef != nil
	if (cmSet && secSet) || (!cmSet && !secSet) {
		return errors.New("spec.rollout.gate.inputFrom must set exactly one of configMapKeyRef or secretKeyRef")
	}
	return nil
}

// ResolveGateInput resolves the gate input from inline JSON or from a referenced ConfigMap/Secret
// Returns the input bytes and a boolean indicating whether the input came from a Secret
func ResolveGateInput(gate *temporaliov1alpha1.GateWorkflowConfig, namespace string, configMapData map[string]string, configMapBinaryData map[string][]byte, secretData map[string][]byte) ([]byte, bool, error) {
	if gate == nil {
		return nil, false, nil
	}
	if err := validateGateInputConfig(gate); err != nil {
		return nil, false, err
	}
	if gate.Input != nil {
		return gate.Input.Raw, false, nil
	}
	if gate.InputFrom == nil {
		return nil, false, nil
	}
	if cmRef := gate.InputFrom.ConfigMapKeyRef; cmRef != nil {
		if configMapData != nil {
			if val, ok := configMapData[cmRef.Key]; ok {
				return []byte(val), false, nil
			}
		}
		if configMapBinaryData != nil {
			if bval, ok := configMapBinaryData[cmRef.Key]; ok {
				return bval, false, nil
			}
		}
		return nil, false, fmt.Errorf("key %q not found in ConfigMap %s/%s", cmRef.Key, namespace, cmRef.Name)
	}
	if secRef := gate.InputFrom.SecretKeyRef; secRef != nil {
		if secretData != nil {
			if bval, ok := secretData[secRef.Key]; ok {
				return bval, true, nil // true indicates this came from a Secret
			}
		}
		return nil, false, fmt.Errorf("key %q not found in Secret %s/%s", secRef.Key, namespace, secRef.Name)
	}
	return nil, false, nil
}
