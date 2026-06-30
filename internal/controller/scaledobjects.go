// Copyright 2025 The Atlan Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

// Package controller — per-version KEDA ScaledObject reconciliation (B2).
//
// Problem
// -------
// The KEDA ScaledObject for a worker app traditionally targets the
// TemporalWorkerDeployment (TWD) CRD. KEDA creates one HPA that writes to
// TWD.spec.replicas via the /scale subresource. The TWD controller's
// getScaleDeployments() then fans this single number out to every active child
// Deployment, so during a rollout total pods = N × num_active_versions.
//
// Fix
// ---
// The TWD controller manages one KEDA ScaledObject per active worker-deployment-
// version's child Deployment. Each ScaledObject's Temporal trigger embeds the
// version's BuildID, so KEDA's per-version-aware DescribeTaskQueueEnhanced and
// CountWorkflowExecutions queries return per-version stats. Each version's HPA
// scales its own Deployment independently. The multiplication bug goes away
// because there is no single replica number to fan out.
//
// Coordination protocol
// ---------------------
// - Each child Deployment we manage is labelled `twd.temporal.io/keda-managed=true`.
//   The planner reads this label and skips spec.replicas writes for those
//   Deployments, yielding scaling ownership to KEDA's HPA.
// - When a version drains, we perform a two-phase reconcile: cycle 1 deletes
//   the ScaledObject (which removes its child HPA via owner refs); cycle 2
//   sees no SO + no HPA and is then free to scale the Deployment to 0 via the
//   existing planner machinery. Until the HPA is gone, the planner's skip
//   label prevents fighting it.
// - To handle the inverse (HPA managing a version whose SO was just deleted
//   before label removal), we strip the `keda-managed` label as part of the
//   delete step.
//
// Rollout
// -------
// This is always-on once the image is deployed — there is no feature flag.
// Per-tenant rollout is controlled by which controller image each tenant
// is running. Behavior is backwards-compatible: on the first reconcile after
// upgrade, `deleteLegacyTWDScaledObject` removes any pre-B2 SO that targeted
// the TWD itself, then per-version SOs are created for active versions.

package controller

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// --- Constants ----------------------------------------------------------------

const (
	// ManagedByLabel is set on every Deployment whose scaling is owned by a
	// per-version ScaledObject (and therefore by KEDA's HPA). The planner uses
	// this label to skip its own spec.replicas writes for that Deployment.
	ManagedByLabel = "twd.temporal.io/keda-managed"

	// BuildIDLabel identifies which build a per-version ScaledObject (or
	// labelled Deployment) belongs to.
	BuildIDLabel = "twd.temporal.io/buildID"

	// OwnerTWDLabel records the TWD name on per-version ScaledObjects so the
	// listing query can scope to one TWD's SOs.
	OwnerTWDLabel = "twd.temporal.io/owner"

	// scaledObjectKind / scaledObjectAPIVersion identify the KEDA CRD without
	// importing the heavy KEDA module — we manipulate it as Unstructured.
	scaledObjectAPIVersion = "keda.sh/v1alpha1"
	scaledObjectKind       = "ScaledObject"

	// scaledObjectMaxNameLen is the K8s name length limit. Longer names are
	// hashed in ScaledObjectName().
	scaledObjectMaxNameLen = 63

	// scaledObjectSuffix appears on every per-version SO name.
	scaledObjectSuffix = "-scale"

	// scaledObjectFieldManager is the Server-Side Apply field manager under which
	// the controller owns only the ScaledObject fields it sets. Fields it never
	// sets — KEDA's keda.sh finalizer, the scaledobject.keda.sh/name label, and
	// server-defaulted spec such as advanced.scalingModifiers — stay owned by
	// KEDA and are left untouched.
	scaledObjectFieldManager = "temporal-worker-controller"
)

// --- Naming -------------------------------------------------------------------

// ScaledObjectName returns the per-version ScaledObject name for a given
// TWD + buildID. Format: <twdName>-<buildID>-scale. If the result exceeds
// K8s's 63-char DNS-label limit, the buildID is replaced with a short hash
// so the result fits.
func ScaledObjectName(twdName, buildID string) string {
	full := fmt.Sprintf("%s-%s%s", twdName, buildID, scaledObjectSuffix)
	if len(full) <= scaledObjectMaxNameLen {
		return full
	}
	// Truncated fallback: prefix(twdName) + "-" + sha1(buildID)[:8] + suffix.
	// Keep enough of the prefix to be recognizable; the rest is deterministic
	// from buildID so the SO is stable across reconciles.
	sum := sha1.Sum([]byte(buildID))
	hash := hex.EncodeToString(sum[:])[:8]
	// Budget: 63 - len("-") - 8 - len(suffix) = 63 - 1 - 8 - 6 = 48 for prefix
	const hashLen = 8
	maxPrefix := scaledObjectMaxNameLen - 1 - hashLen - len(scaledObjectSuffix)
	prefix := twdName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	return prefix + "-" + hash + scaledObjectSuffix
}

// --- Version enumeration ------------------------------------------------------

// versionRef is a minimal projection of a worker deployment version, just what's
// needed for SO construction. Decoupled from the per-status-bucket CRD types
// (which share no interface today).
type versionRef struct {
	BuildID    string
	Status     temporaliov1alpha1.VersionStatus
	Deployment *corev1.ObjectReference
	IsTarget   bool
}

// activeVersionsForScaling returns versions that should have a live SO.
// Drained versions are excluded — they'll be scaled to 0 by the existing
// planner machinery after the configured drain delay.
func activeVersionsForScaling(
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
) []versionRef {
	var out []versionRef

	if status.CurrentVersion != nil && status.CurrentVersion.Deployment != nil {
		out = append(out, versionRef{
			BuildID:    status.CurrentVersion.BuildID,
			Status:     status.CurrentVersion.Status,
			Deployment: status.CurrentVersion.Deployment,
		})
	}
	if status.TargetVersion.BuildID != "" &&
		status.TargetVersion.Deployment != nil &&
		(status.CurrentVersion == nil || status.TargetVersion.BuildID != status.CurrentVersion.BuildID) {
		out = append(out, versionRef{
			BuildID:    status.TargetVersion.BuildID,
			Status:     status.TargetVersion.Status,
			Deployment: status.TargetVersion.Deployment,
			IsTarget:   true,
		})
	}
	for _, v := range status.DeprecatedVersions {
		if v == nil || v.Deployment == nil {
			continue
		}
		if v.Status == temporaliov1alpha1.VersionStatusDrained {
			continue // planner handles drain-to-zero
		}
		out = append(out, versionRef{
			BuildID:    v.BuildID,
			Status:     v.Status,
			Deployment: v.Deployment,
		})
	}
	return out
}

// resolveMinReplicas returns the per-version MinReplicaCount and whether the
// value should be set on the ScaledObject. Returns (_, false) when nothing
// should be written (omit the field so KEDA's own default applies).
//
// Some versions are floored at 1 even when the user's configured min is 0,
// so Temporal always has somewhere to route work and we avoid a cold-start
// chicken-and-egg (no workers → no traffic → never scales up). This applies
// to Ramping and Inactive versions, and to a NotRegistered *target*: with no
// worker it can never poll Temporal to register its build ID, so a
// KEDA-managed target would read 0 backlog and stay at 0 forever - never
// registered, never promoted. The NotRegistered floor is scoped to the
// target so a stale version that was deleted server-side (also NotRegistered,
// pending cleanup) is not pinned. Once a version becomes Current the floor is
// released and the user's configured min applies.
func resolveMinReplicas(v versionRef, twd *temporaliov1alpha1.TemporalWorkerDeployment) (int64, bool) {
	var base int32
	var baseSet bool
	if twd.Spec.WorkerScaling != nil && twd.Spec.WorkerScaling.MinReplicaCount != nil {
		base = *twd.Spec.WorkerScaling.MinReplicaCount
		baseSet = true
	}

	// Warm-start invariant for new versions.
	if v.Status == temporaliov1alpha1.VersionStatusRamping ||
		v.Status == temporaliov1alpha1.VersionStatusInactive ||
		(v.IsTarget && v.Status == temporaliov1alpha1.VersionStatusNotRegistered) {
		if !baseSet || base < 1 {
			return 1, true
		}
	}

	if !baseSet {
		return 0, false // omit; let KEDA default
	}
	return int64(base), true
}

// resolveMaxReplicas returns the per-version MaxReplicaCount from the TWD's
// WorkerScaling. Returns (_, false) when unset — caller should omit the field
// so KEDA's own default applies.
func resolveMaxReplicas(twd *temporaliov1alpha1.TemporalWorkerDeployment) (int64, bool) {
	if twd.Spec.WorkerScaling != nil && twd.Spec.WorkerScaling.MaxReplicaCount != nil {
		return int64(*twd.Spec.WorkerScaling.MaxReplicaCount), true
	}
	return 0, false
}

// resolveTargetQueueSize returns the Temporal-scaler targetQueueSize from
// the TWD's WorkerScaling. Returns ("", false) when unset — caller should
// omit the field so the scaler's own default applies.
func resolveTargetQueueSize(twd *temporaliov1alpha1.TemporalWorkerDeployment) (string, bool) {
	if twd.Spec.WorkerScaling != nil && twd.Spec.WorkerScaling.TargetQueueSize != nil {
		return strconv.FormatInt(int64(*twd.Spec.WorkerScaling.TargetQueueSize), 10), true
	}
	return "", false
}

// --- Reconciler ---------------------------------------------------------------

// reconcileScaledObjects creates / updates / deletes ScaledObjects to match
// the set of active versions, and ensures each managed child Deployment is
// labelled so the planner skips it.
func (r *TemporalWorkerDeploymentReconciler) reconcileScaledObjects(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	temporalEndpoint string,
) error {
	l = l.WithValues("phase", "scaledObjects")

	// Step 0 — opt-out. When WorkerScaling is unset, the app has not opted
	// into per-version SO management (the chart's `keda.enabled: false`
	// path). Clean up anything we previously owned for this TWD, return
	// scaling ownership to the planner (strip managed labels), and exit.
	// Apps that previously had keda.enabled=true and now flip to false
	// land here on the next reconcile.
	if twd.Spec.WorkerScaling == nil {
		return r.disablePerVersionScaling(ctx, l, twd)
	}

	// Step 1 — enumerate desired SOs from active versions.
	versions := activeVersionsForScaling(&twd.Status)
	desired := make(map[string]*unstructured.Unstructured, len(versions))
	desiredVersionsByName := make(map[string]versionRef, len(versions))
	for _, v := range versions {
		so := buildScaledObject(twd, v, temporalEndpoint)
		desired[so.GetName()] = so
		desiredVersionsByName[so.GetName()] = v
	}

	// Step 2 — list existing SOs we own.
	existing, err := r.listOwnedScaledObjects(ctx, twd)
	if err != nil {
		return fmt.Errorf("list scaled objects: %w", err)
	}

	// Step 3 — migration: delete any LEGACY SO that targets the TWD itself
	//          (scaleTargetRef.kind=TemporalWorkerDeployment). The new scheme
	//          targets child Deployments only.
	if err := r.deleteLegacyTWDScaledObject(ctx, l, twd); err != nil {
		// don't block reconciliation on this — log and continue
		l.Error(err, "failed to delete legacy TWD-targeting ScaledObject")
	}

	// Step 4 — create / update.
	for name, want := range desired {
		v := desiredVersionsByName[name]
		// Ensure the child Deployment is labelled so planner yields.
		if err := r.ensureDeploymentManagedLabel(ctx, v.Deployment.Namespace, v.Deployment.Name, true); err != nil {
			l.Error(err, "failed to label Deployment as managed", "deployment", v.Deployment.Name)
			// continue — best effort
		}
		if err := r.applyDesiredScaledObject(ctx, l, name, want, existing[name], v); err != nil {
			return err
		}
	}

	// Step 5 — delete stale (version no longer active).
	for name, so := range existing {
		if _, want := desired[name]; want {
			continue
		}
		buildID := so.GetLabels()[BuildIDLabel]
		l.Info("deleting stale ScaledObject", "name", name, "buildId", buildID)

		// Strip the managed-by label off the Deployment first so the planner
		// is free to drive replicas to zero on the next cycle.
		if deployName, deployNS, ok := scaleTargetRef(so); ok {
			if err := r.ensureDeploymentManagedLabel(ctx, deployNS, deployName, false); err != nil {
				l.Error(err, "failed to remove managed label", "deployment", deployName)
			}
		}

		if err := r.Delete(ctx, so); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete scaledobject %s: %w", name, err)
		}
	}

	return nil
}

// disablePerVersionScaling is the opt-out path: clean up any SOs we previously
// created for this TWD, strip the keda-managed label from child Deployments
// (so the planner takes back replica control), and also remove any legacy
// TWD-targeting SO. Called when twd.Spec.WorkerScaling is nil.
func (r *TemporalWorkerDeploymentReconciler) disablePerVersionScaling(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) error {
	existing, err := r.listOwnedScaledObjects(ctx, twd)
	if err != nil {
		return fmt.Errorf("list scaled objects (disable path): %w", err)
	}
	for name, so := range existing {
		buildID := so.GetLabels()[BuildIDLabel]
		l.Info("disabling per-version scaling — deleting owned ScaledObject",
			"name", name, "buildId", buildID)
		// Strip the managed-by label so the planner is free to drive replicas.
		if deployName, deployNS, ok := scaleTargetRef(so); ok {
			if err := r.ensureDeploymentManagedLabel(ctx, deployNS, deployName, false); err != nil {
				l.Error(err, "failed to remove managed label", "deployment", deployName)
			}
		}
		if err := r.Delete(ctx, so); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete scaledobject %s: %w", name, err)
		}
	}
	// Also handle the legacy SO if it's still around from before the bump.
	if err := r.deleteLegacyTWDScaledObject(ctx, l, twd); err != nil {
		l.Error(err, "failed to delete legacy TWD-targeting ScaledObject (disable path)")
	}
	return nil
}

// listOwnedScaledObjects lists ScaledObjects this TWD owns. Filters by both
// owner reference and the OwnerTWDLabel to be defensive against label drift.
func (r *TemporalWorkerDeploymentReconciler) listOwnedScaledObjects(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) (map[string]*unstructured.Unstructured, error) {
	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda.sh",
		Version: "v1alpha1",
		Kind:    "ScaledObjectList",
	})
	if err := r.List(ctx, &list,
		client.InNamespace(twd.Namespace),
		client.MatchingLabels{OwnerTWDLabel: twd.Name},
	); err != nil {
		return nil, err
	}

	out := make(map[string]*unstructured.Unstructured, len(list.Items))
	for i := range list.Items {
		item := list.Items[i]
		// Defensive ownerRef check: only return SOs we actually own.
		if !ownedBy(&item, twd) {
			continue
		}
		out[item.GetName()] = &item
	}
	return out, nil
}

// deleteLegacyTWDScaledObject removes any pre-B2 ScaledObject that targets the
// TWD itself (scaleTargetRef.kind == TemporalWorkerDeployment). Without this,
// the legacy SO would coexist with the new per-version SOs and double up.
// Idempotent: no-op if no legacy SO present.
func (r *TemporalWorkerDeploymentReconciler) deleteLegacyTWDScaledObject(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) error {
	var list unstructured.UnstructuredList
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda.sh",
		Version: "v1alpha1",
		Kind:    "ScaledObjectList",
	})
	if err := r.List(ctx, &list, client.InNamespace(twd.Namespace)); err != nil {
		return err
	}

	for i := range list.Items {
		so := list.Items[i]
		kind, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "kind")
		name, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
		if kind != "TemporalWorkerDeployment" {
			continue
		}
		if name != twd.Name {
			continue
		}
		l.Info("deleting legacy TWD-targeting ScaledObject (migration)", "name", so.GetName())
		if err := r.Delete(ctx, &so); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete legacy SO %s: %w", so.GetName(), err)
		}
	}
	return nil
}

// ensureDeploymentManagedLabel sets or removes the `keda-managed` label on
// a child Deployment. The planner uses this label to skip its own scaling.
func (r *TemporalWorkerDeploymentReconciler) ensureDeploymentManagedLabel(
	ctx context.Context,
	namespace, name string,
	want bool,
) error {
	var dep appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, &dep); err != nil {
		if apierrors.IsNotFound(err) {
			return nil // nothing to label
		}
		return err
	}

	has := dep.GetLabels()[ManagedByLabel] == "true"
	if has == want {
		return nil
	}

	patch := client.MergeFrom(dep.DeepCopy())
	if dep.Labels == nil {
		dep.Labels = map[string]string{}
	}
	if want {
		dep.Labels[ManagedByLabel] = "true"
	} else {
		delete(dep.Labels, ManagedByLabel)
	}
	return r.Patch(ctx, &dep, patch)
}

// --- ScaledObject construction ------------------------------------------------

// buildScaledObject constructs the per-version ScaledObject as Unstructured.
// We use Unstructured to avoid importing the KEDA module's Go types — the
// schema we set is the public, stable keda.sh/v1alpha1 ScaledObject shape.
func buildScaledObject(
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	v versionRef,
	temporalEndpoint string,
) *unstructured.Unstructured {
	so := &unstructured.Unstructured{}
	so.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "keda.sh",
		Version: "v1alpha1",
		Kind:    scaledObjectKind,
	})
	so.SetName(ScaledObjectName(twd.Name, v.BuildID))
	so.SetNamespace(twd.Namespace)
	so.SetLabels(map[string]string{
		OwnerTWDLabel: twd.Name,
		BuildIDLabel:  v.BuildID,
	})
	so.SetOwnerReferences([]metav1.OwnerReference{
		*metav1.NewControllerRef(twd, twd.GroupVersionKind()),
	})

	// Resolve scaling config from TWD spec — no defaults hardcoded in the
	// controller. Unset values are omitted from the SO so KEDA's own
	// defaults apply.
	//
	// The controller sets endpoint/namespace/taskQueue/workerDeploymentName/
	// workerDeploymentBuildId on the trigger since those derive from the TWD
	// context. All other Temporal-scaler metadata flows through from
	// twd.Spec.WorkerScaling. KEDA expects all triggerMetadata values as
	// strings.
	//
	// Field names match upstream KEDA v2.20.1's schema (workerDeploymentName +
	// workerDeploymentBuildId). The fork (atlanhq/keda 2.19.0-main) accepts
	// these via the rename PR companion to this change.
	triggerMetadata := map[string]interface{}{
		"endpoint":                temporalEndpoint,
		"namespace":               twd.Spec.WorkerOptions.TemporalNamespace,
		"taskQueue":               resolveTaskQueue(twd),
		"workerDeploymentName":    resolveWorkerDeployment(twd),
		"workerDeploymentBuildId": v.BuildID,
	}
	// Current and Ramping versions catch unassigned backlog so they can scale
	// from zero. In Temporal's worker-deployment-versioning model, newly-queued
	// workflows have no version search attribute until a worker picks them up,
	// and matching routes them at AddTask time but spools the task in the
	// default (unversioned) partition until sync-match succeeds. Per-build
	// scoped queries (TaskQueueVersionSelection.BuildIDs / TemporalWorker
	// DeploymentVersion) return 0 for these spooled tasks. Setting
	// selectAllActive + selectUnversioned on both Current and Ramping routes
	// their DescribeTaskQueueEnhanced query through the all-active +
	// unversioned buckets so they see the spooled work and wake a worker up.
	//
	// Ramping needs this too: when matching pre-routes a task to Ramping's
	// version (via deterministic workflowId hash + routing %), the task sits
	// in the default partition with syncMatchQueue=Ramping. Without the flags,
	// Ramping's per-build SO query returns 0, no Ramping pod starts, and the
	// task is stuck (Current workers don't poll the default partition; only
	// the version Ramping points at can drain it).
	//
	// Drained/Deprecated versions stay strictly per-build-scoped: they only
	// see workflows already pinned to them, which is the correct behavior
	// (they should never reach into the unversioned bucket).
	//
	// Trade-off: Current and Ramping double-count the same default-partition
	// backlog, so both can scale up in response to the same spooled task. The
	// over-provisioning is contained to the cold-start window when ramping is
	// active; once both versions are warm, sync-match succeeds at AddTask time
	// and the default partition stays empty.
	if v.Status == temporaliov1alpha1.VersionStatusCurrent ||
		v.Status == temporaliov1alpha1.VersionStatusRamping {
		triggerMetadata["selectAllActive"] = "true"
		triggerMetadata["selectUnversioned"] = "true"
	}
	setTriggerMetadata(triggerMetadata, twd)

	spec := map[string]interface{}{
		"scaleTargetRef": map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"name":       v.Deployment.Name,
		},
		"triggers": []interface{}{
			map[string]interface{}{
				"type":     "temporal",
				"metadata": triggerMetadata,
			},
		},
	}
	if minR, ok := resolveMinReplicas(v, twd); ok {
		spec["minReplicaCount"] = minR
	}
	if maxR, ok := resolveMaxReplicas(twd); ok {
		spec["maxReplicaCount"] = maxR
	}
	setScaledObjectSpec(spec, twd)

	_ = unstructured.SetNestedField(so.Object, spec, "spec")
	return so
}

// setTriggerMetadata writes the optional Temporal-scaler triggerMetadata
// fields from twd.Spec.WorkerScaling into m. Fields are omitted when unset
// so the KEDA scaler's own defaults apply. KEDA expects all triggerMetadata
// values to be strings.
func setTriggerMetadata(m map[string]interface{}, twd *temporaliov1alpha1.TemporalWorkerDeployment) {
	ws := twd.Spec.WorkerScaling
	if ws == nil {
		return
	}
	if ws.TargetQueueSize != nil {
		m["targetQueueSize"] = strconv.FormatInt(int64(*ws.TargetQueueSize), 10)
	}
	if ws.ActivationTargetQueueSize != nil {
		m["activationTargetQueueSize"] = strconv.FormatInt(int64(*ws.ActivationTargetQueueSize), 10)
	}
	if ws.ActivitySlotsPerWorker != nil {
		m["activitySlotsPerWorker"] = strconv.FormatInt(int64(*ws.ActivitySlotsPerWorker), 10)
	}
	if ws.GateSlotsOnRunningWorkflow != nil {
		m["gateSlotsOnRunningWorkflow"] = strconv.FormatBool(*ws.GateSlotsOnRunningWorkflow)
	}
	if len(ws.QueueTypes) > 0 {
		m["queueTypes"] = strings.Join(ws.QueueTypes, ",")
	}
	if ws.IncludeRunningWorkflowCount != nil {
		m["includeRunningWorkflowCount"] = strconv.FormatBool(*ws.IncludeRunningWorkflowCount)
	}
	if ws.WorkflowTaskQueueForCount != "" {
		m["workflowTaskQueueForCount"] = ws.WorkflowTaskQueueForCount
	}
	if ws.WorkerMetricsPort != nil {
		m["workerMetricsPort"] = strconv.FormatInt(int64(*ws.WorkerMetricsPort), 10)
	}
	if ws.MinConnectTimeout != nil {
		m["minConnectTimeout"] = strconv.FormatInt(int64(*ws.MinConnectTimeout), 10)
	}
}

// setScaledObjectSpec writes the optional ScaledObject-level fields (other
// than min/max which the caller already handled) from twd.Spec.WorkerScaling
// into spec. Fields are omitted when unset so KEDA's own defaults apply.
func setScaledObjectSpec(spec map[string]interface{}, twd *temporaliov1alpha1.TemporalWorkerDeployment) {
	ws := twd.Spec.WorkerScaling
	if ws == nil {
		return
	}
	if ws.IdleReplicaCount != nil {
		spec["idleReplicaCount"] = int64(*ws.IdleReplicaCount)
	}
	if ws.PollingInterval != nil {
		spec["pollingInterval"] = int64(*ws.PollingInterval)
	}
	if ws.CooldownPeriod != nil {
		spec["cooldownPeriod"] = int64(*ws.CooldownPeriod)
	}
	if ws.InitialCooldownPeriod != nil {
		spec["initialCooldownPeriod"] = int64(*ws.InitialCooldownPeriod)
	}
	if ws.Fallback != nil {
		fb := map[string]interface{}{}
		if ws.Fallback.FailureThreshold != nil {
			fb["failureThreshold"] = int64(*ws.Fallback.FailureThreshold)
		}
		if ws.Fallback.Replicas != nil {
			fb["replicas"] = int64(*ws.Fallback.Replicas)
		}
		if ws.Fallback.Behavior != "" {
			fb["behavior"] = ws.Fallback.Behavior
		}
		if len(fb) > 0 {
			spec["fallback"] = fb
		}
	}
	if ws.Advanced != nil && len(ws.Advanced.Raw) > 0 {
		var adv map[string]interface{}
		// If the raw JSON is malformed, fall back to omitting — the CRD
		// validator should reject this earlier in practice.
		if err := json.Unmarshal(ws.Advanced.Raw, &adv); err == nil && len(adv) > 0 {
			spec["advanced"] = adv
		}
	}
}

// applyDesiredScaledObject reconciles the per-version ScaledObject toward `want`
// using Server-Side Apply with a dedicated field manager. The controller owns
// only the fields buildScaledObject sets; fields it never sets — KEDA's keda.sh
// finalizer, the scaledobject.keda.sh/name label, and server-defaulted spec such
// as advanced.scalingModifiers — remain owned by KEDA and are left untouched.
// In steady state the apply is a server-side no-op, so KEDA's finalizer and the
// object's generation stay put rather than churning every reconcile. `got` only
// selects the log verb.
func (r *TemporalWorkerDeploymentReconciler) applyDesiredScaledObject(
	ctx context.Context,
	l logr.Logger,
	name string,
	want, got *unstructured.Unstructured,
	v versionRef,
) error {
	if got == nil {
		l.Info("creating ScaledObject", "name", name, "buildId", v.BuildID)
	} else {
		l.V(1).Info("applying ScaledObject", "name", name, "buildId", v.BuildID)
	}
	if err := r.Patch(ctx, want, client.Apply,
		client.FieldOwner(scaledObjectFieldManager),
		client.ForceOwnership,
	); err != nil {
		return fmt.Errorf("apply scaledobject %s: %w", name, err)
	}
	return nil
}

// resolveTaskQueue returns the actual Temporal task queue name workers poll.
// Reads from twd.Spec.WorkerScaling.TaskQueue (chart-populated). Returns ""
// if unset — caller may choose to omit the trigger metadata field, which
// would make the SO non-functional. The previous implementation returned
// "<namespace>:<name>" (the worker-deployment-name, NOT a task queue),
// which made the SO query the wrong attribute and never see backlog.
func resolveTaskQueue(twd *temporaliov1alpha1.TemporalWorkerDeployment) string {
	if twd.Spec.WorkerScaling != nil && twd.Spec.WorkerScaling.TaskQueue != "" {
		return twd.Spec.WorkerScaling.TaskQueue
	}
	return ""
}

// resolveWorkerDeployment returns the Temporal worker-deployment-name for use
// in the trigger metadata. The KEDA Temporal scaler combines this with
// buildId to query "TemporalWorkerDeploymentVersion = '<dep>:<bid>'" — the
// canonical, non-deprecated way to count workflows pinned to / assigned to
// a specific version. Format is "<namespace>/<twd-name>" matching the
// TEMPORAL_DEPLOYMENT_NAME env on worker pods.
func resolveWorkerDeployment(twd *temporaliov1alpha1.TemporalWorkerDeployment) string {
	return twd.Namespace + "/" + twd.Name
}

// --- Helpers ------------------------------------------------------------------

// scaleTargetRef extracts the Deployment name and namespace from an SO's
// scaleTargetRef. Returns ok=false if any field is missing.
func scaleTargetRef(so *unstructured.Unstructured) (name, namespace string, ok bool) {
	n, found, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
	if !found || n == "" {
		return "", "", false
	}
	// scaleTargetRef doesn't carry a namespace; it implicitly matches the SO's.
	return n, so.GetNamespace(), true
}

// ownedBy returns true if obj has an owner reference pointing to twd.
func ownedBy(obj client.Object, twd *temporaliov1alpha1.TemporalWorkerDeployment) bool {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.UID == twd.UID {
			return true
		}
		if ref.Kind == "TemporalWorkerDeployment" && ref.Name == twd.Name {
			return true
		}
	}
	return false
}

// IsDeploymentKEDAManaged reports whether the given Deployment is labelled as
// managed by a per-version ScaledObject. Used by the planner (or its caller)
// to decide whether to skip writing spec.replicas — letting KEDA's HPA own
// the value instead.
//
// Exported so the planner package can call it without importing internal
// state. Cheap pure function: just label lookup.
func IsDeploymentKEDAManaged(d *appsv1.Deployment) bool {
	if d == nil {
		return false
	}
	return strings.EqualFold(d.GetLabels()[ManagedByLabel], "true")
}
