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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/go-logr/logr"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
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

	// VariantSOLabel records the spec.variants name on a variant's per-version
	// ScaledObject (absent on base SOs).
	VariantSOLabel = "twd.temporal.io/variant"

	// specHashAnnotation stores a hash of the controller-managed content
	// (labels + spec) that buildScaledObject produces. It lets the reconciler
	// skip the server-side apply when the live object already carries the same
	// hash, so a converged SO is not PATCHed on every ~10s requeue.
	specHashAnnotation = "twd.temporal.io/spec-hash"

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

// ScaledObjectName returns the per-version ScaledObject name for a TWD version,
// or for one of that version's spec.variants when variant is non-empty. Format:
// <twdName>[-<variant>]-<buildID>-scale. If the result exceeds K8s's 63-char
// DNS-label limit, the buildID is replaced with a short hash so the result fits.
//
// The hash covers twdName, variant and buildID together. Hashing the buildID
// alone is not enough: a variant registers under the SAME buildID as its base
// by design, so base and every variant of a version would hash identically.
// The variant is also excluded from the truncated segment so it stays readable
// on a long twdName instead of being cut off the end — an operator reading
// `kubectl get scaledobject` during an incident needs to see which tier is which.
func ScaledObjectName(twdName, variant, buildID string) string {
	var variantPart string
	if variant != "" {
		variantPart = "-" + variant
	}
	full := fmt.Sprintf("%s%s-%s%s", twdName, variantPart, buildID, scaledObjectSuffix)
	if len(full) <= scaledObjectMaxNameLen {
		return full
	}
	// Truncated fallback:
	//   prefix(twdName) + [-variant] + "-" + sha1(twdName/variant/buildID)[:8] + suffix
	// "/" cannot appear in a TWD name, a variant name or a build ID, so the hash
	// input is unambiguous across the three fields. The digest is deterministic,
	// so the SO name is stable across reconciles.
	sum := sha1.Sum([]byte(twdName + "/" + variant + "/" + buildID))
	hash := hex.EncodeToString(sum[:])[:8]
	// Budget: 63 - len(variantPart) - len("-") - 8 - len(suffix). variantPart is
	// bounded by the CRD's MaxLength=20 on spec.variants[].name, which keeps at
	// least 27 characters of twdName.
	const hashLen = 8
	maxPrefix := scaledObjectMaxNameLen - len(variantPart) - 1 - hashLen - len(scaledObjectSuffix)
	if maxPrefix < 0 {
		maxPrefix = 0
	}
	prefix := twdName
	if len(prefix) > maxPrefix {
		prefix = prefix[:maxPrefix]
	}
	// A cut landing on a separator would leave "...--<variant>" or "...--<hash>".
	prefix = strings.TrimRight(prefix, "-.")
	return prefix + variantPart + "-" + hash + scaledObjectSuffix
}

// warnOnSONameCollision reports two versions of one TWD resolving to the same
// ScaledObject name. Such a pair collapses in the reconciler's desired-state map
// with no API error to surface: the loser is dropped, never gets the
// keda-managed label, and its Deployment silently runs on the planner's static
// replicas instead of scaling on backlog. ScaledObjectName is injective over
// (twdName, variant, buildID), so a collision is a controller bug rather than a
// user-input problem — reconciliation continues for the versions that are fine,
// but the event and log make the gap visible.
func (r *TemporalWorkerDeploymentReconciler) warnOnSONameCollision(
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	names []string,
) {
	dupes := duplicateStrings(names)
	if len(dupes) == 0 {
		return
	}
	joined := strings.Join(dupes, ", ")
	l.Error(fmt.Errorf("colliding names: %s", joined),
		"two versions resolved to the same ScaledObject name; the losing Deployments will not autoscale")
	r.Recorder.Eventf(twd, corev1.EventTypeWarning, ReasonScaledObjectNameCollision,
		"ScaledObject name collision (%s): a version or variant will not be autoscaled", joined)
}

// duplicateStrings returns the values appearing more than once, each reported
// once, in first-seen order.
func duplicateStrings(values []string) []string {
	counts := make(map[string]int, len(values))
	var dupes []string
	for _, v := range values {
		counts[v]++
		if counts[v] == 2 {
			dupes = append(dupes, v)
		}
	}
	return dupes
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
	// RecordedWDN is the Temporal worker deployment name this version's pods
	// actually registered under, read from the TEMPORAL_DEPLOYMENT_NAME env
	// var recorded on its Deployment at creation time. For versions left
	// behind by a spec.workerOptions.workerDeploymentName change this differs
	// from the currently-resolved name, and their per-version Temporal stats
	// live under the recorded name — KEDA's queries must be aimed there.
	// Empty when the Deployment couldn't be read.
	RecordedWDN string
	// Variant, when non-nil, marks this ref as a spec.variants child of the
	// version: Deployment points at the variant's Deployment and the generated
	// SO watches <taskQueue><variant.TaskQueueSuffix> under the SAME
	// {workerDeploymentName, buildID}. Nil for the base.
	Variant *temporaliov1alpha1.WorkerVariant
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

// variantVersionsForScaling expands the base version refs with one ref per
// (version, spec.variant) pair whose variant Deployment exists in status. A
// status variant no longer declared in the spec is skipped - its SO then falls
// out of the desired set and is deleted as stale.
func variantVersionsForScaling(
	baseVersions []versionRef,
	status *temporaliov1alpha1.TemporalWorkerDeploymentStatus,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) []versionRef {
	if len(twd.Spec.Variants) == 0 {
		return nil
	}
	specVariants := make(map[string]*temporaliov1alpha1.WorkerVariant, len(twd.Spec.Variants))
	for i := range twd.Spec.Variants {
		specVariants[twd.Spec.Variants[i].Name] = &twd.Spec.Variants[i]
	}
	statusVariants := func(buildID string) []temporaliov1alpha1.VariantStatus {
		if status.CurrentVersion != nil && status.CurrentVersion.BuildID == buildID {
			return status.CurrentVersion.Variants
		}
		if status.TargetVersion.BuildID == buildID {
			return status.TargetVersion.Variants
		}
		for _, dv := range status.DeprecatedVersions {
			if dv != nil && dv.BuildID == buildID {
				return dv.Variants
			}
		}
		return nil
	}
	var out []versionRef
	for _, base := range baseVersions {
		for _, vs := range statusVariants(base.BuildID) {
			if vs.Deployment == nil {
				continue
			}
			variant, declared := specVariants[vs.Name]
			if !declared {
				continue
			}
			out = append(out, versionRef{
				BuildID:    base.BuildID,
				Status:     base.Status,
				Deployment: vs.Deployment,
				IsTarget:   base.IsTarget,
				Variant:    variant,
			})
		}
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
	// A variant's own scaling override wins over the shared workerScaling value.
	// The warm-start floor below still applies: a variant's queue must register
	// before its version can be promoted.
	if v.Variant != nil && v.Variant.Scaling != nil && v.Variant.Scaling.MinReplicaCount != nil {
		base = *v.Variant.Scaling.MinReplicaCount
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

	// Step 1 — enumerate desired SOs from active versions (base + variants).
	versions := activeVersionsForScaling(&twd.Status)
	versions = append(versions, variantVersionsForScaling(versions, &twd.Status, twd)...)
	desired := make(map[string]*unstructured.Unstructured, len(versions))
	desiredVersionsByName := make(map[string]versionRef, len(versions))
	names := make([]string, 0, len(versions))
	for _, v := range versions {
		// Resolve the worker deployment name this version's pods actually
		// registered under. A version preserved across a
		// spec.workerOptions.workerDeploymentName change (see the planner's
		// NotRegistered guard) reports its backlog and running pinned
		// workflows under its *recorded* name; building its SO against the
		// newly-resolved name would point KEDA at a version that does not
		// exist, its metrics would read zero forever, and the preserved
		// Deployment would be scaled to min while pinned workflows starve.
		if v.Deployment != nil {
			var dep appsv1.Deployment
			if err := r.Get(ctx, types.NamespacedName{Namespace: v.Deployment.Namespace, Name: v.Deployment.Name}, &dep); err == nil {
				v.RecordedWDN = k8s.WorkerDeploymentNameFromDeployment(&dep)
			}
		}
		so := buildScaledObject(twd, v, temporalEndpoint)
		names = append(names, so.GetName())
		desired[so.GetName()] = so
		desiredVersionsByName[so.GetName()] = v
	}

	r.warnOnSONameCollision(l, twd, names)

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

	claimed := claimedScaleTargets(desired)

	// Step 4 — release renamed SOs BEFORE applying. KEDA's admission webhook
	// rejects a ScaledObject whose scaleTargetRef points at a Deployment another
	// ScaledObject already manages, so a rename has to be delete-then-create. If
	// the old name survived into the apply below, the apply would be denied and
	// the version would never get its scaler.
	if err := r.deleteRenamedScaledObjects(ctx, l, existing, desired, claimed); err != nil {
		return err
	}

	// Step 5 — create / update. Apply failures are collected rather than
	// returned immediately: one failing version must not skip the stale cleanup
	// below, or a rejected apply would leave the object it conflicts with in
	// place forever and never converge.
	var applyErrs []error
	for name, want := range desired {
		v := desiredVersionsByName[name]
		// Ensure the child Deployment is labelled so planner yields.
		if err := r.ensureDeploymentManagedLabel(ctx, v.Deployment.Namespace, v.Deployment.Name, true); err != nil {
			l.Error(err, "failed to label Deployment as managed", "deployment", v.Deployment.Name)
			// continue — best effort
		}
		if err := r.applyDesiredScaledObject(ctx, l, name, want, existing[name], v); err != nil {
			applyErrs = append(applyErrs, err)
		}
	}

	// Step 6 — delete stale (version no longer active).
	if err := r.deleteStaleScaledObjects(ctx, l, existing, desired, claimed); err != nil {
		applyErrs = append(applyErrs, err)
	}

	return errors.Join(applyErrs...)
}

// claimedScaleTargets returns the namespace/name of every Deployment a desired
// ScaledObject points its scaleTargetRef at.
func claimedScaleTargets(desired map[string]*unstructured.Unstructured) map[string]struct{} {
	claimed := make(map[string]struct{}, len(desired))
	for _, so := range desired {
		if name, ns, ok := scaleTargetRef(so); ok {
			claimed[ns+"/"+name] = struct{}{}
		}
	}
	return claimed
}

// deleteRenamedScaledObjects deletes the stale SOs whose scale target a desired
// SO also claims, and drops them from existing so the later stale sweep skips
// them. That pairing — same Deployment, different SO name — is what a change to
// the naming function leaves behind, and KEDA will not let the replacement be
// created while the old object still manages the Deployment.
//
// The keda-managed label is deliberately left in place: the replacement SO
// claims the same Deployment, so handing replicas back to the planner here would
// let it write spec.replicas while KEDA still owns them.
func (r *TemporalWorkerDeploymentReconciler) deleteRenamedScaledObjects(
	ctx context.Context,
	l logr.Logger,
	existing, desired map[string]*unstructured.Unstructured,
	claimed map[string]struct{},
) error {
	for name, so := range existing {
		if _, want := desired[name]; want {
			continue
		}
		target, ns, ok := scaleTargetRef(so)
		if !ok {
			continue
		}
		if _, isClaimed := claimed[ns+"/"+target]; !isClaimed {
			continue
		}
		l.Info("deleting renamed ScaledObject so its replacement can claim the Deployment",
			"name", name, "deployment", target, "buildId", so.GetLabels()[BuildIDLabel])
		if err := r.Delete(ctx, so); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete renamed scaledobject %s: %w", name, err)
		}
		delete(existing, name)
	}
	return nil
}

// deleteStaleScaledObjects removes the SOs we own that the desired state no
// longer includes, handing each one's Deployment back to the planner on the way
// out. Renamed SOs are already gone by this point (deleteRenamedScaledObjects),
// so in the normal path every SO here has an unclaimed target. The claimed check
// in releaseStaleScaleTarget still applies, for the case where a rename delete
// failed and the object reaches this sweep anyway.
func (r *TemporalWorkerDeploymentReconciler) deleteStaleScaledObjects(
	ctx context.Context,
	l logr.Logger,
	existing, desired map[string]*unstructured.Unstructured,
	claimed map[string]struct{},
) error {
	for name, so := range existing {
		if _, want := desired[name]; want {
			continue
		}
		l.Info("deleting stale ScaledObject", "name", name, "buildId", so.GetLabels()[BuildIDLabel])
		r.releaseStaleScaleTarget(ctx, l, so, claimed)
		if err := r.Delete(ctx, so); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete scaledobject %s: %w", name, err)
		}
	}

	return nil
}

// releaseStaleScaleTarget strips the keda-managed label off a stale SO's
// Deployment so the planner is free to drive replicas to zero, unless a desired
// SO still claims that Deployment (see deleteStaleScaledObjects).
func (r *TemporalWorkerDeploymentReconciler) releaseStaleScaleTarget(
	ctx context.Context,
	l logr.Logger,
	so *unstructured.Unstructured,
	claimed map[string]struct{},
) {
	deployName, deployNS, ok := scaleTargetRef(so)
	if !ok {
		return
	}
	if _, keep := claimed[deployNS+"/"+deployName]; keep {
		l.V(1).Info("keeping managed label; Deployment still claimed by a desired ScaledObject",
			"deployment", deployName, "staleScaledObject", so.GetName())
		return
	}
	if err := r.ensureDeploymentManagedLabel(ctx, deployNS, deployName, false); err != nil {
		l.Error(err, "failed to remove managed label", "deployment", deployName)
	}
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
	soLabels := map[string]string{
		OwnerTWDLabel: twd.Name,
		BuildIDLabel:  v.BuildID,
	}
	if v.Variant != nil {
		// Variant SOs get their own name (variant folded into the twd prefix so
		// the hash fallback keeps names distinct) and a discriminator label.
		so.SetName(ScaledObjectName(twd.Name, v.Variant.Name, v.BuildID))
		soLabels[VariantSOLabel] = v.Variant.Name
	} else {
		so.SetName(ScaledObjectName(twd.Name, "", v.BuildID))
	}
	so.SetNamespace(twd.Namespace)
	so.SetLabels(soLabels)
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
	resolvedWDN := resolveWorkerDeployment(twd)
	// Aim the per-version query at the worker deployment name this version's
	// pods actually registered under. For versions preserved across a
	// workerDeploymentName change, that is the RECORDED (pre-change) name —
	// querying the resolved name would target a version Temporal has never
	// seen, read zero forever, and starve the version's pinned workflows of
	// workers. For everything else RecordedWDN either matches resolvedWDN or
	// is empty (Deployment unreadable), and the resolved name is used.
	triggerWDN := resolvedWDN
	if v.RecordedWDN != "" && v.RecordedWDN != resolvedWDN {
		triggerWDN = v.RecordedWDN
	}
	taskQueue := resolveTaskQueue(twd)
	if v.Variant != nil {
		// The variant polls (and its SO watches) the suffixed queue; the
		// version identity {workerDeploymentName, buildID} stays the base's.
		taskQueue += v.Variant.TaskQueueSuffix
	}
	triggerMetadata := map[string]interface{}{
		"endpoint":                temporalEndpoint,
		"namespace":               twd.Spec.WorkerOptions.TemporalNamespace,
		"taskQueue":               taskQueue,
		"workerDeploymentName":    triggerWDN,
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
	if v.Variant != nil {
		// Per-variant scaler overrides, applied AFTER the shared workerScaling
		// fields so they win for this SO only - the base keeps whatever
		// workerScaling says. A variant polls "<queue><suffix>", a dedicated
		// ACTIVITY queue where no workflow ever runs, so it needs the same
		// treatment a dedicated activity pool gets (atlanhq/keda#8): without a
		// false gate the scaler discards its used-slots term (the only "an
		// activity is executing" signal) and reaps the pod mid-activity.
		setVariantTriggerMetadata(triggerMetadata, v.Variant.Scaling)
	}
	if v.Variant != nil && v.Variant.TaskQueueSuffix != "" {
		// A workflowTaskQueueForCount inherited from workerScaling names the BASE
		// workflow queue; the variant's version-scoped workflow count lives on the
		// suffixed queue (mirroring how the variant's own worker derives it).
		if wtqfc, ok := triggerMetadata["workflowTaskQueueForCount"].(string); ok && wtqfc != "" {
			triggerMetadata["workflowTaskQueueForCount"] = wtqfc + v.Variant.TaskQueueSuffix
		}
	}

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
	if v.Variant != nil && v.Variant.Scaling != nil && v.Variant.Scaling.MaxReplicaCount != nil {
		spec["maxReplicaCount"] = int64(*v.Variant.Scaling.MaxReplicaCount)
	} else if maxR, ok := resolveMaxReplicas(twd); ok {
		spec["maxReplicaCount"] = maxR
	}
	setScaledObjectSpec(spec, twd)

	_ = unstructured.SetNestedField(so.Object, spec, "spec")

	// Stamp a hash of the controller-managed content so applyDesiredScaledObject
	// can skip the server-side apply when the live object is already converged.
	so.SetAnnotations(map[string]string{
		specHashAnnotation: scaledObjectSpecHash(so.GetLabels(), spec),
	})
	return so
}

// scaledObjectSpecHash returns a stable hash over the controller-managed
// ScaledObject content (labels + spec). json.Marshal sorts map keys at every
// level, so the encoding - and thus the hash - is deterministic for equal
// input. It is a change-detection digest, not a security primitive.
func scaledObjectSpecHash(labels map[string]string, spec map[string]interface{}) string {
	payload := map[string]interface{}{
		"labels": labels,
		"spec":   spec,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		// A marshal failure should be impossible for this shape; fall back to a
		// sentinel so the apply is never skipped rather than skipped wrongly.
		return ""
	}
	sum := sha1.Sum(b)
	return hex.EncodeToString(sum[:])
}

// setVariantTriggerMetadata applies a variant's per-variant scaler overrides on
// top of the shared workerScaling values, so they scope to that variant's
// ScaledObject alone. Nil fields leave the inherited value untouched.
func setVariantTriggerMetadata(m map[string]interface{}, vs *temporaliov1alpha1.VariantScaling) {
	if vs == nil {
		return
	}
	if vs.GateSlotsOnRunningWorkflow != nil {
		m["gateSlotsOnRunningWorkflow"] = strconv.FormatBool(*vs.GateSlotsOnRunningWorkflow)
	}
	if vs.ActivitySlotsPerWorker != nil {
		m["activitySlotsPerWorker"] = strconv.FormatInt(int64(*vs.ActivitySlotsPerWorker), 10)
	}
	if vs.IncludeRunningWorkflowCount != nil {
		m["includeRunningWorkflowCount"] = strconv.FormatBool(*vs.IncludeRunningWorkflowCount)
	}
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
		// Skip the apply when the live object already carries the hash of the
		// desired content. A converged SO would otherwise be server-side
		// applied on every ~10s requeue - a no-op on the object but still a
		// PATCH request against the apiserver for every version of every TWD.
		if h := want.GetAnnotations()[specHashAnnotation]; h != "" &&
			got.GetAnnotations()[specHashAnnotation] == h {
			l.V(1).Info("scaledobject unchanged; skipping apply", "name", name, "buildId", v.BuildID)
			return nil
		}
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
// a specific version. This MUST match the TEMPORAL_DEPLOYMENT_NAME env on
// worker pods (which comes from k8s.ComputeWorkerDeploymentName), otherwise
// KEDA queries the wrong deployment and sees zero backlog — even though the
// workers are registered and picking up tasks under the actual deployment
// name. Delegates to k8s.ComputeWorkerDeploymentName so
// spec.workerOptions.workerDeploymentName overrides (used to make several
// TWDs share one Temporal Worker Deployment identity) are honored the same
// way the deployment template already honors them.
func resolveWorkerDeployment(twd *temporaliov1alpha1.TemporalWorkerDeployment) string {
	return k8s.ComputeWorkerDeploymentName(twd)
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
