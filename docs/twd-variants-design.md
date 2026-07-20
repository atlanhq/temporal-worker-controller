# One TWD, N Deployments: worker variants for the on-demand mirror

Status: design draft. Base: temporal-worker-controller `v1.6.0-atlan` (`8633f91`; tree-identical to `f192899`, so all file:line anchors are exact). Companion to `temporal-ondemand-od-twin-vpa-sizing.md` (which selected this direction - "approach B" - over forking VPA) and the rerouter suite results.

## TL;DR

Make one TemporalWorkerDeployment reconcile each worker version into multiple k8s Deployments - the base worker plus declared **variants** (first consumer: the `-od` on-demand mirror) - all registering the same `{workerDeploymentName, buildID}`. Because the TWD scale-subresource selector is version-independent (`temporal.io/deployment-name`), the app's existing VPA then observes and sizes **all** variants, closing the od-OOM blind spot with stock VPA.

Two API shapes considered:
- **A. single-purpose flag** `spec.onDemandMirror {enabled, ...}` - od semantics hard-coded in TWC.
- **B. generalized list** `spec.variants: []` - TWC implements "N pod-shape variants per version" generically; the od-ness (suffix, on-demand affinity) is just data supplied by the chart.

**Recommendation: B.** The controller-side refactor is the same either way (the buildID-keyed Deployment map is the load-bearing change); B removes od-specific branches from TWC, keeps the od opinion in the chart where atlan's other capacity opinions live, gives future tiers (GPU, arm64, high-mem, warm standby) for free, and is the more upstreamable story. The one hazard of B is API creep - mitigated by making a variant a **bounded delta**, not a template override.

## Context (why)

- The `-od` mirror worker exists so the activity rerouter can move reclaimed spot work to on-demand capacity (BLDX-1532; live-validated 2026-07-18, including case F: pinned activities dispatch across deployments that share a build ID).
- Today the chart renders the mirror as a **second TWD** (`<app>-worker-od-twd`). Consequence: the base VPA cannot see od pods (disjoint scale selectors), so od runs static resources, never receives the base's OOM bumps, and - worse - **an OOM on od itself is invisible to any recommender** (the od-OOM blind spot).
- Consolidating to one TWD makes the existing VPA span both pod sets with **zero VPA changes**: `status.selector` is `temporal.io/deployment-name=<twdName>` only (`internal/controller/genstatus.go:69`, `internal/k8s/deployments.go:255-257`), never the build id (guarded by `deployments_test.go:583-617`).
- Temporal versioning is not a blocker: version identity is `{DeploymentName, BuildID}` and a version natively carries a *list* of task queues (`api/v1alpha1/worker_types.go:429-430`; `internal/temporal/worker_deployment.go:127-131` derives the version set from the server, not from counting k8s Deployments). Two Deployments polling `<base>` and `<base>-od` are one version with two queues - the same fact the live case-F test already exercised across two TWDs.

## API shapes

### A. `spec.onDemandMirror` (single-purpose flag)

```yaml
spec:
  onDemandMirror:
    enabled: true
    taskQueueSuffix: "-od"      # default
    taskQueueEnvVars: [ATLAN_DEPLOYMENT_NAME]   # values get the suffix appended
    affinity: {...}             # on-demand pin
    tolerations: [...]
    resources: {...}            # optional override
```

Pros: smallest CRD delta; validation is trivial; reads as exactly what it does.
Cons: od semantics (one specific suffix, one specific second tier) are baked into TWC; a future third tier repeats the whole exercise; the field is atlan-bespoke, so the fork diverges from upstream in a way upstream would never take.

### B. `spec.variants` (generalized; recommended)

```yaml
spec:
  workerScaling:
    taskQueue: atlan-<app>-production        # base queue, unchanged
  variants:
    - name: od                               # DNS-safe, short; "base"/"" reserved
      taskQueueSuffix: "-od"                 # "" = poll the SAME queue (warm standby)
      envValueSuffixes:                      # env vars whose VALUES get the suffix appended
        - ATLAN_DEPLOYMENT_NAME
      affinity: {...}                        # bounded delta fields, all optional:
      nodeSelector: {...}
      tolerations: [...]
      resources: {...}
      scaling:                               # optional KEDA overrides; default inherit
        minReplicaCount: 0
```

Each variant produces, per worker version: one Deployment (base template + the bounded delta) and one KEDA ScaledObject (trigger on `<baseQueue><suffix>`), registering the same `{workerDeploymentName, buildID}`.

Pros: TWC stays od-agnostic - the chart owns the od opinion as data; future tiers are config, not controller work; a `variants` concept ("one worker version, N pod shapes") is a plausible upstream contribution; controller code is arguably *simpler* (a loop over `[base] + variants` instead of an if-od branch).
Cons: list validation (unique names, name-length budget), a slightly larger status surface, and the temptation to grow the delta into a template - which must be resisted.

**Why a bounded delta and not a full `template`/strategic-merge patch:** two full templates drift (image bumps must touch both); and strategic merge on env lists has a known corruption failure mode in this stack (temporal-operator `services.overrides` precedent - env lists must be appended via JSON patch, never strategically merged). The delta fields above are exactly the axes the od twin actually varies on today in the chart (affinity, tolerations, queue identity, resources).

**Semantics of `envValueSuffixes` (matters for atlan):** the atlan main worker has no explicit queue env - the SDK *derives* the queue from `ATLAN_DEPLOYMENT_NAME`. So the variant transform is "append the suffix to this env's **value**" (`production` -> `production-od`, SDK derives `atlan-<app>-production-od`), not "set env to the full queue name". This matches how the two-TWD chart builds the od twin today (`deployment.yaml:529-530`).

## Change surface (anchored at `8633f91`)

The load-bearing refactor is identical for A and B; written here in B's terms.

1. **CRD + webhook** - `api/v1alpha1/worker_types.go:74-127`: add `Variants []WorkerVariant`. Webhook (`temporalworker_webhook.go:46-56, 82-117`): default `taskQueueSuffix`, validate unique DNS-safe names, reserved `base`, `workerScaling.taskQueue` set when any variant has a suffix. Regenerate deepcopy + CRD.
2. **Re-key `DeploymentState`** - `internal/k8s/deployments.go:45-52, 86-92`: `map[buildID]*Deployment` -> `map[buildID]map[variant]*Deployment` (variant `""`/`base` = base). Discriminator label `temporal.io/variant: <name>` on each Deployment + its pod template; read back on list. **Two same-build Deployments currently collide in this map - this is the central bug-by-construction to remove.**
3. **Selector** - `ComputeSelectorLabels` (`deployments.go:244-249`) gains the variant label -> per-Deployment selectors `{deployment-name, build-id, variant}` are disjoint; `TWDNameSelector` (`deployments.go:255-257`) is **untouched** so `/scale` (declared `worker_types.go:597`) spans all variants - the VPA payoff. Consumers: `deployments.go:268`, `workerresourcetemplates.go:90` (WorkerResourceTemplate applies go to all variants).
   - **Selector immutability:** existing base Deployments (2-label selector) cannot gain the variant label. Policy: 3-label selectors only on Deployments created after variants are configured -> variants materialize at the **next version rollout**, never retrofitted onto the live buildID. Zero migration; atlan apps roll constantly.
4. **Build path** - `NewDeploymentWithOwnerRef` (`deployments.go:260-328`) gains the variant delta: name `ComputeVersionedDeploymentName(twdName+"-"+variant, buildID)` (hash-truncation already handles overflow, `deployments.go:175-182`), delta fields applied, env value-suffix transform, same `TEMPORAL_DEPLOYMENT_NAME`/`TEMPORAL_WORKER_BUILD_ID` injection (`deployments.go:382-389`) so every variant registers the same version.
5. **Plan/exec** - `planner.ShouldCreateDeployment` (`planner.go:50, 727-750`) -> per-variant; `plan.CreateDeployment` (`genplan.go:30, 187-205`) -> slice; `execplan.go:36-44` loops. Drift detection `checkAndUpdateDeploymentPodTemplateSpec` (`planner.go:397-506`) and scale/delete (`planner.go:573, 644-679`) iterate all variants - else variants never roll template changes and never sunset.
6. **Status + state mapper** - `BaseWorkerDeploymentVersion.Deployment` stays the base ref (backward-compatible); add `variantDeployments: [{name, ref}]`. `state_mapper.go:57, 90-91, 125-126, 170, 185-186` bind all variants; `genstatus.go:63` sums readyReplicas across variants.
   - **Promotion gating:** `HealthySince` gates on the **base** variant only - an on-demand capacity shortage must not block app rollouts. Variants stay floored to >=1 replicas while the version is Ramping/Inactive (`resolveMinReplicas`, `scaledobjects.go:214-235` - existing behavior) so their queues register before `SetCurrentVersion` (`execplan.go:239` does not set `IgnoreMissingTaskQueues`; needs the live validation below).
7. **ScaledObjects** - `scaledobjects.go`: one SO per variant per active version - `ScaledObjectName(twd.Name+"-"+variant, buildID)` (63-char hash fallback at `:119-137`), `scaleTargetRef` = the variant's Deployment, `triggerMetadata.taskQueue = resolveTaskQueue(twd)+suffix`, same WDN/buildID (`:540-546`); `ensureDeploymentManagedLabel` (`:459-487`) for every variant. Per-variant `scaling` overrides merge over the TWD's `workerScaling`.

## What does NOT change

- Temporal version lifecycle: one version, N queues is native; no extra versions are minted (variants inherit `unsafeCustomBuildID`), so no interaction with the 100-version GC wedge.
- `TWDNameSelector` / scale subresource / VPA wiring - byte-identical.
- No variants configured -> desired state is byte-identical to today (regression gate for every phase).

## Ripples (separate PRs, same release window)

- **Rerouter** (`temporal-activity-rerouter`): `HasODTier` (`registry.go:52-56`) infers the od tier from a *second TWD* whose `workerScaling.taskQueue == <base>-od` (`registry.go:88-98`). Consolidation removes that TWD -> `HasODTier` = false -> **the rerouter skips 100% of reclaims for consolidated apps**. Rework: the registry keys the od tier off the TWD's variants (a variant whose `taskQueueSuffix == odSuffix`) - arguably cleaner than the bespoke flag. Attribution (`QueueForDeployment` via `temporal.io/deployment-name`) keeps working; od-queue candidates are already skipped by the `-od`-suffix idempotency check.
- **atlan chart**: drop the `od:twd` entry from `$deploymentTypes` (`subcharts/atlan-app/templates/deployment.yaml:27-32`) and express the mirror as `spec.variants: [{name: od, ...}]` on the single TWD, moving over the on-demand affinity/tolerations and the `-od` scaling trio. `vpa.yaml` is untouched and now covers both pod sets.

## Implementation phases (TDD; flag-off equivalence is the gate at every step)

| Phase | Work | Verification |
|---|---|---|
| 0 | CRD `Variants` + webhook + regen | webhook unit tests; CRD round-trip |
| 1 | `DeploymentState` re-key + variant label + selector policy | rewritten `deployments_test.go` state tests first; no-variant path byte-identical |
| 2 | variant build path (name, delta, env transform) | golden-object diff base vs variant |
| 3 | plan/exec/drift/scale/delete over variants | `planner_test.go` per-variant expectCreate; sunset sweeps variants |
| 4 | status + state mapper + promotion gating | mapper tests: version with variants, mixed health |
| 5 | per-variant ScaledObjects | `scaledobjects_test.go` desired-set on/off |
| 6 | envtest integration + live markeznp25 | 2 Deployments + 2 SOs + status; rollout v1->v2 with a variant (validates the SetCurrentVersion missing-task-queues subtlety); VPA observes od pods; ballooned od pod's OOM bumps `status.recommendation` |

Estimated effort unchanged from the flag design: the core is phases 1+3+4 (~15 call sites on the hottest reconcile path); the generalized API adds list validation and removes od special-casing - roughly a wash.

## Decisions

1. **API shape: variants (B) over the flag (A)** - recommended; the rest assumes B.
2. Variant delta stays **bounded** (affinity, nodeSelector, tolerations, resources, taskQueueSuffix, envValueSuffixes, scaling overrides) - no template override, no strategic-merge patch.
3. Selector migration: variants materialize at the next rollout only (selector immutability; zero migration).
4. Promotion gates on base health only; variants floored >=1 pre-Current.
5. Status: additive `variantDeployments`, base ref unchanged.
6. WorkerResourceTemplate applies target all variants.

## Open validation items

- `SetCurrentVersion` behavior when a variant queue registers late (live test in phase 6).
- Variant scale-to-zero during rollout windows (the >=1 floor trades a small idle cost for promotion safety).
- Rerouter registry change ships in lockstep - a consolidated app without the rerouter rework silently stops rerouting.
