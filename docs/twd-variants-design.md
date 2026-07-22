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

1. **CRD + webhook** - `api/v1alpha1/worker_types.go:74-127`: add `Variants []WorkerVariant`. Webhook (`temporalworker_webhook.go:46-56, 82-117`): validate unique DNS-safe names, reserved `base`, `workerScaling.taskQueue` set when any variant has a non-empty suffix (no global suffix default - `""` means poll the base queue). Regenerate deepcopy + CRD.
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

## Performance impact

Net neutral-to-positive at the control plane; zero impact on the data path (task queues, pollers, and dispatch are identical either way - the same pods poll the same queues).

**Cheaper, fleet-wide:**
- **TWD count halves** (2 -> 1 per split+TWD app; ~123 apps on the test tenant alone). Each TWD is its own reconcile loop with its own Temporal describe polling, and TWC reconcile pressure is a known incident class (the ~10s-per-TWD hot-loop, fleet-wide ~580 tenants). Fewer TWDs = fewer reconciles, fewer TWC->Temporal RPCs, fewer status writes.
- **Temporal worker-deployment count halves** - today base and od are two WDNs server-side, each accumulating version records per release. One WDN with two queues halves version-record accumulation and shrinks the 100-version GC-wedge surface (per-WDN churn rate unchanged; half as many WDNs churning).
- Per-reconcile work grows slightly (build/diff 2 Deployments + 2 SOs instead of 1+1) - in-memory, noise against the RPC-bound loop. KEDA SO count and trigger polling are unchanged (the od twin already has its own SO today). VPA recommender load is unchanged (it already watches all pods; merging od pods into one histogram is trivial).

**Watch-items:**
1. **Rollout latency (the open validation item).** If `SetCurrentVersion`'s missing-task-queues check ends up waiting for the od queue to be polled, promotion inherits on-demand node provisioning latency (~1-2 min Karpenter spin-up) on every release. The >=1 floor during ramp exists to register the queue early; whether promotion can still race it is the phase-6 live test.
2. **VPA eviction churn now reaches od pods.** Today od pods are never VPA-evicted (no VPA). Under `updateMode: Auto` the updater may evict an od pod mid-activity to resize it - and od pods run already-disrupted rerouted work, so an eviction costs another attempt. Partly this is the fix working (evict-to-grow during an OOM loop); residual churn shrinks as VPA's in-place-resize path matures. Sanity-check PDB/minReplicas at rollout.
3. **On-demand capacity when od is active** rises from static ~256Mi requests to VPA-sized ones. That is the point of the change, but it is a real cost delta on the expensive tier - bounded by od being scale-to-zero and intermittent.

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

## Test plan

### Unit (TDD - written before the code they gate)

| Area | Cases |
|---|---|
| webhook / CRD | variant names unique + DNS-safe + short; `base`/empty reserved; no global suffix default - each variant declares its own (`""` = poll the base queue); variant with non-empty suffix requires `workerScaling.taskQueue`; `envValueSuffixes` names non-empty; CRD round-trip with and without variants |
| deployments state | two Deployments of one buildID with distinct `temporal.io/variant` labels do NOT collide (the central bug today); unlabeled (pre-feature) Deployment maps to base; unknown variant label -> surfaced, not silently adopted |
| build path | golden-object diff base vs variant: name (`<twd>-<variant>-<build>`, hash-fallback when >47 chars), 3-label selector + pod labels, affinity/nodeSelector/tolerations/resources deltas applied, `envValueSuffixes` appends to the right env values and only those, identical `TEMPORAL_DEPLOYMENT_NAME`/`TEMPORAL_WORKER_BUILD_ID` injection |
| planner | create decisions per variant (base exists / variant missing -> create variant only, and vice versa); drift in a variant delta rolls only that variant's Deployment; drift in the shared template rolls all; scale ops target the right variant; sunset deletes every variant of the version |
| state mapper / status | version with variants: base ref unchanged, `variantDeployments` populated; readyReplicas = sum over variants; health/promotion gates on base only (variant unhealthy must NOT block `HealthySince`) |
| scaledobjects | one SO per variant per active version; trigger queue = `resolveTaskQueue(twd)+suffix`; empty suffix -> same queue as base; same WDN/buildID in trigger metadata; per-variant `scaling` overrides merge over `workerScaling`; managed label ensured on every variant's Deployment; 63-char SO name fallback |

### Regression gate (every phase)

- No variants configured -> desired objects byte-identical to `v1.6.0-atlan` (golden render diff), and the existing unit + envtest suites pass **unmodified**.
- `TWDNameSelector` stays 1-label (existing `deployments_test.go:583-617` must keep passing untouched).

### Integration (envtest)

- Reconcile a TWD with one `od` variant: 2 Deployments + 2 SOs created, status shape correct, `/scale` selector spans both pod sets.
- Rollout v1 -> v2: both variants get new Deployments; old version's variants sunset together; no orphaned SOs.
- Variant added to an existing TWD: nothing retrofitted onto the live buildID; next version materializes the variant (selector-immutability policy).
- Variant removed: its Deployments/SOs are swept on the next version (or per chosen policy), base untouched.

### Live functional (markeznp25; reuses the BLDX-1532 harness - versioned e2eworker image, heartbeating Sleep activity, `rerouter-e2e` ns)

| # | Test | Pass criteria |
|---|---|---|
| F1 | dual registration | one TWD + od variant -> both pods register the same `{workerDeploymentName, buildID}`; `temporal worker deployment describe` shows ONE version with BOTH queues |
| F2 | pinned dispatch across variants (case-F analog) | pinned workflow on the base queue; reroute its activity to `<base>-od` (rerouter e2e path, `dryRun:false` window) -> runs on the od variant pod with the pin intact |
| F3 | rollout + promotion | bump buildID -> promotion succeeds; **measure** whether `SetCurrentVersion` waits on od-queue registration (the open validation item); od floored >=1 during ramp; scales to zero after Current |
| F4 | VPA spanning (the point of the change) | VPA targets the TWD -> `status.recommendation` reflects od pod usage; balloon the od pod -> OOMKilled -> recommendation memory bumps; next od pod admission gets the bumped requests |
| F5 | per-variant KEDA | backlog on `<base>-od` scales od 0->1 while base replica count is unaffected, and vice versa |
| F6 | rerouter lockstep | reworked registry sees the variant-declared od tier -> `HasODTier=true`, reroute e2e passes on a consolidated app; negative control: app without an od variant -> `skip{no-od-tier}` |
| F7 | resource patch without version churn | patch the od variant's `resources` -> only the od Deployment rolls; buildID/version count unchanged (`unsafeCustomBuildID` pins identity) |
| F8 | chart equivalence | consolidated chart (1 TWD + variants) vs today's two-TWD render: pod-level equivalence on env, affinity, tolerations, queues (helm template diff) |

Harness cautions carried over from the live suite: untaint `karpenter.sh/disrupted` within seconds (held ~90s it deletes the node); the reclaim target pod needs `karpenter.sh/do-not-disrupt: "true"`; config flips are global - keep `dryRun:false` windows short.

### Performance / soak (canary tenant, before/after)

- TWC reconcile duration + TWC->Temporal RPC rate (expect a drop with halved TWD count).
- Rollout wall-time delta across ~5 releases (watch-item 1).
- VPA eviction count on od pods over a week (watch-item 2).

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
