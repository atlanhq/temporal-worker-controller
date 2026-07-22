# Worker variants - live validation on markeznp25

Status: executing. Date: 2026-07-20. Controller under test: `ghcr.io/atlanhq/temporal-worker-controller:sha-c7d1e9703d5dad8c3a8696409215a7dff3548706` (PR #23 head). Chart under test: `ghcr.io/atlanhq/atlan-app:0.1.2-pr14006-gb7a008e` (PR #14006 head, already live on the tenant). Restore point for the controller: `1.6.0-atlan-2-8633f9133227cb74e3cea0db7690da275c5a6785` (= the PR's base commit, so the swap delta is exactly PR #23).

## Prior art - already validated (not repeated here)

| When | What | Result |
|---|---|---|
| 2026-07-18 | Case F (two-TWD shape): version-PINNED activity reroutes onto an od twin sharing the build ID, pin intact; negative control | PASS (rerouter `docs/temporal-ondemand-watcher-live-suite-results.md`) |
| 2026-07-18 | Rerouter skip gates 2a-2e (below-threshold / oom / voluntary / final-attempt / never-type) | PASS (all five) |
| 2026-07-18 | Watcher-down resilience (3A fallback, 3B durability-by-construction) | PASS |
| 2026-07-20 | TWC unit+envtest suite incl. variants matrix; flag-off byte-identical gate (existing tests unmodified) | PASS (`make test-unit`) |
| 2026-07-20 | Chart render equivalence: `odMirrorMode=twd` byte-identical to pre-change; `variants` shape correct (helm --output-dir diff) | PASS |
| 2026-07-20 | Validate-without-merge pipeline: PR chart published, OCIRepository flipped, ai-memory canary HelmRelease Ready on the PR chart, pruned od TWDs recreated | PASS |

## Setup (S-phase)

- S1: apply the PR's regenerated TWD CRD (additive optional fields; old CRD would silently prune `spec.variants`).
- S2: swap `default/markeznp25-manager` image to `sha-c7d1e97...`; verify rollout, clean logs, and **fleet stability** (no TWD/child-Deployment churn - flag-off byte-identical in production; spot-check generations before/after).
- S3: harness namespace `rerouter-e2e`: TemporalConnection + TWD `rr` (e2eworker-4 image, versioned via `unsafeCustomBuildID: test-v1` + Pinned workflows) declaring `variants: [{name: od, taskQueueSuffix: -od, envValueSuffixes: [TASK_QUEUE], scaling: {minReplicaCount: 0}}]`; fast KEDA (`polling 10s, cooldown 60s, min 0, activation 0, targetQueueSize 1`) and fast sunset (`scaledownDelay 1m, deleteDelay 2m`).

## Test matrix

| # | Test | Pass criteria |
|---|---|---|
| V1 | dual reconcile + single version | TWC creates base+od Deployments (disjoint selectors, shared `temporal.io/deployment-name`) + 2 SOs; both pods register the SAME `{WDN, buildID}`; `temporal worker deployment describe` shows ONE version with BOTH queues; `status.targetVersion.variants[]` filled; `/scale` selector spans both |
| V2 | KEDA scale UP on both queues | backlog on base queue wakes base 0->1; backlog on `-od` queue wakes od 0->1 (od SO triggers on the suffixed queue) |
| V3 | KEDA scale DOWN - **side queue to 0 while main busy** | with work ONLY on the base queue, the od variant scales to 0 after cooldown and STAYS 0 (od SO must not count base-queue work); base stays up while its work runs |
| V4 | VPA applies to BOTH pod sets | one VPA targeting the TWD: recommendation appears; admission stamps VPA-computed requests on BOTH base and od pods; od's OWN OOM (balloon + low variant mem limit) bumps the shared recommendation - the blind-spot fix |
| V5 | TWC upgrade (rollout) | bump `unsafeCustomBuildID` test-v1 -> test-v2: new base+od Deployments for v2, promotion succeeds (measure whether SetCurrentVersion waits on od-queue registration - open design item), v2 Current |
| V6 | TWC drain (sunset) | v1 drains -> after scaledownDelay BOTH v1 Deployments scale to 0 -> after deleteDelay BOTH deleted (variant cascade), v1 SOs removed |
| V7 | chart-level variants on a real app | flip `odMirrorMode=variants` on ai-memory's HelmRelease: old `ai-memory-worker-od-twd` pruned, base TWD gains `spec.variants`, TWC reconciles the variant Deployment+SO, the existing `ai-memory-vpa` now spans both pod sets |
| V8 | rerouter lockstep + pinned reroute (case F on variants) | rerouter image -> `sha-d973885` (dual-signal registry): variant-declared od tier lights `HasODTier`; reclaim on the base pod's node -> escalate -> patch to `-od` -> od variant scales 0->1 and runs the PINNED activity (same WDN+buildID) |

## Execution log / results

**ALL 8 TESTS PASS.** Run: 2026-07-20 ~16:20-19:10 UTC on markeznp25.

| # | Result | Evidence |
|---|---|---|
| S1/S2 | PASS | CRD applied; image swapped; **fleet stable: zero generation churn across 143 TWDs / 156 child Deployments** after 60s of reconciles (flag-off byte-identical holds in production). Error logs = pre-existing version-GC retry loop only ("active pollers", legitimate) |
| V1 | PASS | `rr` TWD -> `rr-test-v1` + `rr-od-test-v1` Deployments (selectors `{deployment-name, build-id, variant=base/od}` - disjoint, shared deployment-name) + 2 SOs (both min-floored 1 pre-registration); both workers registered `rerouter-e2e/rr` build `test-v1`; version went **Current** with both queues; `status.targetVersion.variants=[od]`; scale selector `temporal.io/deployment-name=rr` |
| V2 | PASS | backlog on base queue woke base 0->1; backlog on `-od` queue woke od 0->1 - both within 20s (each SO triggers on its own queue) |
| V3 | PASS | od work dropped, base activity running: od scaled 1->0 exactly at cooldown (t+70s, 60s cooldown) and STAYED 0 for 60s+ while base held 1/1. **The side queue does not count main-queue work**; the base's running activity holds it up |
| V4 | PASS | one VPA targeting the TWD: `vpaUpdates: "Pod resources updated by rr-vpa"` stamped on **BOTH** base and od pods (different Deployments); requests mutated 50m/64Mi -> 25m/250Mi on both. **od's OWN OOM** (400MB balloon vs 256Mi variant limit; base survived on 2Gi) bumped the shared recommendation **250Mi -> ~488Mi within 40s** - the two-TWD blind spot is closed. Variant-delta drift (resources/env) rolled the right Deployments in place, buildID unchanged |
| V5 | PASS | `unsafeCustomBuildID` test-v1 -> test-v2: both v2 Deployments appeared in ~10s, **promotion to Current in ~16s, zero missing-task-queues errors** - the SetCurrentVersion open item is closed (the >=1 floor registers the od queue before promotion) |
| V6 | PASS | v1 Drained at t+110s with **both v1 SOs swept** (4->2); both v1 Deployments scaled 0 then **deleted together** ~t+240s (scaledownDelay 1m + deleteDelay 2m); v2 untouched |
| V7 | PASS | ai-memory HelmRelease `odMirrorMode=variants`: old `ai-memory-worker-od-twd` **pruned by helm** (t+20s), base TWD gained `spec.variants=[od]`; variant correctly **gated until the next rollout** (legacy immutable selector); synthetic version bump `main-5336556b` -> base+variant Deployments rolled together, promoted Current in ~20s; variant selector shares `deployment-name` so the existing `ai-memory-vpa` spans both pod sets |
| V8 | PASS | rerouter on the dual-signal image: reclaim taint on the base pod's spot node -> `nodereclaim(disrupted-taint-protected)` -> `decisions{escalate,lost-work}` -> `acts{started,applied}` -> queue patched to `-od`. **`HasODTier` fired from the variants signal alone** (rr has NO second TWD). Base pod killed -> retry on `-od` -> KEDA woke the od variant from zero on an **on-demand** node -> activity STARTED there, attempt 2, **pin intact** (`rerouter-e2e/rr.test-v2 VERSIONING_BEHAVIOR_PINNED`). Taint held 26s |

### Findings / follow-ups

1. **Strand window on enablement (rerouter follow-up)**: the registry's variants signal is SPEC-derived. Between flipping `odMirrorMode=variants` and the app's next version rollout, `HasODTier` is true while NO od Deployment/SO exists for the Current (legacy-selector) version - an enforced (`dryRun:false`) rerouter could escalate onto a pollerless `-od` queue and strand the retry until the rollout. Mitigations: enable variants mode before rerouter enforcement (runbook ordering), or better - teach the registry to read `status.targetVersion.variants` (materialized) instead of spec. Filed against rerouter PR #1.
2. **Rerouter CI tags the MERGE sha**, not the head sha (`sha-cdc7bf6b...` for head `d973885`) - read the pushed tag from the image job log; a guessed head-sha tag 404s. (The TWC PR workflow added in #23 deliberately tags the HEAD sha to avoid this.)
3. Promotion timing with variants: ~16s (synthetic) / ~20s (real app) taint-to-Current equivalents; no od-queue coupling observed - on-demand capacity provisioning never gated promotion because the od variant's floor pod schedules during ramp.
4. KEDA idle scale-to-zero applies to BOTH tiers independently (v2 base+od both parked at 0 when idle) - the od tier costs nothing at rest, as designed.


## Round 2 (2026-07-22, rebased controller): V9-V10

Context: PR #23 rebased onto v1.6.0-atlan @ fb9d67a (#25 sunset prune); tenant found rolled to the plain base image `1.6.0-atlan-2-fb9d67a...` (no variants) by the team - re-swapped to the rebased PR image `sha-2e49355...` for this round. Controller delta vs round 1 additionally includes the ExpectedGateQueues hardening (gate evaluation blocks until every spec-declared queue - base + each variant suffix - is REGISTERED on the target version).

| # | Test | Pass criteria |
|---|---|---|
| V9 | route-back after reroute | TwoActs(d1,d2) on the base queue; act 1 rerouted to `-od` mid-flight and completes on the od variant; act 2 then schedules and runs on the BASE queue (the patch is per-activity - subsequent work returns to spot) |
| V10 | gated rollout covers the od queue | `rollout.gate: GateWf` + buildID bump: gate workflows fan out to BOTH queues, promotion blocks until BOTH complete (incl. the od one, run by the od variant worker), then v2 Current. Hardening: gate not evaluated before both queues are registered |

### Results

**BOTH PASS** (run 2026-07-22, controller `sha-2e49355...` = rebased PR #23 incl. the ExpectedGateQueues hardening; fleet stable through the swap - zero controller-induced generation churn).

| # | Result | Evidence |
|---|---|---|
| V9 | PASS | `TwoActs` on the base queue: act 5 STARTED on the base pod -> reclaim -> rerouter `escalate{lost-work}` + patch to `atlan-rr9-e2e-prod-od` (`dry_run:false`); act 5's in-flight attempt completed on base (patchWhileStarted takes effect on the NEXT attempt - none happened); **act 11 then SCHEDULED on `atlan-rr9-e2e-prod` (BASE)** and completed there; workflow COMPLETED. The per-activity patch never leaks to subsequent activities - post-reroute work returns to the spot queue. (The od-failover leg of the same patch was proven in V8.) |
| V10 | PASS | `rollout.gate: GateWf` + buildID bump test-v1 -> test-v2: **exactly 2 gate workflows** (`test-rerouter-e2e/rr9:test-v2-<queue>`, one per queue); the `-od` one ran on the od variant worker (sole poller of that queue). Promotion **waited for both**: at t+10s the od gate was Completed, the base gate pending, target still Inactive; v2 went Current only after both completed (bump -> promoted in 22s). Gated rollouts cover the od queue end to end |

### Infrastructure findings from this round (all real, none variants-caused)

1. **`matching.maxDeployments` (default 100) is exceeded fleet-wide on markeznp25**: 145 worker deployments registered; every NEW deployment-name registration fails with `reached maximum deployments in namespace (100)` (matching logs, ~140 errors/30s). Existing names keep working, which masks it - any newly onboarded versioned app on a tenant this size will silently fail to register. Raised to 200 for the tenant; the FLEET fix (chart-level dynamic config bump, sized to ~2x TWD count for twd-mode od twins) should ship with the rollout. Consolidating to variants HALVES the deployment count - one more argument for the migration.
2. **Dynamic-config edits do not propagate**: the active file is a subPath mount (no in-place CM updates) AND the vcluster syncer served a stale host-side copy across CM delete/recreate and pod restarts. The working lever is the TemporalCluster CR's `services.overrides` volume (operator owns the deployment specs and reverts direct patches) pointed at a FRESH CM name - forces a clean sync + re-render. Applied as `temporal-cluster-runtime-dynamic-config-v2`.
3. **Workflows started while matching rolls can spool invisibly**: a workflow started mid-roll sat with a SCHEDULED workflow task that neither the queue stats nor the KEDA scaler could see; restarting the workflow after the roll behaved normally. Worth knowing for any maintenance window.
4. Round-1 finding confirmed by accident: the plain base-image controller (tenant was rolled to it by the team between rounds) ignores `spec.variants` harmlessly - base reconciles fine, the field no-ops (CRD forward-compat holds).


## Coverage mapping: additional scenarios (2026-07-22)

Three scenarios raised in review, mapped to existing evidence plus two variant-side unit tests added to close thin gaps (`scaledobjects_variants_test.go`).

| Scenario | Verdict | Evidence |
|---|---|---|
| Deprecated worker version -> its SOs deprecated too | COVERED, with deliberate timing: SOs are removed at **Drained**, not at deprecated - a Draining version keeps its SOs because pinned work still runs on it and KEDA must keep scaling those workers | Live V6 (drain swept both tiers' SOs 4->2, then scaled 0 + deleted both Deployments); unit `TestActiveVersionsForScaling_ExcludesDrained` (base) + `TestVariantSOsDeprecatedWithVersion` (variant set derives from the drained-excluding base set - both tiers deprecate atomically) |
| SO target matches the appropriate worker deployment | COVERED - every SO's scaleTargetRef points at exactly its own (version x tier) Deployment; RecordedWDN keeps the trigger aimed at the right worker-deployment identity across WDN renames | Live V1 (`rr-test-v1-scale -> rr-test-v1`, `rr-od-test-v1-scale -> rr-od-test-v1`; 4 correctly-paired SOs during the V5 rollout); unit `TestBuildScaledObjectVariant` + the surviving-refs map assertion in `TestVariantSOsDeprecatedWithVersion` |
| Workflow task without a Build ID -> ramping+current WDs scale to 1 for the unversioned queue | COVERED by two overlapping mechanisms: (a) Current/Ramping SOs carry `selectAllActive+selectUnversioned` so spooled no-build-ID tasks wake them from 0; (b) the `resolveMinReplicas` floor pins Ramping/Inactive/NotRegistered-target versions at >=1 unconditionally, making the "to 1" outcome deterministic during rollouts regardless of backlog visibility | Live V2 + V9b (unversioned workflow starts woke the Current version 0->1 in ~20s on BOTH the base and `-od` queues); unit: base flags per status (pre-existing) + `TestVariantSOUnversionedFlags` (variant SOs: flags on for Current/Ramping, strictly per-build when Draining). Caveat: the Ramping half of the scaler path is masked live by the floor (never observably at 0) - unit-verified only; the documented Current+Ramping double-count of the same spooled task remains the accepted cold-start trade-off |


## Round 3 (2026-07-22): odMirrorMode removed; fleet consolidation wave; deletion-deadlock finding

The interim `odMirrorMode` knob was removed from the chart (atlan reland commit `9062d3a6`, ported to staging/preprod): variants is the only od-mirror shape, rendered unconditionally inside the `keda.enabled` gate. Default render is byte-identical to the previously validated `--set odMirrorMode=variants` output. The knob-removed chart (`0.1.2-pr14006-g9062d3a`) was deployed tenant-wide on markeznp25.

| Check | Result |
|---|---|
| keda-on app (ai-memory, already variants) | no-op upgrade: variants intact, now-inert `odMirrorMode` HelmRelease value harmless |
| keda-off app (anaplan) | od TWD pruned, NO variants - by design (variants live in the keda gate; keda-off apps were never rerouter-covered: no `workerScaling.taskQueue` -> not in the od-tier registry) |
| fleet | od TWDs prune as the HelmRelease wave cycles; each keda-on prune also deletes its Temporal version + deployment records, actively freeing `matching.maxDeployments` headroom |

### Finding: TWD deletion deadlocks while its own children still poll (pre-existing fork bug)

The round-1 "version-GC retry loop" errors were misdiagnosed as benign: they were **stuck od-TWD deletions from the 2026-07-20 prune wave**. Mechanism: TWD deletion -> `temporal.io/delete-protection` finalizer tries to delete the Temporal version record **before tearing down the TWD's own child Deployments** -> children still poll -> server refuses ("Version cannot be deleted since it has active pollers") -> retry forever. Deadlocked 2+ days: anaplan, sagemaker, trino (keda-off: static replicas=1) + azure-data-factory (keda-on with SO min=1 - the SO survives into termination and fights any manual scale-down; its child also had a hash-truncated name `azure-data-chore-tool-68faabe01c` from a long custom buildID, which disguises it).

- **Stuck predicate**: any od TWD whose children sit at >0 replicas when deleted (keda-off static, or SO min>=1).
- **Unblock**: delete the TWD's ScaledObjects, then delete (not scale - KEDA fights) its child Deployments; the version record deletes ~4-5 min later (server-side poller TTL) and the finalizer clears. Verified on all four.
- **Fix for the fork (follow-up, NOT #23 scope)**: the deletion/finalizer path should delete child Deployments + SOs first, wait for the poller window, then delete the version record. Until then this bites ANY TWD deletion with live pollers - od twins during consolidation are just the common case.
- Note: variants-mode teardown does not hit this shape in the same way (V6 drained before deletion), but a variants TWD deleted outright with running children would - same fix covers it.

## End state

- markeznp25 KEPT on: TWC `sha-2e49355...` (rebased PR #23 + gate hardening), atlan-app chart `0.1.2-pr14006-g9062d3a` (knob-removed, OCIRepository), rerouter `sha-cdc7bf6b...` (PR #1 head d973885, dryRun:true, minLostWork:30m), `matching.maxDeployments: 200` via the `-v2` runtime CM + CR override. This is deliberate - it is the validation tenant (Argo autosync off, targetRevision pinned).
- ai-memory left on `odMirrorMode=variants` + `unsafeCustomBuildID=main-5336556b` (the consolidated real-app example; revert = remove both HelmRelease values keys).
- Harness (`rerouter-e2e` ns incl. VPA), test workflows, taints: all removed. Fleet generations: unchanged except the canary and enrichment-studio's own perpetual churn (+1 on gen ~680, unrelated).
- Restore points: TWC `1.6.0-atlan-2-8633f913...`, chart `0.1.2-beta-d263ae5`, rerouter `sha-4bce6904...`.
