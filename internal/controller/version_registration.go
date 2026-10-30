package controller

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/go-logr/logr"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
	"github.com/temporalio/temporal-worker-controller/internal/k8s"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// RegistrationAnnotation opts a TWD into controller-side registration of new versions. Matching adds a
// task queue to a version when a worker of that version starts polling it, so without this every pool
// of a new version runs one worker until the version registers and is promoted. With it, the
// controller polls each of the target version's task queues once itself, with that version's
// deployment options, and the pools start at zero.
const RegistrationAnnotation = "twd.temporal.io/controller-registration"

const (
	// heldReportInterval spaces repeats of the PromotionHeld event while a promotion stays held;
	// Kubernetes drops events after about an hour.
	heldReportInterval = 30 * time.Minute
	// registrationPollTimeout bounds each registration poll. The frontend rejects a poll with less than
	// 2s left before it reaches matching, and logs a warning below 10s.
	registrationPollTimeout = 10 * time.Second
	// registrationDescribeInterval spaces the describes that tell a poll its queue has registered.
	registrationDescribeInterval = time.Second
	// registrationRetryInterval spaces attempts after a failed or incomplete registration.
	registrationRetryInterval = 30 * time.Second
	// registrationGrace is how long a pool may wait for the controller to register it before its
	// worker is started instead, as without the annotation.
	registrationGrace = 5 * time.Minute
)

// errRegistrationTaskReceived reports that a registration poll was handed a task. It cannot happen
// while the version is neither Current nor Ramping and no workflow is pinned to it; the task is
// failed back to the server, which retries it.
var errRegistrationTaskReceived = errors.New("registration poll received a task")

// versionQueue is one task queue and type held by a worker deployment version.
type versionQueue struct {
	name string
	typ  enumspb.TaskQueueType
}

func (q versionQueue) String() string {
	return fmt.Sprintf("%s/%s", q.name, q.typ)
}

// registration is the controller's registration state for one TWD's target build.
type registration struct {
	buildID string
	// pools are the spec.variants names ("" for the base) whose queues the controller registers. Their
	// Deployments start at zero and get no warm-start floor.
	pools map[string]bool
	// holdPromotion is set while the target misses a queue the Current version holds and a live TWD on
	// the same worker deployment declares. The server's own check lets an idle missing queue through,
	// and that queue would then go unversioned until a worker polls it.
	holdPromotion bool
	complete      bool
	fellBack      bool
	// polling is set while this TWD's registration polls are open. A sibling must not promote then:
	// a queue already registered still has a poll that could be handed a task.
	polling        bool
	heldReportedAt time.Time
	// proved is set once a worker of the build, not the controller, has been seen polling one of its
	// queues. Promotion waits for it: a Deployment at zero replicas is Available, so without it nothing
	// shows the build can start before it is promoted.
	proved    bool
	since     time.Time
	checkedAt time.Time
}

// registers reports whether the controller registers the given pool of this registration's build.
func (g *registration) registers(buildID, variant string) bool {
	return g != nil && g.buildID == buildID && !g.fellBack && g.pools[variant]
}

// registrationCache holds each TWD's registration state for its current target build.
type registrationCache struct {
	mu      sync.Mutex
	entries map[types.UID]*registration
}

func (c *registrationCache) get(twd types.UID) *registration {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e := c.entries[twd]; e != nil {
		cp := *e
		return &cp
	}
	return nil
}

func (c *registrationCache) put(twd types.UID, e *registration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[types.UID]*registration{}
	}
	c.entries[twd] = e
}

func (c *registrationCache) forget(twd types.UID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, twd)
}

// registers reports whether the controller registers a pool of twd's build.
func (c *registrationCache) registers(twd types.UID, buildID, variant string) bool {
	return c.get(twd).registers(buildID, variant)
}

// promotionHeld reports whether twd's planner must not promote its target yet. It fails closed: an
// opted-in TWD whose target still has to register is held until a registration pass for that build
// has succeeded and cleared the hold, so a Temporal error or a controller restart never lets the
// version through unchecked.
func (r *TemporalWorkerDeploymentReconciler) promotionHeld(twd *temporaliov1alpha1.TemporalWorkerDeployment) bool {
	if !registrationApplies(twd) {
		return false
	}
	e := r.registration.get(twd.UID)
	return e == nil || e.buildID != twd.Status.TargetVersion.BuildID || e.holdPromotion
}

func registrationEnabled(twd *temporaliov1alpha1.TemporalWorkerDeployment) bool {
	return twd.Annotations[RegistrationAnnotation] == "true" && twd.Spec.WorkerScaling != nil
}

// declaredQueues maps each task queue twd's pools poll to its spec.variants name ("" for the base).
func declaredQueues(twd *temporaliov1alpha1.TemporalWorkerDeployment) map[string]string {
	base := resolveTaskQueue(twd)
	if base == "" {
		return nil
	}
	out := map[string]string{base: ""}
	for _, v := range twd.Spec.Variants {
		out[base+v.TaskQueueSuffix] = v.Name
	}
	return out
}

// registerTargetVersion registers the queues of twd's target version, when the TWD opts in and the
// target still has to register before it can be promoted. It records which pools it covers and
// whether promotion must wait, for the ScaledObject reconcile and the planner of this reconcile.
//
// The queues and types come from the Current version, limited to the queues twd's spec declares, so
// the target registers exactly what the current workers poll. A pool keeps today's warm worker when
// the controller can't stand in for it: there is no Current version on this worker deployment (a
// first version, or a workerDeploymentName change), the Current version doesn't hold the pool's
// queue (a new pool), the queue has a type other than workflow or activity, or the Current version's
// pool has workers up, where the new worker will have work and a cold start at promotion would cost
// more than the worker.
//
// One pool per worker deployment always keeps its warm worker: the base pool of the canary TWD (see
// isCanary). Promotion waits for its queues, which only that worker registers, so a build is never
// promoted before one of its workers has started and polled; a build that can't start stays
// unpromoted and the Current version keeps serving.
//
// Polls end before this returns, so a promotion later in this reconcile can't route a task to one of
// them, and siblings hold their promotion while they are open.
func (r *TemporalWorkerDeploymentReconciler) registerTargetVersion(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) {
	st := &twd.Status
	target := st.TargetVersion
	if !registrationApplies(twd) {
		r.registration.forget(twd.UID)
		return
	}
	l = l.WithValues("phase", "registration", "buildID", target.BuildID)

	prev := r.registration.get(twd.UID)
	if prev != nil && prev.buildID != target.BuildID {
		prev = nil
	}
	if prev.upToDate() {
		if prev.complete && r.siblingPolling(ctx, twd, prev.buildID) {
			prev.holdPromotion = true
			prev.complete = false        // re-evaluate the hold on the next pass,
			prev.checkedAt = time.Time{} // without waiting for the retry interval
			r.registration.put(twd.UID, prev)
		}
		return
	}

	describe := r.describeVersionQueues
	if describe == nil {
		describe = r.describeVersionQueuesFromTemporal
	}
	wdn := k8s.ComputeWorkerDeploymentName(twd)
	current, err := describe(ctx, twd, wdn, st.CurrentVersion.BuildID)
	if err != nil {
		l.Info("could not read the Current version's task queues; retrying", "error", err.Error())
		if prev == nil {
			return // no pools chosen: every pool keeps its warm worker
		}
		prev.checkedAt = time.Now()
		r.registration.put(twd.UID, prev)
		return
	}

	e := prev
	if e == nil {
		e = &registration{buildID: target.BuildID, since: time.Now(), holdPromotion: true,
			pools: r.choosePools(ctx, l, twd, current)}
	} else if e.pools[""] && r.isCanary(ctx, twd) {
		// The canary changed under this release (a sibling left): this base pool must now prove
		// the build with its own worker.
		l.Info("this TWD became its worker deployment's canary; starting its base worker")
		delete(e.pools, "")
	} else if e.pools[""] && !e.proved && time.Since(e.since) > registrationGrace {
		// No worker of the build has polled within the grace: the canary may be failing for its
		// own reasons (a secret, a node selector). This TWD's base worker proves the build instead.
		l.Info("no worker of the build has polled; starting this TWD's base worker to prove it")
		delete(e.pools, "")
	}
	own := pairsFor(current, declaredQueues(twd), e.pools)
	registered, err := describe(ctx, twd, wdn, target.BuildID)
	if err != nil && !isNotFound(err) {
		l.Info("could not read the target version's task queues; retrying", "error", err.Error())
		e.holdPromotion = true
		e.checkedAt = time.Now()
		r.registration.put(twd.UID, e)
		return
	}
	if missing := subtract(own, registered); len(missing) > 0 && !e.fellBack {
		e.polling = true
		r.registration.put(twd.UID, e)
		registered = r.pollUntilRegistered(ctx, l, twd, wdn, target.BuildID, missing, describe)
		e.polling = false
	}
	wasComplete := prev != nil && prev.complete
	e.complete = len(subtract(own, registered)) == 0
	e.checkedAt = time.Now()
	r.recordOutcome(l, twd, e, len(own), wasComplete)
	r.updateHold(ctx, l, twd, e, current, registered)
	r.registration.put(twd.UID, e)
}

// registrationApplies reports whether twd opts in and its target still has to register before it
// can be promoted, with a Current version to take the queues from.
func registrationApplies(twd *temporaliov1alpha1.TemporalWorkerDeployment) bool {
	st := &twd.Status
	target := st.TargetVersion
	if !registrationEnabled(twd) || target.BuildID == "" || st.CurrentVersion == nil || st.CurrentVersion.BuildID == target.BuildID {
		return false
	}
	switch target.Status {
	case "", temporaliov1alpha1.VersionStatusNotRegistered, temporaliov1alpha1.VersionStatusInactive:
		return true
	case temporaliov1alpha1.VersionStatusRamping, temporaliov1alpha1.VersionStatusCurrent,
		temporaliov1alpha1.VersionStatusDraining, temporaliov1alpha1.VersionStatusDrained:
		return false
	}
	return false
}

// upToDate reports whether this reconcile can reuse the registration as it stands: complete with
// nothing to wait for, or failed and not yet due for a retry.
func (g *registration) upToDate() bool {
	if g == nil {
		return false
	}
	if g.complete && !g.holdPromotion {
		return true
	}
	return !g.complete && !g.fellBack && time.Since(g.checkedAt) < registrationRetryInterval
}

// recordOutcome reports a completed registration once, and hands the pools back to their warm
// workers when registration has not completed within the grace.
func (r *TemporalWorkerDeploymentReconciler) recordOutcome(
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	e *registration,
	queues int,
	wasComplete bool,
) {
	if !e.complete && !e.fellBack && time.Since(e.since) > registrationGrace {
		e.fellBack = true
		r.Recorder.Eventf(twd, corev1.EventTypeWarning, ReasonRegistrationFellBack,
			"Could not register build %q within %s; starting a worker in each pool so it registers itself",
			e.buildID, registrationGrace)
	}
	if e.complete && queues > 0 && !wasComplete {
		l.Info("registered the target version's task queues", "queues", queues)
		r.Recorder.Eventf(twd, corev1.EventTypeNormal, ReasonVersionRegistered,
			"Registered %d task queue(s) of build %q without starting workers", queues, e.buildID)
	}
}

// updateHold sets whether promotion must wait: for a queue the Current version holds and a live TWD
// on the same worker deployment declares, or for a sibling whose registration polls are still open.
// The first time it holds once this TWD's own part is done, it says what it waits for.
func (r *TemporalWorkerDeploymentReconciler) updateHold(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	e *registration,
	current, registered []versionQueue,
) {
	expected, siblings, err := r.expectedQueues(ctx, twd, e.buildID)
	if err != nil {
		l.Info("could not list the TWDs on this worker deployment; holding promotion", "error", err.Error())
		e.holdPromotion = true
		return
	}
	var missing []versionQueue
	if time.Since(e.since) <= registrationGrace {
		missing = subtract(pairsFor(current, expected, nil), registered)
		missing = append(missing, unregisteredNewQueues(expected, current, registered)...)
	} else {
		// After the grace a queue counts once any of its types registered: a type the build's
		// workers no longer poll must not hold a release for good. Every queue still needs one.
		missing = unregisteredQueues(pairsFor(current, expected, nil), registered)
	}
	polling := false
	for _, uid := range siblings {
		if g := r.registration.get(uid); g != nil && g.buildID == e.buildID && g.polling {
			polling = true
		}
	}
	if len(missing) == 0 && !polling && !e.proved {
		e.proved = r.workerPolled(ctx, l, twd, e.buildID, registered)
	}
	e.holdPromotion = len(missing) > 0 || polling || !e.proved
	if e.holdPromotion && (e.complete || e.fellBack) && (len(missing) > 0 || !e.proved) &&
		time.Since(e.heldReportedAt) > heldReportInterval {
		e.heldReportedAt = time.Now()
		if len(missing) > 0 {
			r.Recorder.Eventf(twd, corev1.EventTypeNormal, ReasonPromotionHeld,
				"Holding promotion of build %q until its workers register %d task queue(s), e.g. %s",
				e.buildID, len(missing), missing[0])
		} else {
			r.Recorder.Eventf(twd, corev1.EventTypeNormal, ReasonPromotionHeld,
				"Holding promotion of build %q until one of its workers polls", e.buildID)
		}
	}
}

// siblingPolling reports whether a live sibling on twd's worker deployment has registration polls open
// for buildID.
func (r *TemporalWorkerDeploymentReconciler) siblingPolling(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
) bool {
	siblings, err := r.sameBuildSiblings(ctx, twd, buildID)
	if err != nil {
		return true
	}
	for _, s := range siblings {
		if g := r.registration.get(s.UID); g != nil && g.buildID == buildID && g.polling {
			return true
		}
	}
	return false
}

// workerPolled reports whether a worker of the build, any poller but the controller itself, has
// polled one of the build's queues within the server's poller history.
func (r *TemporalWorkerDeploymentReconciler) workerPolled(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
	queues []versionQueue,
) bool {
	pollers := r.describePollers
	if pollers == nil {
		pollers = r.describePollersFromTemporal
	}
	wdn := k8s.ComputeWorkerDeploymentName(twd)
	self := getControllerIdentity()
	for _, q := range queues {
		if q.typ != enumspb.TASK_QUEUE_TYPE_WORKFLOW && q.typ != enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			continue
		}
		seen, err := pollers(ctx, twd, q)
		if err != nil {
			l.Info("could not read the task queue's pollers; retrying", "taskQueue", q.name, "error", err.Error())
			return false
		}
		for _, p := range seen {
			if p.identity != self && p.deploymentName == wdn && p.buildID == buildID {
				return true
			}
		}
	}
	return false
}

// poller is one entry of a task queue's recent pollers.
type poller struct {
	identity       string
	deploymentName string
	buildID        string
}

// describePollersFromTemporal returns the recent pollers of one task queue and type.
func (r *TemporalWorkerDeploymentReconciler) describePollersFromTemporal(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	q versionQueue,
) ([]poller, error) {
	if r.TemporalClientPool == nil {
		return nil, errors.New("no Temporal client pool")
	}
	c, err := r.temporalClientFor(ctx, twd)
	if err != nil {
		return nil, err
	}
	resp, err := c.WorkflowService().DescribeTaskQueue(ctx, &workflowservice.DescribeTaskQueueRequest{
		Namespace:     twd.Spec.WorkerOptions.TemporalNamespace,
		TaskQueue:     &taskqueuepb.TaskQueue{Name: q.name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		TaskQueueType: q.typ,
	})
	if err != nil {
		return nil, err
	}
	out := make([]poller, 0, len(resp.GetPollers()))
	for _, p := range resp.GetPollers() {
		out = append(out, poller{
			identity:       p.GetIdentity(),
			deploymentName: p.GetDeploymentOptions().GetDeploymentName(),
			buildID:        p.GetDeploymentOptions().GetBuildId(),
		})
	}
	return out, nil
}

// unregisteredNewQueues returns the declared queues the Current version does not hold (a new pool, or
// one a previous version was promoted without) that the target holds no type of yet. Their workers
// start as without the controller and register them; promotion waits for that, within the grace, so
// the queue does not go unversioned at promotion. Their types are unknown until a worker polls, so
// one registered type is enough.
func unregisteredNewQueues(expected map[string]string, current, registered []versionQueue) []versionQueue {
	held := map[string]bool{}
	for _, q := range current {
		held[q.name] = true
	}
	for _, q := range registered {
		held[q.name] = true
	}
	var out []versionQueue
	for name := range expected {
		if !held[name] {
			out = append(out, versionQueue{name: name, typ: enumspb.TASK_QUEUE_TYPE_UNSPECIFIED})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// unregisteredQueues returns one entry per queue of want that has no type registered at all.
func unregisteredQueues(want, registered []versionQueue) []versionQueue {
	have := map[string]bool{}
	for _, q := range registered {
		have[q.name] = true
	}
	seen := map[string]bool{}
	var out []versionQueue
	for _, q := range want {
		if !have[q.name] && !seen[q.name] {
			seen[q.name] = true
			out = append(out, versionQueue{name: q.name, typ: enumspb.TASK_QUEUE_TYPE_UNSPECIFIED})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// choosePools picks the pools whose queues the controller registers for a new target build. The
// choice is made once per build, so a pool started at zero is never handed back to a floor except by
// the grace fallback.
func (r *TemporalWorkerDeploymentReconciler) choosePools(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	current []versionQueue,
) map[string]bool {
	byQueue := map[string][]versionQueue{}
	for _, q := range current {
		byQueue[q.name] = append(byQueue[q.name], q)
	}
	canary := r.isCanary(ctx, twd)
	pools := map[string]bool{}
	for queue, variant := range declaredQueues(twd) {
		held := byQueue[queue]
		reason := ""
		switch {
		case variant == "" && canary:
			reason = "this pool's worker proves the new build runs before it is promoted"
		case len(held) == 0:
			reason = "the Current version does not hold this pool's queue"
		case !onlyWorkflowAndActivity(held):
			reason = "the queue has a task type the controller does not register"
		case r.currentPoolBusy(ctx, twd, variant):
			reason = "the Current version's pool has workers up"
		}
		if reason != "" {
			l.Info("keeping a warm worker for this pool's registration", "taskQueue", queue, "reason", reason)
			continue
		}
		pools[variant] = true
	}
	return pools
}

func onlyWorkflowAndActivity(qs []versionQueue) bool {
	for _, q := range qs {
		if q.typ != enumspb.TASK_QUEUE_TYPE_WORKFLOW && q.typ != enumspb.TASK_QUEUE_TYPE_ACTIVITY {
			return false
		}
	}
	return true
}

// currentPoolBusy reports whether the Current version's Deployment for the pool has workers wanted.
// It answers true when the Deployment can't be read, which keeps the pool's warm worker.
func (r *TemporalWorkerDeploymentReconciler) currentPoolBusy(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	variant string,
) bool {
	cv := twd.Status.CurrentVersion
	ref := cv.Deployment
	if variant != "" {
		ref = nil
		for _, vs := range cv.Variants {
			if vs.Name == variant {
				ref = vs.Deployment
			}
		}
	}
	if ref == nil {
		return true
	}
	var d appsv1.Deployment
	if err := r.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, &d); err != nil {
		return true
	}
	return d.Spec.Replicas == nil || *d.Spec.Replicas > 0
}

// expectedQueues are the task queues the target must hold before promotion: those twd declares, and
// those declared by every other live TWD on the same worker deployment whose spec moves to the same
// build. The spec, not the status: a release updates every TWD's spec together, and a sibling's
// status names the new build only after its own next reconcile, by which time this TWD may have
// promoted the version without that sibling's queues.
func (r *TemporalWorkerDeploymentReconciler) expectedQueues(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
) (map[string]string, []types.UID, error) {
	out := map[string]string{}
	for q, v := range declaredQueues(twd) {
		out[q] = v
	}
	siblings, err := r.sameBuildSiblings(ctx, twd, buildID)
	if err != nil {
		return nil, nil, err
	}
	uids := make([]types.UID, 0, len(siblings))
	for _, s := range siblings {
		uids = append(uids, s.UID)
		for q, v := range declaredQueues(s) {
			out[q] = v
		}
	}
	return out, uids, nil
}

// sameBuildSiblings returns the other live TWDs on twd's worker deployment whose spec moves to buildID.
func (r *TemporalWorkerDeploymentReconciler) sameBuildSiblings(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
) ([]*temporaliov1alpha1.TemporalWorkerDeployment, error) {
	siblings, err := r.siblingTWDs(ctx, twd)
	if err != nil {
		return nil, err
	}
	var out []*temporaliov1alpha1.TemporalWorkerDeployment
	for _, s := range siblings {
		if s.DeletionTimestamp.IsZero() && k8s.ComputeBuildID(s) == buildID {
			out = append(out, s)
		}
	}
	return out, nil
}

// isCanary reports whether twd's base pool is the one pool on its worker deployment that keeps a warm
// worker through a release: of twd and its live siblings moving to the same build, the TWD whose base
// worker requests the least CPU, then the first by name. Every TWD computes the same answer. It
// answers true when the siblings can't be listed, which keeps the warm worker.
func (r *TemporalWorkerDeploymentReconciler) isCanary(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
) bool {
	siblings, err := r.sameBuildSiblings(ctx, twd, k8s.ComputeBuildID(twd))
	if err != nil {
		return true
	}
	best := twd
	for _, s := range siblings {
		if cheaperCanary(s, best) {
			best = s
		}
	}
	return best.UID == twd.UID
}

// cheaperCanary orders canary candidates by the CPU their base worker requests, then by name.
func cheaperCanary(a, b *temporaliov1alpha1.TemporalWorkerDeployment) bool {
	ca, cb := baseCPURequest(a), baseCPURequest(b)
	if ca != cb {
		return ca < cb
	}
	return a.Name < b.Name
}

// baseCPURequest is the CPU the TWD's worker container requests, in millicores; unset sorts last.
func baseCPURequest(twd *temporaliov1alpha1.TemporalWorkerDeployment) int64 {
	cs := twd.Spec.Template.Spec.Containers
	if len(cs) == 0 {
		return math.MaxInt64
	}
	q, ok := cs[0].Resources.Requests[corev1.ResourceCPU]
	if !ok {
		return math.MaxInt64
	}
	return q.MilliValue()
}

// pairsFor returns the Current version's queue/type pairs whose queue is in declared and, when pools
// is set, belongs to one of those pools.
func pairsFor(current []versionQueue, declared map[string]string, pools map[string]bool) []versionQueue {
	var out []versionQueue
	for _, q := range current {
		variant, ok := declared[q.name]
		if !ok || (pools != nil && !pools[variant]) {
			continue
		}
		out = append(out, q)
	}
	return out
}

func subtract(want, have []versionQueue) []versionQueue {
	got := make(map[versionQueue]bool, len(have))
	for _, q := range have {
		got[q] = true
	}
	var out []versionQueue
	for _, q := range want {
		if !got[q] {
			out = append(out, q)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func isNotFound(err error) bool {
	var nf *serviceerror.NotFound
	return errors.As(err, &nf)
}

// pollUntilRegistered polls each missing queue with the target's deployment options, ends each poll
// as soon as the target lists its queue, and returns once every poll has returned. It returns the
// target's queues as last described.
func (r *TemporalWorkerDeploymentReconciler) pollUntilRegistered(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	wdn, buildID string,
	missing []versionQueue,
	describe func(context.Context, *temporaliov1alpha1.TemporalWorkerDeployment, string, string) ([]versionQueue, error),
) []versionQueue {
	cancels, done := r.startRegistrationPolls(ctx, l, twd, wdn, buildID, missing)
	var registered []versionQueue
	ticker := time.NewTicker(registrationDescribeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			if got, err := describe(ctx, twd, wdn, buildID); err == nil {
				registered = got
			}
			return registered
		case <-ticker.C:
			got, err := describe(ctx, twd, wdn, buildID)
			if err != nil {
				continue
			}
			registered = got
			have := make(map[versionQueue]bool, len(got))
			for _, q := range got {
				have[q] = true
			}
			for q, cancel := range cancels {
				if have[q] {
					cancel()
				}
			}
		}
	}
}

// startRegistrationPolls starts one poll per missing queue. It returns each poll's cancel and a
// channel closed once every poll has returned.
func (r *TemporalWorkerDeploymentReconciler) startRegistrationPolls(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	wdn, buildID string,
	missing []versionQueue,
) (map[versionQueue]context.CancelFunc, <-chan struct{}) {
	poll := r.pollVersionQueue
	if poll == nil {
		poll = r.pollVersionQueueFromTemporal
	}
	cancels := make(map[versionQueue]context.CancelFunc, len(missing))
	var wg sync.WaitGroup
	for _, q := range missing {
		pctx, cancel := context.WithTimeout(ctx, registrationPollTimeout)
		cancels[q] = cancel
		wg.Add(1)
		go func(q versionQueue) {
			defer wg.Done()
			defer cancel()
			r.reportPollEnd(l, twd, buildID, q, poll(pctx, twd, wdn, buildID, q))
		}(q)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return cancels, done
}

// reportPollEnd logs a registration poll that ended with an error. A task handed to the poll is
// logged at error level and recorded as an event: it should never happen.
func (r *TemporalWorkerDeploymentReconciler) reportPollEnd(
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
	q versionQueue,
	err error,
) {
	switch {
	case err == nil:
	case errors.Is(err, errRegistrationTaskReceived):
		l.Error(err, "a registration poll received a task and failed it back to the server",
			"taskQueue", q.name, "type", q.typ.String())
		r.Recorder.Eventf(twd, corev1.EventTypeWarning, ReasonRegistrationTaskReceived,
			"A registration poll of %s for build %q received a task; it was failed back to the server for retry", q, buildID)
	default:
		l.Info("registration poll ended with an error", "taskQueue", q.name, "type", q.typ.String(), "error", err.Error())
	}
}

// describeVersionQueuesFromTemporal returns the task queues and types a version of the worker
// deployment holds.
func (r *TemporalWorkerDeploymentReconciler) describeVersionQueuesFromTemporal(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	wdn, buildID string,
) ([]versionQueue, error) {
	if r.TemporalClientPool == nil {
		return nil, errors.New("no Temporal client pool")
	}
	c, err := r.temporalClientFor(ctx, twd)
	if err != nil {
		return nil, err
	}
	desc, err := c.WorkerDeploymentClient().GetHandle(wdn).DescribeVersion(ctx,
		sdkclient.WorkerDeploymentDescribeVersionOptions{BuildID: buildID})
	if err != nil {
		return nil, err
	}
	out := make([]versionQueue, 0, len(desc.Info.TaskQueuesInfos))
	for _, tq := range desc.Info.TaskQueuesInfos {
		t := enumspb.TASK_QUEUE_TYPE_UNSPECIFIED
		switch tq.Type {
		case sdkclient.TaskQueueTypeWorkflow:
			t = enumspb.TASK_QUEUE_TYPE_WORKFLOW
		case sdkclient.TaskQueueTypeActivity:
			t = enumspb.TASK_QUEUE_TYPE_ACTIVITY
		case sdkclient.TaskQueueTypeNexus:
			t = enumspb.TASK_QUEUE_TYPE_NEXUS
		}
		out = append(out, versionQueue{name: tq.Name, typ: t})
	}
	return out, nil
}

// pollVersionQueueFromTemporal makes one poll of q as a worker of the version would. Matching adds the
// queue to the version when the poll starts, before it waits for a task. The poll's end by deadline or
// cancel is its normal outcome.
func (r *TemporalWorkerDeploymentReconciler) pollVersionQueueFromTemporal(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	wdn, buildID string,
	q versionQueue,
) error {
	if r.TemporalClientPool == nil {
		return errors.New("no Temporal client pool")
	}
	c, err := r.temporalClientFor(ctx, twd)
	if err != nil {
		return err
	}
	ns := twd.Spec.WorkerOptions.TemporalNamespace
	opts := &deploymentpb.WorkerDeploymentOptions{
		DeploymentName:       wdn,
		BuildId:              buildID,
		WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED,
	}
	tq := &taskqueuepb.TaskQueue{Name: q.name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
	identity := getControllerIdentity()
	failure := &failurepb.Failure{
		Message: "received by the worker controller's registration poll, which runs no workflow or activity code",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type: "WorkerControllerRegistrationPoll",
		}},
	}
	// The task goes back on a fresh context: the poll's own is cancelled once its queue registers.
	respondCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), registrationPollTimeout)
	defer cancel()

	switch q.typ {
	case enumspb.TASK_QUEUE_TYPE_WORKFLOW:
		resp, err := c.WorkflowService().PollWorkflowTaskQueue(ctx, &workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: ns, TaskQueue: tq, Identity: identity, DeploymentOptions: opts,
		})
		if err != nil {
			return pollEnd(ctx, err)
		}
		if len(resp.GetTaskToken()) == 0 {
			return nil
		}
		if _, err := c.WorkflowService().RespondWorkflowTaskFailed(respondCtx, &workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: ns, TaskToken: resp.GetTaskToken(), Identity: identity, DeploymentOptions: opts, Failure: failure,
			Cause: enumspb.WORKFLOW_TASK_FAILED_CAUSE_UNSPECIFIED,
		}); err != nil {
			return fmt.Errorf("%w; failing it back: %v", errRegistrationTaskReceived, err)
		}
		return errRegistrationTaskReceived
	case enumspb.TASK_QUEUE_TYPE_ACTIVITY:
		resp, err := c.WorkflowService().PollActivityTaskQueue(ctx, &workflowservice.PollActivityTaskQueueRequest{
			Namespace: ns, TaskQueue: tq, Identity: identity, DeploymentOptions: opts,
		})
		if err != nil {
			return pollEnd(ctx, err)
		}
		if len(resp.GetTaskToken()) == 0 {
			return nil
		}
		if _, err := c.WorkflowService().RespondActivityTaskFailed(respondCtx, &workflowservice.RespondActivityTaskFailedRequest{
			Namespace: ns, TaskToken: resp.GetTaskToken(), Identity: identity, DeploymentOptions: opts, Failure: failure,
		}); err != nil {
			return fmt.Errorf("%w; failing it back: %v", errRegistrationTaskReceived, err)
		}
		return errRegistrationTaskReceived
	case enumspb.TASK_QUEUE_TYPE_UNSPECIFIED, enumspb.TASK_QUEUE_TYPE_NEXUS:
	}
	return fmt.Errorf("task queue type %s is not registered by the controller", q.typ)
}

// pollEnd treats a poll that ended because its own deadline passed or it was cancelled as finished.
func pollEnd(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return nil
	}
	return err
}
