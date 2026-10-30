package controller

import (
	"context"
	"errors"
	"fmt"
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
	since         time.Time
	checkedAt     time.Time
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

// holdPromotion reports whether twd's planner must not promote buildID yet.
func (c *registrationCache) holdPromotion(twd types.UID, buildID string) bool {
	e := c.get(twd)
	return e != nil && e.buildID == buildID && e.holdPromotion
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
// more than the worker. Polls end before this returns, so a promotion later in the reconcile can't
// route a task to one of them.
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
		e = &registration{buildID: target.BuildID, since: time.Now(), pools: r.choosePools(ctx, l, twd, current)}
	}
	own := pairsFor(current, declaredQueues(twd), e.pools)
	registered, err := describe(ctx, twd, wdn, target.BuildID)
	if err != nil && !isNotFound(err) {
		l.Info("could not read the target version's task queues; retrying", "error", err.Error())
		e.checkedAt = time.Now()
		r.registration.put(twd.UID, e)
		return
	}
	if missing := subtract(own, registered); len(missing) > 0 && !e.fellBack {
		registered = r.pollUntilRegistered(ctx, l, twd, wdn, target.BuildID, missing, describe)
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

// updateHold sets whether promotion must wait for a queue the Current version holds and a live TWD
// on the same worker deployment declares.
func (r *TemporalWorkerDeploymentReconciler) updateHold(
	ctx context.Context,
	l logr.Logger,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	e *registration,
	current, registered []versionQueue,
) {
	expected, err := r.expectedQueues(ctx, twd, e.buildID)
	if err != nil {
		l.Info("could not list the TWDs on this worker deployment; holding promotion", "error", err.Error())
		e.holdPromotion = true
		return
	}
	e.holdPromotion = len(subtract(pairsFor(current, expected, nil), registered)) > 0
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
	pools := map[string]bool{}
	for queue, variant := range declaredQueues(twd) {
		held := byQueue[queue]
		reason := ""
		switch {
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
// those declared by every other live TWD on the same worker deployment that targets the same build.
func (r *TemporalWorkerDeploymentReconciler) expectedQueues(
	ctx context.Context,
	twd *temporaliov1alpha1.TemporalWorkerDeployment,
	buildID string,
) (map[string]string, error) {
	out := map[string]string{}
	for q, v := range declaredQueues(twd) {
		out[q] = v
	}
	siblings, err := r.siblingTWDs(ctx, twd)
	if err != nil {
		return nil, err
	}
	for _, s := range siblings {
		if !s.DeletionTimestamp.IsZero() || s.Status.TargetVersion.BuildID != buildID {
			continue
		}
		for q, v := range declaredQueues(s) {
			out[q] = v
		}
	}
	return out, nil
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
