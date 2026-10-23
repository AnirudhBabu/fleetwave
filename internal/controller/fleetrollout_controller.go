package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/health"
	"github.com/AnirudhBabu/fleetwave/internal/planner"
	"github.com/AnirudhBabu/fleetwave/internal/remote"
	"github.com/AnirudhBabu/fleetwave/internal/revision"
)

const (
	finalizer = "fleet.fleetwave.io/cleanup"
	// LabelRolloutName is set on ControllerRevisions owned by a FleetRollout.
	LabelRolloutName = "fleet.fleetwave.io/rollout-name"
	// revisionHistory is how many ControllerRevisions to keep per rollout,
	// on top of any still referenced by the stable revision or a cluster.
	revisionHistory = 5
)

// FleetRolloutReconciler rolls revisions across member clusters.
type FleetRolloutReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
	Remotes  remote.Getter

	// PollInterval is how often in-flight clusters are re-checked.
	PollInterval time.Duration
	// DriftInterval re-applies a finished rollout to undo manual edits in
	// member clusters. 0 disables it.
	DriftInterval time.Duration
	// ApplyConcurrency bounds parallel member operations per reconcile.
	ApplyConcurrency int
	// Now is injectable for tests.
	Now func() time.Time
}

// +kubebuilder:rbac:groups=fleet.fleetwave.io,resources=fleetrollouts,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=fleet.fleetwave.io,resources=fleetrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fleet.fleetwave.io,resources=fleetrollouts/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=controllerrevisions,verbs=get;list;watch;create;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch

// Reconcile moves one FleetRollout forward.
func (r *FleetRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var fr fleetv1.FleetRollout
	if err := r.Get(ctx, req.NamespacedName, &fr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fr.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, &fr)
	}
	if controllerutil.AddFinalizer(&fr, finalizer) {
		if err := r.Update(ctx, &fr); err != nil {
			return ctrl.Result{}, err
		}
	}

	objs, err := revision.Parse(fr.Spec.Resources)
	if err != nil {
		return r.invalid(ctx, &fr, err)
	}
	rev, err := revision.Hash(objs)
	if err != nil {
		return r.invalid(ctx, &fr, err)
	}
	store := &revisionStore{c: r.Client, fr: &fr, cache: map[string][]*unstructured.Unstructured{rev: objs}}
	if err := store.ensure(ctx, r.Scheme, rev, objs); err != nil {
		return ctrl.Result{}, err
	}

	strategy := withDefaults(fr.Spec.Strategy)
	sel, err := metav1.LabelSelectorAsSelector(&fr.Spec.ClusterSelector)
	if err != nil {
		return r.invalid(ctx, &fr, fmt.Errorf("clusterSelector: %w", err))
	}
	var mcs fleetv1.MemberClusterList
	if err := r.List(ctx, &mcs, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return ctrl.Result{}, err
	}
	byName := map[string]*fleetv1.MemberCluster{}
	members := make([]planner.Member, 0, len(mcs.Items))
	for i := range mcs.Items {
		mc := &mcs.Items[i]
		byName[mc.Name] = mc
		members = append(members, planner.Member{Name: mc.Name, Canary: mc.Labels[fleetv1.LabelCanary] == "true"})
	}
	waves, err := planner.AssignWaves(members, strategy.Waves)
	if err != nil {
		return r.invalid(ctx, &fr, fmt.Errorf("strategy.waves: %w", err))
	}

	now := r.now()
	newRevision := fr.Status.Revision != rev
	prevPhase := fr.Status.Phase
	in := planner.Input{
		Now:              now,
		Desired:          rev,
		Stable:           fr.Status.StableRevision,
		Phase:            prevPhase,
		CurrentWave:      int(fr.Status.CurrentWave),
		TotalWaves:       len(waves),
		Paused:           fr.Spec.Paused,
		BakeTime:         strategy.BakeTime.Duration,
		ProgressDeadline: strategy.ProgressDeadline.Duration,
		MaxFailures:      int(strategy.MaxFailures),
		AutoRollback:     *strategy.AutoRollback,
		PollInterval:     r.PollInterval,
		DriftInterval:    r.DriftInterval,
	}
	if fr.Status.WaveCompletedAt != nil {
		t := fr.Status.WaveCompletedAt.Time
		in.WaveCompletedAt = &t
	}
	if newRevision || prevPhase == fleetv1.PhaseInvalid {
		in.Phase, in.CurrentWave, in.WaveCompletedAt = "", 0, nil
		if newRevision && fr.Status.Revision != "" {
			r.Recorder.Eventf(&fr, corev1.EventTypeNormal, "NewRevision", "rolling out revision %s (stable: %s)", rev, orNone(fr.Status.StableRevision))
		}
	}

	prev := map[string]fleetv1.ClusterRolloutStatus{}
	for _, c := range fr.Status.Clusters {
		prev[c.Name] = c
	}

	// Gather observations. Health checks hit member API servers, so run
	// them in parallel; one slow cluster should not stall the wave.
	var clusters []planner.Cluster
	for w, names := range waves {
		for _, name := range names {
			mc := byName[name]
			p := prev[name]
			c := planner.Cluster{
				Name:      name,
				Wave:      w,
				Revision:  p.Revision,
				State:     p.State,
				Reachable: meta.IsStatusConditionTrue(mc.Status.Conditions, fleetv1.ConditionReady),
				Cordoned:  mc.Spec.Cordoned,
			}
			if p.AppliedAt != nil {
				c.AppliedAt = p.AppliedAt.Time
			}
			if newRevision && c.State == fleetv1.ClusterFailed {
				c.State = fleetv1.ClusterProgressing // a new revision gets a fresh verdict
			}
			if c.State == fleetv1.ClusterFailed {
				c.Health = health.Result{Status: health.Degraded, Message: p.Message}
			}
			clusters = append(clusters, c)
		}
	}
	checkHealth := !in.Phase.Terminal() && in.Phase != fleetv1.PhaseSucceeded
	r.forEach(ctx, clusters, func(ctx context.Context, c *planner.Cluster) {
		if !checkHealth || c.Revision != rev || c.State == fleetv1.ClusterFailed || !c.Reachable {
			return
		}
		rem, err := r.Remotes.Get(ctx, byName[c.Name])
		if err != nil {
			c.Health = health.Result{Status: health.Unknown, Message: err.Error()}
			return
		}
		c.Health = assess(ctx, rem.Client, objs)
	})
	in.Clusters = clusters

	out := planner.Plan(in)

	// Execute.
	owner := fr.Namespace + "/" + fr.Name
	idx := map[string]int{}
	for i, c := range out.Clusters {
		idx[c.Name] = i
	}
	retrySoon := false
	var mu sync.Mutex

	type action struct {
		kind string // apply, reapply, rollback
		name string
	}
	var actions []action
	for _, n := range out.Apply {
		actions = append(actions, action{"apply", n})
	}
	for _, n := range out.Reapply {
		actions = append(actions, action{"reapply", n})
	}
	for _, n := range out.Rollback {
		actions = append(actions, action{"rollback", n})
	}
	// Preload snapshots outside the parallel section.
	needed := map[string]bool{}
	for _, a := range actions {
		c := out.Clusters[idx[a.name]]
		if c.Revision != "" {
			needed[c.Revision] = true
		}
		if a.kind == "rollback" {
			needed[out.StableRevision] = true
		}
	}
	for rv := range needed {
		if _, err := store.load(ctx, rv); err != nil {
			log.Info("revision snapshot unavailable; pruning skipped for it", "revision", rv, "error", err.Error())
		}
	}

	r.forEachAction(ctx, len(actions), func(ctx context.Context, i int) {
		a := actions[i]
		mu.Lock()
		c := out.Clusters[idx[a.name]]
		mu.Unlock()
		rem, err := r.Remotes.Get(ctx, byName[a.name])
		var target string
		var targetObjs []*unstructured.Unstructured
		var stale []revision.Ref
		if err == nil {
			switch a.kind {
			case "apply", "reapply":
				target, targetObjs = rev, objs
			case "rollback":
				target = out.StableRevision
				targetObjs, err = store.load(ctx, target)
			}
		}
		if err == nil && c.Revision != "" && c.Revision != target {
			if old, lerr := store.load(ctx, c.Revision); lerr == nil {
				stale = revision.Stale(revision.Inventory(old), revision.Inventory(targetObjs))
			}
		}
		if err == nil {
			err = applyRevision(ctx, rem.Client, owner, target, targetObjs, stale)
		}

		mu.Lock()
		defer mu.Unlock()
		cc := &out.Clusters[idx[a.name]]
		switch {
		case err == nil && a.kind == "apply":
			cc.Revision, cc.AppliedAt, cc.State, cc.Message = rev, now, fleetv1.ClusterProgressing, "applied; waiting for workloads"
		case err == nil && a.kind == "reapply":
			cc.AppliedAt = now
		case err == nil && a.kind == "rollback":
			cc.Revision, cc.AppliedAt, cc.State = target, now, fleetv1.ClusterRolledBack
			if cc.Message == "" {
				cc.Message = "rolled back to " + target
			}
		case a.kind == "apply" && permanent(err):
			// Partially applied and will not get better by retrying: record
			// it against this revision so the failure budget sees it.
			cc.Revision, cc.AppliedAt, cc.State, cc.Message = rev, now, fleetv1.ClusterFailed, "apply rejected: "+err.Error()
			r.Recorder.Eventf(&fr, corev1.EventTypeWarning, "ApplyRejected", "cluster %s: %v", a.name, err)
		default:
			cc.Message = fmt.Sprintf("%s failed, will retry: %v", a.kind, err)
			retrySoon = true
			log.Info("member operation failed", "cluster", a.name, "action", a.kind, "error", err.Error())
		}
	})
	if len(out.Rollback) > 0 && prevPhase != fleetv1.PhaseRolledBack {
		rollbacksTotal.WithLabelValues(fr.Namespace, fr.Name).Inc()
	}
	// A permanent apply failure may have used up the failure budget; come
	// back quickly so the planner can halt or roll back.
	if hasNewFailure(out) {
		retrySoon = true
	}

	for _, e := range out.Events {
		t := corev1.EventTypeNormal
		if e.Warning {
			t = corev1.EventTypeWarning
		}
		r.Recorder.Event(&fr, t, e.Reason, e.Message)
	}

	// Status.
	fr.Status.ObservedGeneration = fr.Generation
	fr.Status.Revision = rev
	fr.Status.StableRevision = out.StableRevision
	fr.Status.Phase = out.Phase
	fr.Status.CurrentWave = int32(out.CurrentWave)
	fr.Status.TotalWaves = int32(len(waves))
	fr.Status.Message = out.Message
	fr.Status.WaveCompletedAt = nil
	if out.WaveCompletedAt != nil {
		t := metav1.NewTime(*out.WaveCompletedAt)
		fr.Status.WaveCompletedAt = &t
	}
	fr.Status.Clusters = make([]fleetv1.ClusterRolloutStatus, 0, len(out.Clusters))
	for _, c := range out.Clusters {
		s := fleetv1.ClusterRolloutStatus{Name: c.Name, Wave: int32(c.Wave), Revision: c.Revision, State: c.State, Message: c.Message}
		if !c.AppliedAt.IsZero() {
			t := metav1.NewTime(c.AppliedAt)
			s.AppliedAt = &t
		}
		fr.Status.Clusters = append(fr.Status.Clusters, s)
	}
	setConditions(&fr)
	if err := r.Status().Update(ctx, &fr); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		return ctrl.Result{}, err
	}
	recordMetrics(&fr)

	if err := store.gc(ctx, &fr); err != nil {
		log.Info("revision gc failed", "error", err.Error())
	}

	res := ctrl.Result{RequeueAfter: out.RequeueAfter}
	if retrySoon && (res.RequeueAfter == 0 || res.RequeueAfter > r.poll()) {
		res.RequeueAfter = r.poll()
	}
	return res, nil
}

func hasNewFailure(out planner.Output) bool {
	for _, c := range out.Clusters {
		if c.State == fleetv1.ClusterFailed && strings.HasPrefix(c.Message, "apply rejected") {
			return true
		}
	}
	return false
}

func (r *FleetRolloutReconciler) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *FleetRolloutReconciler) poll() time.Duration {
	if r.PollInterval > 0 {
		return r.PollInterval
	}
	return 5 * time.Second
}

func (r *FleetRolloutReconciler) concurrency() int {
	if r.ApplyConcurrency > 0 {
		return r.ApplyConcurrency
	}
	return 10
}

func (r *FleetRolloutReconciler) forEach(ctx context.Context, cs []planner.Cluster, fn func(context.Context, *planner.Cluster)) {
	r.forEachAction(ctx, len(cs), func(ctx context.Context, i int) { fn(ctx, &cs[i]) })
}

func (r *FleetRolloutReconciler) forEachAction(ctx context.Context, n int, fn func(context.Context, int)) {
	sem := make(chan struct{}, r.concurrency())
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			fn(ctx, i)
		}(i)
	}
	wg.Wait()
}

func (r *FleetRolloutReconciler) invalid(ctx context.Context, fr *fleetv1.FleetRollout, cause error) (ctrl.Result, error) {
	if fr.Status.Phase != fleetv1.PhaseInvalid || fr.Status.Message != cause.Error() {
		r.Recorder.Event(fr, corev1.EventTypeWarning, "InvalidSpec", cause.Error())
	}
	fr.Status.ObservedGeneration = fr.Generation
	fr.Status.Phase = fleetv1.PhaseInvalid
	fr.Status.Message = cause.Error()
	setConditions(fr)
	if err := r.Status().Update(ctx, fr); err != nil {
		return ctrl.Result{}, err
	}
	recordMetrics(fr)
	// Waiting for a spec change; the generation watch will wake us.
	return ctrl.Result{}, nil
}

func (r *FleetRolloutReconciler) finalize(ctx context.Context, fr *fleetv1.FleetRollout) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(fr, finalizer) {
		return ctrl.Result{}, nil
	}
	if fr.Spec.DeletionPolicy != fleetv1.DeletionPolicyOrphan {
		store := &revisionStore{c: r.Client, fr: fr, cache: map[string][]*unstructured.Unstructured{}}
		owner := fr.Namespace + "/" + fr.Name
		var errs []string
		var mu sync.Mutex
		r.forEachAction(ctx, len(fr.Status.Clusters), func(ctx context.Context, i int) {
			cs := fr.Status.Clusters[i]
			if cs.Revision == "" {
				return
			}
			var mc fleetv1.MemberCluster
			if err := r.Get(ctx, types.NamespacedName{Name: cs.Name}, &mc); err != nil {
				return // cluster deregistered: nothing we can reach
			}
			if mc.Spec.Cordoned {
				return // documented escape hatch for clusters that are gone for good
			}
			objs, err := store.load(ctx, cs.Revision)
			if err == nil {
				var rem *remote.Remote
				if rem, err = r.Remotes.Get(ctx, &mc); err == nil {
					err = deleteRefs(ctx, rem.Client, owner, revision.Inventory(objs))
				}
			}
			if err != nil {
				mu.Lock()
				errs = append(errs, fmt.Sprintf("%s: %v", cs.Name, err))
				mu.Unlock()
			}
		})
		if len(errs) > 0 {
			sort.Strings(errs)
			msg := "cleanup blocked (cordon a cluster to skip it): " + strings.Join(errs, "; ")
			r.Recorder.Event(fr, corev1.EventTypeWarning, "CleanupFailed", msg)
			return ctrl.Result{RequeueAfter: r.poll()}, nil
		}
	}
	controllerutil.RemoveFinalizer(fr, finalizer)
	if err := r.Update(ctx, fr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	forgetMetrics(fr.Namespace, fr.Name)
	return ctrl.Result{}, nil
}

func withDefaults(s fleetv1.RolloutStrategy) fleetv1.RolloutStrategy {
	out := *s.DeepCopy()
	if len(out.Waves) == 0 {
		out.Waves = planner.DefaultWaves()
	}
	if out.ProgressDeadline.Duration == 0 {
		out.ProgressDeadline = metav1.Duration{Duration: 5 * time.Minute}
	}
	if out.AutoRollback == nil {
		t := true
		out.AutoRollback = &t
	}
	return out
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func setConditions(fr *fleetv1.FleetRollout) {
	gen := fr.Generation
	phase := fr.Status.Phase
	failed := 0
	for _, c := range fr.Status.Clusters {
		if c.State == fleetv1.ClusterFailed {
			failed++
		}
	}
	set := func(t string, ok bool, reason, msg string) {
		st := metav1.ConditionFalse
		if ok {
			st = metav1.ConditionTrue
		}
		meta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{Type: t, Status: st, Reason: reason, Message: msg, ObservedGeneration: gen})
	}
	reason := string(phase)
	set("Ready", phase == fleetv1.PhaseSucceeded, reason, fr.Status.Message)
	set("Progressing", phase == fleetv1.PhaseProgressing || phase == fleetv1.PhaseBaking, reason, fr.Status.Message)
	degradedMsg := fr.Status.Message
	if failed > 0 && !phase.Terminal() {
		degradedMsg = fmt.Sprintf("%d cluster(s) failed", failed)
	}
	set("Degraded", phase.Terminal() || failed > 0, reason, degradedMsg)
}

func recordMetrics(fr *fleetv1.FleetRollout) {
	counts := map[fleetv1.ClusterState]int{}
	for _, c := range fr.Status.Clusters {
		counts[c.State]++
	}
	for _, s := range []fleetv1.ClusterState{fleetv1.ClusterPending, fleetv1.ClusterProgressing, fleetv1.ClusterHealthy, fleetv1.ClusterFailed, fleetv1.ClusterRolledBack, fleetv1.ClusterSkipped} {
		rolloutClusters.WithLabelValues(fr.Namespace, fr.Name, string(s)).Set(float64(counts[s]))
	}
	for _, p := range []fleetv1.RolloutPhase{fleetv1.PhaseProgressing, fleetv1.PhaseBaking, fleetv1.PhasePaused, fleetv1.PhaseSucceeded, fleetv1.PhaseHalted, fleetv1.PhaseRolledBack, fleetv1.PhaseInvalid} {
		v := 0.0
		if fr.Status.Phase == p {
			v = 1
		}
		rolloutPhase.WithLabelValues(fr.Namespace, fr.Name, string(p)).Set(v)
	}
	rolloutWave.WithLabelValues(fr.Namespace, fr.Name).Set(float64(fr.Status.CurrentWave + 1))
	rolloutTotalWaves.WithLabelValues(fr.Namespace, fr.Name).Set(float64(fr.Status.TotalWaves))
}

func forgetMetrics(ns, name string) {
	l := map[string]string{"namespace": ns, "rollout": name}
	rolloutClusters.DeletePartialMatch(l)
	rolloutPhase.DeletePartialMatch(l)
	rolloutWave.DeletePartialMatch(l)
	rolloutTotalWaves.DeletePartialMatch(l)
	rollbacksTotal.DeletePartialMatch(l)
}

// SetupWithManager wires watches: spec changes on rollouts, and membership
// or readiness changes on clusters, which re-plan every rollout selecting them.
func (r *FleetRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1.FleetRollout{}, builder.WithPredicates(predicate.Or(
			predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{}))).
		Watches(&fleetv1.MemberCluster{}, handler.EnqueueRequestsFromMapFunc(r.rolloutsForCluster),
			builder.WithPredicates(clusterChanged())).
		Named("fleetrollout").
		Complete(r)
}

func clusterChanged() predicate.Funcs {
	return predicate.Funcs{
		UpdateFunc: func(e event.UpdateEvent) bool {
			o, ok1 := e.ObjectOld.(*fleetv1.MemberCluster)
			n, ok2 := e.ObjectNew.(*fleetv1.MemberCluster)
			if !ok1 || !ok2 {
				return true
			}
			return o.Generation != n.Generation ||
				!labels.Equals(o.Labels, n.Labels) ||
				meta.IsStatusConditionTrue(o.Status.Conditions, fleetv1.ConditionReady) !=
					meta.IsStatusConditionTrue(n.Status.Conditions, fleetv1.ConditionReady)
		},
	}
}

func (r *FleetRolloutReconciler) rolloutsForCluster(ctx context.Context, obj client.Object) []reconcile.Request {
	var list fleetv1.FleetRolloutList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, fr := range list.Items {
		sel, err := metav1.LabelSelectorAsSelector(&fr.Spec.ClusterSelector)
		if err != nil {
			continue
		}
		_, wasMember := statusHas(&fr, obj.GetName())
		if sel.Matches(labels.Set(obj.GetLabels())) || wasMember {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: fr.Namespace, Name: fr.Name}})
		}
	}
	return reqs
}

func statusHas(fr *fleetv1.FleetRollout, name string) (fleetv1.ClusterRolloutStatus, bool) {
	for _, c := range fr.Status.Clusters {
		if c.Name == name {
			return c, true
		}
	}
	return fleetv1.ClusterRolloutStatus{}, false
}

// revisionStore persists revision snapshots as ControllerRevisions.
type revisionStore struct {
	c     client.Client
	fr    *fleetv1.FleetRollout
	mu    sync.Mutex
	cache map[string][]*unstructured.Unstructured
}

func revisionName(rollout, rev string) string {
	max := 253 - len(rev) - 1
	if len(rollout) > max {
		rollout = rollout[:max]
	}
	return rollout + "-" + rev
}

func (s *revisionStore) ensure(ctx context.Context, scheme *runtime.Scheme, rev string, objs []*unstructured.Unstructured) error {
	name := revisionName(s.fr.Name, rev)
	var existing appsv1.ControllerRevision
	err := s.c.Get(ctx, types.NamespacedName{Namespace: s.fr.Namespace, Name: name}, &existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	data, err := revision.Encode(objs)
	if err != nil {
		return err
	}
	var list appsv1.ControllerRevisionList
	if err := s.c.List(ctx, &list, client.InNamespace(s.fr.Namespace), client.MatchingLabels{LabelRolloutName: s.fr.Name}); err != nil {
		return err
	}
	var next int64 = 1
	for _, cr := range list.Items {
		if cr.Revision >= next {
			next = cr.Revision + 1
		}
	}
	cr := &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   s.fr.Namespace,
			Labels:      map[string]string{LabelRolloutName: s.fr.Name},
			Annotations: map[string]string{AnnotationRevision: rev},
		},
		Data:     runtime.RawExtension{Raw: data},
		Revision: next,
	}
	if err := controllerutil.SetControllerReference(s.fr, cr, scheme); err != nil {
		return err
	}
	if err := s.c.Create(ctx, cr); err != nil && !apierrors.IsAlreadyExists(err) {
		return err
	}
	return nil
}

func (s *revisionStore) load(ctx context.Context, rev string) ([]*unstructured.Unstructured, error) {
	s.mu.Lock()
	if objs, ok := s.cache[rev]; ok {
		s.mu.Unlock()
		return objs, nil
	}
	s.mu.Unlock()
	var cr appsv1.ControllerRevision
	if err := s.c.Get(ctx, types.NamespacedName{Namespace: s.fr.Namespace, Name: revisionName(s.fr.Name, rev)}, &cr); err != nil {
		return nil, fmt.Errorf("revision %s: %w", rev, err)
	}
	objs, err := revision.Decode(cr.Data.Raw)
	if err != nil {
		return nil, fmt.Errorf("revision %s: %w", rev, err)
	}
	s.mu.Lock()
	s.cache[rev] = objs
	s.mu.Unlock()
	return objs, nil
}

// gc keeps the newest revisionHistory snapshots plus every revision still
// referenced by the rollout or one of its clusters.
func (s *revisionStore) gc(ctx context.Context, fr *fleetv1.FleetRollout) error {
	var list appsv1.ControllerRevisionList
	if err := s.c.List(ctx, &list, client.InNamespace(fr.Namespace), client.MatchingLabels{LabelRolloutName: fr.Name}); err != nil {
		return err
	}
	if len(list.Items) <= revisionHistory {
		return nil
	}
	keep := map[string]bool{fr.Status.Revision: true, fr.Status.StableRevision: true}
	for _, c := range fr.Status.Clusters {
		keep[c.Revision] = true
	}
	sort.Slice(list.Items, func(i, j int) bool { return list.Items[i].Revision > list.Items[j].Revision })
	for i, cr := range list.Items {
		if i < revisionHistory || keep[cr.Annotations[AnnotationRevision]] {
			continue
		}
		if err := s.c.Delete(ctx, &list.Items[i]); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
