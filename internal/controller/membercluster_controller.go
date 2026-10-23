package controller

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/remote"
)

// MemberClusterReconciler probes member clusters and publishes reachability.
type MemberClusterReconciler struct {
	client.Client
	Remotes       remote.Getter
	ProbeInterval time.Duration
	// ProbeTimeout bounds one discovery call so an unreachable cluster cannot
	// hold a worker.
	ProbeTimeout time.Duration
}

// +kubebuilder:rbac:groups=fleet.fleetwave.io,resources=memberclusters,verbs=get;list;watch
// +kubebuilder:rbac:groups=fleet.fleetwave.io,resources=memberclusters/status,verbs=get;update;patch
// +kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch

// Reconcile probes one MemberCluster.
func (r *MemberClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)
	var mc fleetv1.MemberCluster
	if err := r.Get(ctx, req.NamespacedName, &mc); err != nil {
		if apierrors.IsNotFound(err) {
			if f, ok := r.Remotes.(interface{ Forget(string) }); ok {
				f.Forget(req.Name)
			}
			memberReady.DeleteLabelValues(req.Name)
		}
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	before := mc.Status.DeepCopy()
	now := metav1.Now()
	mc.Status.LastProbeTime = &now

	cond := metav1.Condition{Type: fleetv1.ConditionReady, ObservedGeneration: mc.Generation}
	rem, err := r.Remotes.Get(ctx, &mc)
	if err == nil {
		version, verr := r.probe(ctx, rem)
		if verr != nil {
			err = verr
		} else {
			mc.Status.KubernetesVersion = version
		}
	}
	if err != nil {
		cond.Status = metav1.ConditionFalse
		cond.Reason = "Unreachable"
		cond.Message = err.Error()
		memberReady.WithLabelValues(mc.Name).Set(0)
		log.V(1).Info("member cluster unreachable", "cluster", mc.Name, "error", err.Error())
	} else {
		cond.Status = metav1.ConditionTrue
		cond.Reason = "Reachable"
		cond.Message = "API server responded to discovery"
		memberReady.WithLabelValues(mc.Name).Set(1)
	}
	meta.SetStatusCondition(&mc.Status.Conditions, cond)

	// Only write when something other than the probe time changed, or the
	// last write is getting old; 160 clusters x 30s of no-op writes is noise
	// the hub API server does not need.
	if statusChanged(before, &mc.Status) || before.LastProbeTime == nil || now.Sub(before.LastProbeTime.Time) > 5*r.ProbeInterval {
		if uerr := r.Status().Update(ctx, &mc); uerr != nil {
			return ctrl.Result{}, uerr
		}
	}
	return ctrl.Result{RequeueAfter: r.ProbeInterval}, nil
}

func (r *MemberClusterReconciler) probe(ctx context.Context, rem *remote.Remote) (string, error) {
	type res struct {
		v   string
		err error
	}
	ch := make(chan res, 1)
	go func() {
		info, err := rem.Discovery.ServerVersion()
		if err != nil {
			ch <- res{err: err}
			return
		}
		ch <- res{v: info.GitVersion}
	}()
	timeout := r.ProbeTimeout
	if timeout == 0 {
		timeout = 10 * time.Second
	}
	select {
	case out := <-ch:
		return out.v, out.err
	case <-time.After(timeout):
		return "", context.DeadlineExceeded
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func statusChanged(a, b *fleetv1.MemberClusterStatus) bool {
	if a.KubernetesVersion != b.KubernetesVersion {
		return true
	}
	ca := meta.FindStatusCondition(a.Conditions, fleetv1.ConditionReady)
	cb := meta.FindStatusCondition(b.Conditions, fleetv1.ConditionReady)
	if (ca == nil) != (cb == nil) {
		return true
	}
	return ca != nil && (ca.Status != cb.Status || ca.Reason != cb.Reason || ca.Message != cb.Message || ca.ObservedGeneration != cb.ObservedGeneration)
}

// SetupWithManager wires the controller. Secret changes re-probe the
// clusters that reference them so rotated credentials apply immediately.
func (r *MemberClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.ProbeInterval == 0 {
		r.ProbeInterval = 30 * time.Second
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1.MemberCluster{}).
		// Metadata-only: the hub caches Secret names, never their contents.
		WatchesMetadata(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.clustersForSecret)).
		Named("membercluster").
		Complete(r)
}

func (r *MemberClusterReconciler) clustersForSecret(ctx context.Context, obj client.Object) []reconcile.Request {
	var list fleetv1.MemberClusterList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for _, mc := range list.Items {
		ref := mc.Spec.KubeconfigSecretRef
		if ref.Namespace == obj.GetNamespace() && ref.Name == obj.GetName() {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: mc.Name}})
		}
	}
	return reqs
}
