package controller

import (
	"context"
	"errors"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/AnirudhBabu/fleetwave/internal/health"
	"github.com/AnirudhBabu/fleetwave/internal/revision"
)

const (
	// FieldOwner is the server-side apply field manager in member clusters.
	FieldOwner = "fleetwave"
	// LabelManagedBy marks every object fleetwave applies.
	LabelManagedBy = "app.kubernetes.io/managed-by"
	// AnnotationOwner records which FleetRollout (<namespace>/<name>) owns an object.
	AnnotationOwner = "fleet.fleetwave.io/owner"
	// AnnotationRevision records the revision an object was applied from.
	AnnotationRevision = "fleet.fleetwave.io/revision"
)

// errOwnedElsewhere means another rollout (or a human) owns the object.
// Taking it over silently is how two teams overwrite each other.
var errOwnedElsewhere = errors.New("object is owned by another FleetRollout")

// permanent reports whether retrying the same apply cannot succeed, so the
// cluster should fail now instead of sitting in Pending until someone looks.
func permanent(err error) bool {
	return errors.Is(err, errOwnedElsewhere) ||
		apierrors.IsInvalid(err) || apierrors.IsBadRequest(err) ||
		apierrors.IsForbidden(err) || meta.IsNoMatchError(err)
}

// applyRevision server-side applies objs to a member and prunes stale refs
// left over from the revision the cluster was on before.
func applyRevision(ctx context.Context, c client.Client, owner, rev string, objs []*unstructured.Unstructured, stale []revision.Ref) error {
	start := time.Now()
	err := func() error {
		for _, o := range objs {
			if err := checkOwner(ctx, c, o, owner); err != nil {
				return err
			}
			u := o.DeepCopy()
			labels := u.GetLabels()
			if labels == nil {
				labels = map[string]string{}
			}
			labels[LabelManagedBy] = FieldOwner
			u.SetLabels(labels)
			ann := u.GetAnnotations()
			if ann == nil {
				ann = map[string]string{}
			}
			ann[AnnotationOwner] = owner
			ann[AnnotationRevision] = rev
			u.SetAnnotations(ann)
			if err := c.Patch(ctx, u, client.Apply, client.FieldOwner(FieldOwner), client.ForceOwnership); err != nil {
				return fmt.Errorf("apply %s: %w", revision.RefOf(o), err)
			}
		}
		return deleteRefs(ctx, c, owner, stale)
	}()
	result := "success"
	if err != nil {
		result = "error"
	}
	applyDuration.WithLabelValues(result).Observe(time.Since(start).Seconds())
	return err
}

func checkOwner(ctx context.Context, c client.Client, o *unstructured.Unstructured, owner string) error {
	live := &unstructured.Unstructured{}
	live.SetGroupVersionKind(o.GroupVersionKind())
	err := c.Get(ctx, client.ObjectKeyFromObject(o), live)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("get %s: %w", revision.RefOf(o), err)
	}
	if got := live.GetAnnotations()[AnnotationOwner]; got != "" && got != owner {
		return fmt.Errorf("%s: %w (%s)", revision.RefOf(o), errOwnedElsewhere, got)
	}
	return nil
}

// deleteRefs deletes objects this rollout owns. Objects someone else owns,
// or that are already gone, are left alone.
func deleteRefs(ctx context.Context, c client.Client, owner string, refs []revision.Ref) error {
	for _, ref := range refs {
		mapping, err := c.RESTMapper().RESTMapping(schema.GroupKind{Group: ref.Group, Kind: ref.Kind})
		if meta.IsNoMatchError(err) {
			continue // the kind itself is gone (e.g. CRD removed); nothing to delete
		}
		if err != nil {
			return fmt.Errorf("prune %s: %w", ref, err)
		}
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(mapping.GroupVersionKind)
		if err := c.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, live); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("prune %s: %w", ref, err)
		}
		if live.GetAnnotations()[AnnotationOwner] != owner {
			continue
		}
		uid := live.GetUID()
		err = c.Delete(ctx, live, client.Preconditions{UID: &uid}, client.PropagationPolicy("Background"))
		if err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return fmt.Errorf("prune %s: %w", ref, err)
		}
	}
	return nil
}

// assess reads every object of a revision back from the member and folds
// their health into one verdict.
func assess(ctx context.Context, c client.Client, objs []*unstructured.Unstructured) health.Result {
	results := make([]health.Result, 0, len(objs))
	for _, o := range objs {
		live := &unstructured.Unstructured{}
		live.SetGroupVersionKind(o.GroupVersionKind())
		err := c.Get(ctx, client.ObjectKeyFromObject(o), live)
		switch {
		case apierrors.IsNotFound(err):
			results = append(results, health.Result{Status: health.Progressing, Message: revision.RefOf(o).String() + " not found"})
		case err != nil:
			return health.Result{Status: health.Unknown, Message: fmt.Sprintf("reading %s: %v", revision.RefOf(o), err)}
		default:
			results = append(results, health.Assess(live))
		}
	}
	return health.Combine(results...)
}
