// Package health decides whether an object applied to a member cluster has
// converged. It works on unstructured objects so it does not need typed
// clients for every kind the fleet ships.
package health

import (
	"fmt"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

// Status is the verdict for one object or one cluster.
type Status int

const (
	// Unknown means health could not be determined (e.g. cluster unreachable).
	Unknown Status = iota
	// Progressing means the object has not converged yet.
	Progressing
	// Healthy means the object has converged.
	Healthy
	// Degraded means the object has failed and will not converge by waiting.
	Degraded
)

func (s Status) String() string {
	switch s {
	case Progressing:
		return "Progressing"
	case Healthy:
		return "Healthy"
	case Degraded:
		return "Degraded"
	default:
		return "Unknown"
	}
}

// Result is a verdict plus a reason a human can act on.
type Result struct {
	Status  Status
	Message string
}

// Assess returns the health of a single live object.
func Assess(obj *unstructured.Unstructured) Result {
	gk := obj.GroupVersionKind().GroupKind()
	switch {
	case gk.Group == "apps" && gk.Kind == "Deployment":
		return deployment(obj)
	case gk.Group == "apps" && gk.Kind == "StatefulSet":
		return statefulSet(obj)
	case gk.Group == "apps" && gk.Kind == "DaemonSet":
		return daemonSet(obj)
	case gk.Group == "batch" && gk.Kind == "Job":
		return job(obj)
	default:
		return generic(obj)
	}
}

// Combine folds per-object results into one cluster verdict: any Degraded
// wins, then Unknown, then Progressing; Healthy only if everything is.
func Combine(results ...Result) Result {
	worst := Result{Status: Healthy}
	rank := map[Status]int{Healthy: 0, Progressing: 1, Unknown: 2, Degraded: 3}
	for _, r := range results {
		if rank[r.Status] > rank[worst.Status] {
			worst = r
		}
	}
	return worst
}

func observedCurrent(obj *unstructured.Unstructured) (bool, string) {
	gen := obj.GetGeneration()
	observed, found, _ := unstructured.NestedInt64(obj.Object, "status", "observedGeneration")
	if !found || observed < gen {
		return false, fmt.Sprintf("%s/%s: controller has not observed generation %d yet", obj.GetKind(), obj.GetName(), gen)
	}
	return true, ""
}

func intField(obj *unstructured.Unstructured, fields ...string) int64 {
	v, _, _ := unstructured.NestedInt64(obj.Object, fields...)
	return v
}

func condition(obj *unstructured.Unstructured, condType string) (status, reason, message string, found bool) {
	conds, _, _ := unstructured.NestedSlice(obj.Object, "status", "conditions")
	for _, c := range conds {
		m, ok := c.(map[string]interface{})
		if !ok || m["type"] != condType {
			continue
		}
		s, _ := m["status"].(string)
		r, _ := m["reason"].(string)
		msg, _ := m["message"].(string)
		return s, r, msg, true
	}
	return "", "", "", false
}

func deployment(obj *unstructured.Unstructured) Result {
	name := "Deployment/" + obj.GetName()
	if s, r, msg, ok := condition(obj, "Progressing"); ok && s == "False" && r == "ProgressDeadlineExceeded" {
		return Result{Degraded, fmt.Sprintf("%s: %s", name, msg)}
	}
	if ok, msg := observedCurrent(obj); !ok {
		return Result{Progressing, msg}
	}
	want, found, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
	if !found {
		want = 1
	}
	updated := intField(obj, "status", "updatedReplicas")
	available := intField(obj, "status", "availableReplicas")
	total := intField(obj, "status", "replicas")
	switch {
	case updated < want:
		return Result{Progressing, fmt.Sprintf("%s: %d of %d replicas updated", name, updated, want)}
	case total > updated:
		return Result{Progressing, fmt.Sprintf("%s: %d old replicas pending termination", name, total-updated)}
	case available < want:
		return Result{Progressing, fmt.Sprintf("%s: %d of %d updated replicas available", name, available, want)}
	}
	return Result{Healthy, ""}
}

func statefulSet(obj *unstructured.Unstructured) Result {
	name := "StatefulSet/" + obj.GetName()
	if ok, msg := observedCurrent(obj); !ok {
		return Result{Progressing, msg}
	}
	want, found, _ := unstructured.NestedInt64(obj.Object, "spec", "replicas")
	if !found {
		want = 1
	}
	ready := intField(obj, "status", "readyReplicas")
	updated := intField(obj, "status", "updatedReplicas")
	cur, _, _ := unstructured.NestedString(obj.Object, "status", "currentRevision")
	upd, _, _ := unstructured.NestedString(obj.Object, "status", "updateRevision")
	switch {
	case updated < want || (upd != "" && cur != upd):
		return Result{Progressing, fmt.Sprintf("%s: %d of %d replicas updated", name, updated, want)}
	case ready < want:
		return Result{Progressing, fmt.Sprintf("%s: %d of %d replicas ready", name, ready, want)}
	}
	return Result{Healthy, ""}
}

func daemonSet(obj *unstructured.Unstructured) Result {
	name := "DaemonSet/" + obj.GetName()
	if ok, msg := observedCurrent(obj); !ok {
		return Result{Progressing, msg}
	}
	desired := intField(obj, "status", "desiredNumberScheduled")
	updated := intField(obj, "status", "updatedNumberScheduled")
	available := intField(obj, "status", "numberAvailable")
	switch {
	case updated < desired:
		return Result{Progressing, fmt.Sprintf("%s: %d of %d nodes updated", name, updated, desired)}
	case available < desired:
		return Result{Progressing, fmt.Sprintf("%s: %d of %d nodes available", name, available, desired)}
	}
	return Result{Healthy, ""}
}

func job(obj *unstructured.Unstructured) Result {
	name := "Job/" + obj.GetName()
	if s, _, msg, ok := condition(obj, "Failed"); ok && s == "True" {
		return Result{Degraded, fmt.Sprintf("%s failed: %s", name, msg)}
	}
	if s, _, _, ok := condition(obj, "Complete"); ok && s == "True" {
		return Result{Healthy, ""}
	}
	return Result{Progressing, name + ": not complete"}
}

// generic treats objects with a Ready condition by that condition, and
// everything else (ConfigMaps, Services, RBAC...) as healthy once it exists.
func generic(obj *unstructured.Unstructured) Result {
	s, reason, msg, ok := condition(obj, "Ready")
	if !ok {
		return Result{Healthy, ""}
	}
	if s == "True" {
		return Result{Healthy, ""}
	}
	return Result{Progressing, fmt.Sprintf("%s/%s not Ready (%s): %s", obj.GetKind(), obj.GetName(), reason, msg)}
}
