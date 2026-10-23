// Package revision turns spec.resources into validated objects, a content
// hash, and an inventory used for pruning. Snapshots are stored in
// apps/v1 ControllerRevisions, the same mechanism StatefulSets and DaemonSets
// use for rollback, so old revisions survive spec edits.
package revision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Ref identifies an object in a member cluster.
type Ref struct {
	Group     string `json:"group"`
	Kind      string `json:"kind"`
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name"`
}

func (r Ref) String() string {
	gk := schema.GroupKind{Group: r.Group, Kind: r.Kind}.String()
	if r.Namespace == "" {
		return gk + "/" + r.Name
	}
	return gk + "/" + r.Namespace + "/" + r.Name
}

// RefOf returns the Ref of an object.
func RefOf(u *unstructured.Unstructured) Ref {
	gvk := u.GroupVersionKind()
	return Ref{Group: gvk.Group, Kind: gvk.Kind, Namespace: u.GetNamespace(), Name: u.GetName()}
}

// Parse decodes and validates raw manifests. Objects are returned in apply
// order: namespaces and CRDs first, so things that live inside them can be
// created in the same pass.
func Parse(raw []runtime.RawExtension) ([]*unstructured.Unstructured, error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("spec.resources is empty")
	}
	seen := map[Ref]bool{}
	objs := make([]*unstructured.Unstructured, 0, len(raw))
	for i, r := range raw {
		u := &unstructured.Unstructured{}
		if err := u.UnmarshalJSON(r.Raw); err != nil {
			return nil, fmt.Errorf("resources[%d]: %w", i, err)
		}
		if u.GetAPIVersion() == "" || u.GetKind() == "" {
			return nil, fmt.Errorf("resources[%d]: apiVersion and kind are required", i)
		}
		if u.GetName() == "" {
			return nil, fmt.Errorf("resources[%d] (%s): metadata.name is required", i, u.GetKind())
		}
		if u.GetGenerateName() != "" {
			return nil, fmt.Errorf("resources[%d] (%s/%s): generateName is not supported, apply needs a fixed name", i, u.GetKind(), u.GetName())
		}
		if errs := validation.IsDNS1123Subdomain(u.GetName()); len(errs) > 0 && !strings.HasPrefix(u.GetAPIVersion(), "rbac.authorization.k8s.io/") {
			return nil, fmt.Errorf("resources[%d] (%s): invalid name %q: %s", i, u.GetKind(), u.GetName(), strings.Join(errs, "; "))
		}
		// Server-populated fields would make apply fight the member API server.
		unstructured.RemoveNestedField(u.Object, "status")
		unstructured.RemoveNestedField(u.Object, "metadata", "resourceVersion")
		unstructured.RemoveNestedField(u.Object, "metadata", "uid")
		unstructured.RemoveNestedField(u.Object, "metadata", "creationTimestamp")
		unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
		ref := RefOf(u)
		if seen[ref] {
			return nil, fmt.Errorf("resources[%d]: duplicate object %s", i, ref)
		}
		seen[ref] = true
		objs = append(objs, u)
	}
	sort.SliceStable(objs, func(i, j int) bool { return applyRank(objs[i]) < applyRank(objs[j]) })
	return objs, nil
}

func applyRank(u *unstructured.Unstructured) int {
	switch u.GetKind() {
	case "Namespace":
		return 0
	case "CustomResourceDefinition":
		return 1
	case "ServiceAccount", "ClusterRole", "Role":
		return 2
	case "ClusterRoleBinding", "RoleBinding", "ConfigMap", "Secret":
		return 3
	default:
		return 4
	}
}

// Hash is a short, stable content hash of the manifests. Two specs that
// differ only in key order hash the same because each object is re-encoded
// canonically (encoding/json sorts map keys).
func Hash(objs []*unstructured.Unstructured) (string, error) {
	h := sha256.New()
	for _, u := range objs {
		b, err := json.Marshal(u.Object)
		if err != nil {
			return "", err
		}
		h.Write(b)
		h.Write([]byte{'\n'})
	}
	return hex.EncodeToString(h.Sum(nil))[:10], nil
}

// Inventory lists the refs of a revision.
func Inventory(objs []*unstructured.Unstructured) []Ref {
	refs := make([]Ref, 0, len(objs))
	for _, u := range objs {
		refs = append(refs, RefOf(u))
	}
	return refs
}

// Stale returns refs in old that are not in current: what to prune when a
// cluster moves from old to current.
func Stale(old, current []Ref) []Ref {
	keep := map[Ref]bool{}
	for _, r := range current {
		keep[r] = true
	}
	var out []Ref
	for _, r := range old {
		if !keep[r] {
			out = append(out, r)
		}
	}
	return out
}

// Snapshot is the payload stored in a ControllerRevision.
type Snapshot struct {
	Resources []runtime.RawExtension `json:"resources"`
}

// Encode serialises manifests for storage.
func Encode(objs []*unstructured.Unstructured) ([]byte, error) {
	s := Snapshot{}
	for _, u := range objs {
		b, err := json.Marshal(u.Object)
		if err != nil {
			return nil, err
		}
		s.Resources = append(s.Resources, runtime.RawExtension{Raw: b})
	}
	return json.Marshal(s)
}

// Decode reverses Encode.
func Decode(b []byte) ([]*unstructured.Unstructured, error) {
	s := Snapshot{}
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return Parse(s.Resources)
}
