package revision

import (
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
)

func raw(ss ...string) []runtime.RawExtension {
	out := make([]runtime.RawExtension, len(ss))
	for i, s := range ss {
		out[i] = runtime.RawExtension{Raw: []byte(s)}
	}
	return out
}

const (
	ns  = `{"apiVersion":"v1","kind":"Namespace","metadata":{"name":"payments"}}`
	cm  = `{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"cfg","namespace":"payments"},"data":{"a":"1"}}`
	dep = `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"payments"},"spec":{"replicas":2}}`
)

func TestParseOrdersNamespacesFirstAndStripsServerFields(t *testing.T) {
	withStatus := `{"apiVersion":"apps/v1","kind":"Deployment","metadata":{"name":"api","namespace":"payments","resourceVersion":"9","uid":"u"},"spec":{"replicas":2},"status":{"replicas":2}}`
	objs, err := Parse(raw(withStatus, cm, ns))
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, o := range objs {
		kinds = append(kinds, o.GetKind())
	}
	if !reflect.DeepEqual(kinds, []string{"Namespace", "ConfigMap", "Deployment"}) {
		t.Fatalf("order = %v", kinds)
	}
	d := objs[2]
	if _, ok := d.Object["status"]; ok || d.GetResourceVersion() != "" || d.GetUID() != "" {
		t.Fatalf("server fields not stripped: %v", d.Object)
	}
}

func TestParseRejects(t *testing.T) {
	cases := map[string]string{
		`{"kind":"ConfigMap","metadata":{"name":"x"}}`:                                       "apiVersion and kind",
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{}}`:                               "metadata.name",
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"x","generateName":"x-"}}`: "generateName",
		`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"Bad_Name"}}`:              "invalid name",
		`not json`: "resources[0]",
	}
	for in, want := range cases {
		if _, err := Parse(raw(in)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("Parse(%s) err = %v, want %q", in, err, want)
		}
	}
	if _, err := Parse(raw(cm, cm)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("duplicate not rejected: %v", err)
	}
	if _, err := Parse(nil); err == nil {
		t.Error("empty resources accepted")
	}
	// RBAC names with colons are legal and common.
	if _, err := Parse(raw(`{"apiVersion":"rbac.authorization.k8s.io/v1","kind":"ClusterRole","metadata":{"name":"system:fleet-reader"}}`)); err != nil {
		t.Errorf("RBAC name rejected: %v", err)
	}
}

func TestHashIgnoresKeyOrderAndServerFields(t *testing.T) {
	a, _ := Parse(raw(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c","namespace":"n"},"data":{"x":"1","y":"2"}}`))
	b, _ := Parse(raw(`{"metadata":{"namespace":"n","name":"c","resourceVersion":"5"},"data":{"y":"2","x":"1"},"kind":"ConfigMap","apiVersion":"v1"}`))
	c, _ := Parse(raw(`{"apiVersion":"v1","kind":"ConfigMap","metadata":{"name":"c","namespace":"n"},"data":{"x":"1","y":"3"}}`))
	ha, _ := Hash(a)
	hb, _ := Hash(b)
	hc, _ := Hash(c)
	if ha != hb {
		t.Fatalf("equivalent manifests hash differently: %s %s", ha, hb)
	}
	if ha == hc {
		t.Fatal("different manifests hash the same")
	}
	if len(ha) != 10 {
		t.Fatalf("hash length %d", len(ha))
	}
}

func TestStale(t *testing.T) {
	old, _ := Parse(raw(ns, cm, dep))
	cur, _ := Parse(raw(ns, dep))
	got := Stale(Inventory(old), Inventory(cur))
	want := []Ref{{Group: "", Kind: "ConfigMap", Namespace: "payments", Name: "cfg"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stale = %v", got)
	}
	if got[0].String() != "ConfigMap/payments/cfg" {
		t.Fatalf("String() = %s", got[0])
	}
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	objs, _ := Parse(raw(ns, cm, dep))
	b, err := Encode(objs)
	if err != nil {
		t.Fatal(err)
	}
	back, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	h1, _ := Hash(objs)
	h2, _ := Hash(back)
	if h1 != h2 {
		t.Fatalf("round trip changed content: %s vs %s", h1, h2)
	}
}
