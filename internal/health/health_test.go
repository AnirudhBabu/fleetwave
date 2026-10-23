package health

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
)

func obj(apiVersion, kind string, generation int64, spec, status map[string]interface{}) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata":   map[string]interface{}{"name": "x", "generation": generation},
	}}
	if spec != nil {
		u.Object["spec"] = spec
	}
	if status != nil {
		u.Object["status"] = status
	}
	return u
}

func cond(t, s, reason, msg string) map[string]interface{} {
	return map[string]interface{}{"type": t, "status": s, "reason": reason, "message": msg}
}

func TestAssess(t *testing.T) {
	tests := []struct {
		name    string
		obj     *unstructured.Unstructured
		want    Status
		wantMsg string
	}{
		{"deployment healthy", obj("apps/v1", "Deployment", 2, map[string]interface{}{"replicas": int64(3)},
			map[string]interface{}{"observedGeneration": int64(2), "replicas": int64(3), "updatedReplicas": int64(3), "availableReplicas": int64(3)}), Healthy, ""},
		{"deployment default replicas", obj("apps/v1", "Deployment", 1, map[string]interface{}{},
			map[string]interface{}{"observedGeneration": int64(1), "replicas": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(1)}), Healthy, ""},
		{"deployment generation not observed", obj("apps/v1", "Deployment", 3, map[string]interface{}{"replicas": int64(1)},
			map[string]interface{}{"observedGeneration": int64(2), "replicas": int64(1), "updatedReplicas": int64(1), "availableReplicas": int64(1)}), Progressing, "not observed generation 3"},
		{"deployment no status yet", obj("apps/v1", "Deployment", 1, map[string]interface{}{"replicas": int64(1)}, nil), Progressing, "not observed"},
		{"deployment updating", obj("apps/v1", "Deployment", 1, map[string]interface{}{"replicas": int64(3)},
			map[string]interface{}{"observedGeneration": int64(1), "replicas": int64(4), "updatedReplicas": int64(2), "availableReplicas": int64(3)}), Progressing, "2 of 3 replicas updated"},
		{"deployment old replicas terminating", obj("apps/v1", "Deployment", 1, map[string]interface{}{"replicas": int64(3)},
			map[string]interface{}{"observedGeneration": int64(1), "replicas": int64(4), "updatedReplicas": int64(3), "availableReplicas": int64(3)}), Progressing, "pending termination"},
		{"deployment crashlooping", obj("apps/v1", "Deployment", 1, map[string]interface{}{"replicas": int64(2)},
			map[string]interface{}{"observedGeneration": int64(1), "replicas": int64(2), "updatedReplicas": int64(2), "availableReplicas": int64(0)}), Progressing, "0 of 2 updated replicas available"},
		{"deployment deadline exceeded", obj("apps/v1", "Deployment", 1, map[string]interface{}{"replicas": int64(2)},
			map[string]interface{}{"observedGeneration": int64(1), "conditions": []interface{}{cond("Progressing", "False", "ProgressDeadlineExceeded", "ReplicaSet \"x-1\" has timed out progressing.")}}), Degraded, "timed out"},
		{"statefulset rolling", obj("apps/v1", "StatefulSet", 1, map[string]interface{}{"replicas": int64(3)},
			map[string]interface{}{"observedGeneration": int64(1), "readyReplicas": int64(3), "updatedReplicas": int64(3), "currentRevision": "a", "updateRevision": "b"}), Progressing, "replicas updated"},
		{"statefulset healthy", obj("apps/v1", "StatefulSet", 1, map[string]interface{}{"replicas": int64(3)},
			map[string]interface{}{"observedGeneration": int64(1), "readyReplicas": int64(3), "updatedReplicas": int64(3), "currentRevision": "b", "updateRevision": "b"}), Healthy, ""},
		{"daemonset partial", obj("apps/v1", "DaemonSet", 1, nil,
			map[string]interface{}{"observedGeneration": int64(1), "desiredNumberScheduled": int64(5), "updatedNumberScheduled": int64(5), "numberAvailable": int64(4)}), Progressing, "4 of 5 nodes available"},
		{"daemonset healthy", obj("apps/v1", "DaemonSet", 1, nil,
			map[string]interface{}{"observedGeneration": int64(1), "desiredNumberScheduled": int64(5), "updatedNumberScheduled": int64(5), "numberAvailable": int64(5)}), Healthy, ""},
		{"job complete", obj("batch/v1", "Job", 1, nil, map[string]interface{}{"conditions": []interface{}{cond("Complete", "True", "", "")}}), Healthy, ""},
		{"job failed", obj("batch/v1", "Job", 1, nil, map[string]interface{}{"conditions": []interface{}{cond("Failed", "True", "BackoffLimitExceeded", "too many retries")}}), Degraded, "too many retries"},
		{"job running", obj("batch/v1", "Job", 1, nil, nil), Progressing, "not complete"},
		{"configmap exists", obj("v1", "ConfigMap", 0, nil, nil), Healthy, ""},
		{"custom resource not ready", obj("cert-manager.io/v1", "Certificate", 1, nil,
			map[string]interface{}{"conditions": []interface{}{cond("Ready", "False", "Issuing", "waiting for ACME")}}), Progressing, "waiting for ACME"},
		{"custom resource ready", obj("cert-manager.io/v1", "Certificate", 1, nil,
			map[string]interface{}{"conditions": []interface{}{cond("Ready", "True", "", "")}}), Healthy, ""},
		{"apps group required for workload rules", obj("example.com/v1", "Deployment", 5, nil, nil), Healthy, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Assess(tt.obj)
			if got.Status != tt.want {
				t.Fatalf("status = %s (%q), want %s", got.Status, got.Message, tt.want)
			}
			if !strings.Contains(got.Message, tt.wantMsg) {
				t.Fatalf("message %q does not contain %q", got.Message, tt.wantMsg)
			}
		})
	}
}

func TestCombine(t *testing.T) {
	h := Result{Status: Healthy}
	p := Result{Status: Progressing, Message: "p"}
	u := Result{Status: Unknown, Message: "u"}
	d := Result{Status: Degraded, Message: "d"}
	cases := []struct {
		in   []Result
		want Status
	}{
		{nil, Healthy},
		{[]Result{h, h}, Healthy},
		{[]Result{h, p}, Progressing},
		{[]Result{p, u}, Unknown},
		{[]Result{u, d, p}, Degraded},
	}
	for _, c := range cases {
		if got := Combine(c.in...); got.Status != c.want {
			t.Errorf("Combine(%v) = %s, want %s", c.in, got.Status, c.want)
		}
	}
}
