package controller_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/controller"
	"github.com/AnirudhBabu/fleetwave/internal/testenv"
)

// ---- helpers ---------------------------------------------------------------

func eventually(t *testing.T, timeout time.Duration, what string, fn func() error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last error
	for time.Now().Before(deadline) {
		if last = fn(); last == nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %s waiting for %s: %v", timeout, what, last)
}

func mustRaw(t *testing.T, objs ...map[string]interface{}) []runtime.RawExtension {
	t.Helper()
	out := make([]runtime.RawExtension, len(objs))
	for i, o := range objs {
		b, err := json.Marshal(o)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = runtime.RawExtension{Raw: b}
	}
	return out
}

func namespace(name string) map[string]interface{} {
	return map[string]interface{}{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": name}}
}

func configMap(ns, name, value string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1", "kind": "ConfigMap",
		"metadata": map[string]interface{}{"name": name, "namespace": ns},
		"data":     map[string]interface{}{"value": value},
	}
}

func deployment(ns, name, image string, annotations map[string]string) map[string]interface{} {
	meta := map[string]interface{}{"name": name, "namespace": ns}
	if annotations != nil {
		meta["annotations"] = annotations
	}
	return map[string]interface{}{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": meta,
		"spec": map[string]interface{}{
			"replicas": 2,
			"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": name}},
			"template": map[string]interface{}{
				"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": name}},
				"spec": map[string]interface{}{"containers": []interface{}{
					map[string]interface{}{"name": "app", "image": image},
				}},
			},
		},
	}
}

func hubNamespace(t *testing.T) string {
	t.Helper()
	name := strings.ToLower(strings.ReplaceAll(t.Name(), "/", "-"))
	if len(name) > 50 {
		name = name[:50]
	}
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name}}
	if err := fleet.HubClient.Create(context.Background(), ns); err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatal(err)
	}
	return name
}

func newRollout(ns, name string, resources []runtime.RawExtension, waves ...string) *fleetv1.FleetRollout {
	fr := &fleetv1.FleetRollout{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: fleetv1.FleetRolloutSpec{
			ClusterSelector: metav1.LabelSelector{MatchLabels: map[string]string{"env": "prod"}},
			Resources:       resources,
			Strategy: fleetv1.RolloutStrategy{
				BakeTime:         metav1.Duration{Duration: time.Second},
				ProgressDeadline: metav1.Duration{Duration: 4 * time.Second},
			},
		},
	}
	for _, w := range waves {
		fr.Spec.Strategy.Waves = append(fr.Spec.Strategy.Waves, intstr.Parse(w))
	}
	return fr
}

func get(t *testing.T, fr *fleetv1.FleetRollout) *fleetv1.FleetRollout {
	t.Helper()
	out := &fleetv1.FleetRollout{}
	if err := fleet.HubClient.Get(context.Background(), client.ObjectKeyFromObject(fr), out); err != nil {
		t.Fatal(err)
	}
	return out
}

func waitPhase(t *testing.T, fr *fleetv1.FleetRollout, phase fleetv1.RolloutPhase, timeout time.Duration) *fleetv1.FleetRollout {
	t.Helper()
	var cur *fleetv1.FleetRollout
	eventually(t, timeout, "phase "+string(phase), func() error {
		cur = get(t, fr)
		if cur.Status.Phase != phase || cur.Status.ObservedGeneration != cur.Generation {
			return fmt.Errorf("phase=%s observed=%d gen=%d msg=%q clusters=%s", cur.Status.Phase,
				cur.Status.ObservedGeneration, cur.Generation, cur.Status.Message, clusterSummary(cur))
		}
		return nil
	})
	return cur
}

func clusterSummary(fr *fleetv1.FleetRollout) string {
	var parts []string
	for _, c := range fr.Status.Clusters {
		parts = append(parts, fmt.Sprintf("%s[w%d %s %s %q]", c.Name, c.Wave, c.State, c.Revision, c.Message))
	}
	return strings.Join(parts, " ")
}

func update(t *testing.T, fr *fleetv1.FleetRollout, mutate func(*fleetv1.FleetRollout)) {
	t.Helper()
	eventually(t, 5*time.Second, "update", func() error {
		cur := get(t, fr)
		mutate(cur)
		return fleet.HubClient.Update(context.Background(), cur)
	})
}

func memberDeployment(t *testing.T, member, ns, name string) (*appsv1.Deployment, error) {
	t.Helper()
	d := &appsv1.Deployment{}
	err := fleet.Member(member).Client.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, d)
	return d, err
}

func clusterStatus(fr *fleetv1.FleetRollout, name string) fleetv1.ClusterRolloutStatus {
	for _, c := range fr.Status.Clusters {
		if c.Name == name {
			return c
		}
	}
	return fleetv1.ClusterRolloutStatus{}
}

// ---- tests -----------------------------------------------------------------

func TestMemberClustersBecomeReady(t *testing.T) {
	for _, m := range fleet.Members {
		eventually(t, 15*time.Second, m.Name+" Ready", func() error {
			mc := &fleetv1.MemberCluster{}
			if err := fleet.HubClient.Get(context.Background(), types.NamespacedName{Name: m.Name}, mc); err != nil {
				return err
			}
			if !meta.IsStatusConditionTrue(mc.Status.Conditions, fleetv1.ConditionReady) {
				return fmt.Errorf("not ready: %+v", mc.Status.Conditions)
			}
			if !strings.HasPrefix(mc.Status.KubernetesVersion, "v1.") {
				return fmt.Errorf("version %q", mc.Status.KubernetesVersion)
			}
			return nil
		})
	}
}

func TestRolloutProgressesInBakedWaves(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-waves"
	fr := newRollout(ns, "shop", mustRaw(t,
		namespace(target),
		configMap(target, "settings", "v1"),
		deployment(target, "api", "registry.example/api:1.0.0", nil),
	), "1", "2", "100%")
	fr.Spec.Strategy.BakeTime = metav1.Duration{Duration: 2 * time.Second}
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}

	done := waitPhase(t, fr, fleetv1.PhaseSucceeded, 45*time.Second)

	if done.Status.StableRevision != done.Status.Revision || done.Status.TotalWaves != 3 {
		t.Fatalf("status = %+v", done.Status)
	}
	// Canary label wins the first wave, then name order.
	wantWaves := map[string]int32{"bkk-1": 0, "sgp-1": 1, "syd-1": 2}
	for name, w := range wantWaves {
		cs := clusterStatus(done, name)
		if cs.Wave != w || cs.State != fleetv1.ClusterHealthy || cs.Revision != done.Status.Revision {
			t.Fatalf("%s = %+v", name, cs)
		}
	}
	// Each wave must start no earlier than bake after the previous one was applied.
	a0 := clusterStatus(done, "bkk-1").AppliedAt.Time
	a1 := clusterStatus(done, "sgp-1").AppliedAt.Time
	a2 := clusterStatus(done, "syd-1").AppliedAt.Time
	if a1.Sub(a0) < 2*time.Second || a2.Sub(a1) < 2*time.Second {
		t.Fatalf("waves did not bake: %s, %s, %s", a0, a1, a2)
	}
	for _, m := range fleet.Members {
		d, err := memberDeployment(t, m.Name, target, "api")
		if err != nil {
			t.Fatalf("%s: %v", m.Name, err)
		}
		if d.Annotations[controller.AnnotationOwner] != ns+"/shop" || d.Annotations[controller.AnnotationRevision] != done.Status.Revision {
			t.Fatalf("%s annotations = %v", m.Name, d.Annotations)
		}
		if d.Labels[controller.LabelManagedBy] != "fleetwave" {
			t.Fatalf("%s labels = %v", m.Name, d.Labels)
		}
	}
	if !meta.IsStatusConditionTrue(done.Status.Conditions, "Ready") || meta.IsStatusConditionTrue(done.Status.Conditions, "Degraded") {
		t.Fatalf("conditions = %+v", done.Status.Conditions)
	}
	var revs appsv1.ControllerRevisionList
	if err := fleet.HubClient.List(context.Background(), &revs, client.InNamespace(ns)); err != nil || len(revs.Items) != 1 {
		t.Fatalf("controller revisions: %v %d", err, len(revs.Items))
	}
}

func TestBadRevisionRollsBackFleetWide(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-rollback"
	v1 := mustRaw(t, namespace(target), deployment(target, "api", "registry.example/api:1.0.0", nil))
	fr := newRollout(ns, "shop", v1, "1", "100%")
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}
	good := waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
	stable := good.Status.Revision

	// v2 crash-loops in syd-1 only. The canary (bkk-1) is fine, so the
	// rollout gets as far as wave 2 before the deadline trips.
	update(t, fr, func(cur *fleetv1.FleetRollout) {
		cur.Spec.Resources = mustRaw(t, namespace(target),
			deployment(target, "api", "registry.example/api:2.0.0", map[string]string{testenv.AnnotationUnhealthyIn: "syd-1"}))
	})
	rb := waitPhase(t, fr, fleetv1.PhaseRolledBack, 40*time.Second)

	if rb.Status.StableRevision != stable || rb.Status.Revision == stable {
		t.Fatalf("revisions: stable=%s revision=%s", rb.Status.StableRevision, rb.Status.Revision)
	}
	syd := clusterStatus(rb, "syd-1")
	if !strings.Contains(syd.Message, "progress deadline") || !strings.Contains(syd.Message, "available") {
		t.Fatalf("syd-1 message should explain the failure: %+v", syd)
	}
	if !meta.IsStatusConditionTrue(rb.Status.Conditions, "Degraded") {
		t.Fatalf("Degraded not set: %+v", rb.Status.Conditions)
	}
	// Every cluster, including the canary that was healthy on v2, is back on v1.
	for _, m := range fleet.Members {
		eventually(t, 10*time.Second, m.Name+" back on v1", func() error {
			d, err := memberDeployment(t, m.Name, target, "api")
			if err != nil {
				return err
			}
			img := d.Spec.Template.Spec.Containers[0].Image
			if img != "registry.example/api:1.0.0" || d.Annotations[controller.AnnotationRevision] != stable {
				return fmt.Errorf("image=%s rev=%s", img, d.Annotations[controller.AnnotationRevision])
			}
			if _, ok := d.Annotations[testenv.AnnotationUnhealthyIn]; ok {
				return fmt.Errorf("v2-only annotation survived the rollback")
			}
			return nil
		})
	}

	// Rolled back is sticky until the spec changes: nothing re-applies v2.
	time.Sleep(time.Second)
	if cur := get(t, fr); cur.Status.Phase != fleetv1.PhaseRolledBack {
		t.Fatalf("phase moved to %s without a spec change", cur.Status.Phase)
	}

	// Shipping a fix starts a fresh rollout.
	update(t, fr, func(cur *fleetv1.FleetRollout) {
		cur.Spec.Resources = mustRaw(t, namespace(target), deployment(target, "api", "registry.example/api:2.0.1", nil))
	})
	fixed := waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
	if fixed.Status.StableRevision == stable {
		t.Fatal("stable revision did not advance after the fix")
	}
}

func TestPruneRemovesObjectsDroppedFromSpec(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-prune"
	fr := newRollout(ns, "cfg", mustRaw(t, namespace(target), configMap(target, "keep", "1"), configMap(target, "drop", "1")), "100%")
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
	update(t, fr, func(cur *fleetv1.FleetRollout) {
		cur.Spec.Resources = mustRaw(t, namespace(target), configMap(target, "keep", "2"))
	})
	waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
	for _, m := range fleet.Members {
		eventually(t, 10*time.Second, m.Name+" pruned", func() error {
			cm := &corev1.ConfigMap{}
			err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: target, Name: "drop"}, cm)
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("drop still present (err=%v)", err)
			}
			if err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: target, Name: "keep"}, cm); err != nil {
				return err
			}
			if cm.Data["value"] != "2" {
				return fmt.Errorf("keep not updated: %v", cm.Data)
			}
			return nil
		})
	}
}

func TestOwnershipConflictIsRejectedNotStolen(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-owner"
	first := newRollout(ns, "first", mustRaw(t, namespace(target), configMap(target, "shared", "first")), "100%")
	if err := fleet.HubClient.Create(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, first, fleetv1.PhaseSucceeded, 30*time.Second)

	second := newRollout(ns, "second", mustRaw(t, configMap(target, "shared", "second")), "100%")
	if err := fleet.HubClient.Create(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	halted := waitPhase(t, second, fleetv1.PhaseHalted, 30*time.Second)
	for _, c := range halted.Status.Clusters {
		if c.State != fleetv1.ClusterFailed || !strings.Contains(c.Message, "owned by another FleetRollout") {
			t.Fatalf("%s = %+v", c.Name, c)
		}
	}
	for _, m := range fleet.Members {
		cm := &corev1.ConfigMap{}
		if err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: target, Name: "shared"}, cm); err != nil {
			t.Fatal(err)
		}
		if cm.Data["value"] != "first" || cm.Annotations[controller.AnnotationOwner] != ns+"/first" {
			t.Fatalf("%s: object was taken over: %v %v", m.Name, cm.Data, cm.Annotations)
		}
	}
}

func TestDeleteCleansUpMemberClusters(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-delete"
	fr := newRollout(ns, "temp", mustRaw(t, namespace(target), configMap(target, "a", "1"), configMap(target, "b", "1")), "100%")
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
	if err := fleet.HubClient.Delete(context.Background(), get(t, fr)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "rollout gone", func() error {
		err := fleet.HubClient.Get(context.Background(), client.ObjectKeyFromObject(fr), &fleetv1.FleetRollout{})
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("still present (err=%v)", err)
		}
		return nil
	})
	for _, m := range fleet.Members {
		for _, name := range []string{"a", "b"} {
			err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: target, Name: name}, &corev1.ConfigMap{})
			if !apierrors.IsNotFound(err) {
				t.Fatalf("%s: ConfigMap %s survived deletion (err=%v)", m.Name, name, err)
			}
		}
	}
}

func TestPausedRolloutAppliesNothing(t *testing.T) {
	ns := hubNamespace(t)
	target := "shop-paused"
	fr := newRollout(ns, "held", mustRaw(t, namespace(target), configMap(target, "x", "1")), "100%")
	fr.Spec.Paused = true
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}
	waitPhase(t, fr, fleetv1.PhasePaused, 20*time.Second)
	time.Sleep(time.Second)
	for _, m := range fleet.Members {
		err := m.Client.Get(context.Background(), types.NamespacedName{Namespace: target, Name: "x"}, &corev1.ConfigMap{})
		if !apierrors.IsNotFound(err) {
			t.Fatalf("%s received a paused rollout (err=%v)", m.Name, err)
		}
	}
	update(t, fr, func(cur *fleetv1.FleetRollout) { cur.Spec.Paused = false })
	waitPhase(t, fr, fleetv1.PhaseSucceeded, 30*time.Second)
}

func TestUnreachableClusterBlocksUntilCordoned(t *testing.T) {
	ctx := context.Background()
	// "ghost" has a kubeconfig whose API server does not exist.
	kcfg, err := clientcmd.Load(fleet.Member("bkk-1").Kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range kcfg.Clusters {
		c.Server = "https://127.0.0.1:1"
	}
	kc, err := clientcmd.Write(*kcfg)
	if err != nil {
		t.Fatal(err)
	}
	sec := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: testenv.SecretNamespace, Name: "ghost-kubeconfig"},
		Data: map[string][]byte{"value": kc}}
	if err := fleet.HubClient.Create(ctx, sec); err != nil {
		t.Fatal(err)
	}
	ghost := &fleetv1.MemberCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "ghost", Labels: map[string]string{"name": "ghost"}},
		Spec: fleetv1.MemberClusterSpec{KubeconfigSecretRef: fleetv1.SecretKeyReference{
			Namespace: testenv.SecretNamespace, Name: sec.Name}},
	}
	if err := fleet.HubClient.Create(ctx, ghost); err != nil {
		t.Fatal(err)
	}
	defer fleet.HubClient.Delete(ctx, ghost)
	eventually(t, 15*time.Second, "ghost Unreachable", func() error {
		mc := &fleetv1.MemberCluster{}
		if err := fleet.HubClient.Get(ctx, types.NamespacedName{Name: "ghost"}, mc); err != nil {
			return err
		}
		c := meta.FindStatusCondition(mc.Status.Conditions, fleetv1.ConditionReady)
		if c == nil || c.Status != metav1.ConditionFalse || c.Reason != "Unreachable" {
			return fmt.Errorf("condition %+v", c)
		}
		return nil
	})

	ns := hubNamespace(t)
	target := "shop-ghost"
	fr := newRollout(ns, "g", mustRaw(t, namespace(target), configMap(target, "x", "1")), "100%")
	fr.Spec.ClusterSelector = metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
		{Key: "name", Operator: metav1.LabelSelectorOpIn, Values: []string{"bkk-1", "ghost"}},
	}}
	if err := fleet.HubClient.Create(ctx, fr); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "ghost pending, bkk-1 healthy", func() error {
		cur := get(t, fr)
		g, b := clusterStatus(cur, "ghost"), clusterStatus(cur, "bkk-1")
		if g.State != fleetv1.ClusterPending || !strings.Contains(g.Message, "not Ready") || b.State != fleetv1.ClusterHealthy {
			return fmt.Errorf("%s", clusterSummary(cur))
		}
		if cur.Status.Phase != fleetv1.PhaseProgressing {
			return fmt.Errorf("phase %s", cur.Status.Phase)
		}
		return nil
	})

	// Cordoning takes it out of the rollout's path.
	eventually(t, 5*time.Second, "cordon", func() error {
		mc := &fleetv1.MemberCluster{}
		if err := fleet.HubClient.Get(ctx, types.NamespacedName{Name: "ghost"}, mc); err != nil {
			return err
		}
		mc.Spec.Cordoned = true
		return fleet.HubClient.Update(ctx, mc)
	})
	done := waitPhase(t, fr, fleetv1.PhaseSucceeded, 20*time.Second)
	if clusterStatus(done, "ghost").State != fleetv1.ClusterSkipped {
		t.Fatalf("ghost = %+v", clusterStatus(done, "ghost"))
	}
	// Deleting must not hang on the cordoned, unreachable cluster.
	if err := fleet.HubClient.Delete(ctx, done); err != nil {
		t.Fatal(err)
	}
	eventually(t, 20*time.Second, "rollout deleted", func() error {
		err := fleet.HubClient.Get(ctx, client.ObjectKeyFromObject(fr), &fleetv1.FleetRollout{})
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("still present (err=%v)", err)
		}
		return nil
	})
}

func TestInvalidSpecIsReported(t *testing.T) {
	ns := hubNamespace(t)
	fr := newRollout(ns, "bad", mustRaw(t, map[string]interface{}{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{}}))
	if err := fleet.HubClient.Create(context.Background(), fr); err != nil {
		t.Fatal(err)
	}
	cur := waitPhase(t, fr, fleetv1.PhaseInvalid, 15*time.Second)
	if !strings.Contains(cur.Status.Message, "metadata.name is required") {
		t.Fatalf("message = %q", cur.Status.Message)
	}
}

// The samples in the README must be accepted by the real CRD schemas.
func TestSamplesAreValid(t *testing.T) {
	for _, f := range []string{"memberclusters.yaml", "fleetrollout.yaml"} {
		data, err := os.ReadFile(filepath.Join(testenv.RepoRoot(), "config", "samples", f))
		if err != nil {
			t.Fatal(err)
		}
		for i, doc := range strings.Split(string(data), "\n---\n") {
			u := &unstructured.Unstructured{}
			if err := yaml.Unmarshal([]byte(doc), &u.Object); err != nil {
				t.Fatalf("%s[%d]: %v", f, i, err)
			}
			u.SetName("sample-" + u.GetName()) // do not collide with the test fleet
			if err := fleet.HubClient.Create(context.Background(), u, client.DryRunAll); err != nil {
				t.Fatalf("%s[%d] rejected by the API server: %v", f, i, err)
			}
		}
	}
}
