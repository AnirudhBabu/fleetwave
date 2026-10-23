// Command demo runs fleetwave against a local, simulated fleet: one hub and
// four member clusters, each a real kube-apiserver + etcd (envtest), with a
// simulated kubelet. It ships a good release, a bad release that only breaks
// in one region, and the fix, and prints what the controller does.
//
//	make demo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/controller"
	"github.com/AnirudhBabu/fleetwave/internal/remote"
	"github.com/AnirudhBabu/fleetwave/internal/testenv"
)

var start = time.Now()

func say(format string, args ...interface{}) {
	fmt.Printf("[%5.1fs] %s\n", time.Since(start).Seconds(), fmt.Sprintf(format, args...))
}

func main() {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS is not set; run `make demo`, which downloads the envtest binaries.")
		os.Exit(1)
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "demo failed:", err)
		os.Exit(1)
	}
}

func run() error {
	quiet := zap.New(zap.Level(zapcore.ErrorLevel))
	ctrl.SetLogger(quiet)
	klog.SetLogger(quiet) // client-go shutdown chatter
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	names := []string{"bkk-1", "sgp-1", "sgp-2", "syd-1"}
	say("starting hub + %d member clusters (%s), each its own kube-apiserver and etcd", len(names), strings.Join(names, ", "))
	fleet, err := testenv.Start(names...)
	if err != nil {
		return err
	}
	defer fleet.Stop()
	fleet.StartKubeletSim(200 * time.Millisecond)
	for _, m := range fleet.Members {
		labels := map[string]string{"env": "prod", "region": m.Name[:3]}
		if m.Name == "bkk-1" {
			labels[fleetv1.LabelCanary] = "true"
		}
		if err := fleet.Register(ctx, m, labels); err != nil {
			return err
		}
	}

	mgr, err := ctrl.NewManager(fleet.HubConfig, ctrl.Options{Scheme: testenv.Scheme(), Cache: controller.CacheOptions(), Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		return err
	}
	remotes := &remote.Cache{Reader: mgr.GetAPIReader(), Timeout: 2 * time.Second}
	if err := (&controller.MemberClusterReconciler{Client: mgr.GetClient(), Remotes: remotes, ProbeInterval: time.Second}).SetupWithManager(mgr); err != nil {
		return err
	}
	if err := (&controller.FleetRolloutReconciler{Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		Recorder: mgr.GetEventRecorderFor("fleetwave"), Remotes: remotes, PollInterval: 300 * time.Millisecond}).SetupWithManager(mgr); err != nil {
		return err
	}
	mgrDone := make(chan struct{})
	go func() { _ = mgr.Start(ctx); close(mgrDone) }()
	// Stop the controller before the API servers it talks to.
	defer func() { cancel(); <-mgrDone }()

	hub := fleet.HubClient
	fr := &fleetv1.FleetRollout{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "checkout"},
		Spec: fleetv1.FleetRolloutSpec{
			ClusterSelector: metav1.LabelSelector{MatchLabels: map[string]string{"env": "prod"}},
			Resources:       manifests("1.4.0", ""),
			Strategy: fleetv1.RolloutStrategy{
				Waves:            []intstr.IntOrString{intstr.FromString("1"), intstr.FromString("50%"), intstr.FromString("100%")},
				BakeTime:         metav1.Duration{Duration: 3 * time.Second},
				ProgressDeadline: metav1.Duration{Duration: 6 * time.Second},
			},
		},
	}

	fmt.Println()
	say("── release 1: checkout 1.4.0 (first rollout, nothing to roll back to yet)")
	if err := hub.Create(ctx, fr); err != nil {
		return err
	}
	if err := watch(ctx, hub, fr, fleetv1.PhaseSucceeded); err != nil {
		return err
	}
	images(ctx, fleet)

	fmt.Println()
	say("── release 2: checkout 1.5.0, which crash-loops in syd-1 only")
	if err := edit(ctx, hub, fr, manifests("1.5.0", "syd-1")); err != nil {
		return err
	}
	if err := watch(ctx, hub, fr, fleetv1.PhaseRolledBack); err != nil {
		return err
	}
	time.Sleep(500 * time.Millisecond)
	images(ctx, fleet)

	fmt.Println()
	say("── release 3: checkout 1.5.1 with the fix")
	if err := edit(ctx, hub, fr, manifests("1.5.1", "")); err != nil {
		return err
	}
	if err := watch(ctx, hub, fr, fleetv1.PhaseSucceeded); err != nil {
		return err
	}
	images(ctx, fleet)
	fmt.Println()
	say("done")
	return nil
}

func manifests(version, unhealthyIn string) []runtime.RawExtension {
	ann := map[string]interface{}{}
	if unhealthyIn != "" {
		ann[testenv.AnnotationUnhealthyIn] = unhealthyIn
	}
	objs := []map[string]interface{}{
		{"apiVersion": "v1", "kind": "Namespace", "metadata": map[string]interface{}{"name": "payments"}},
		{"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]interface{}{"name": "checkout-config", "namespace": "payments"},
			"data": map[string]interface{}{"version": version}},
		{"apiVersion": "apps/v1", "kind": "Deployment",
			"metadata": map[string]interface{}{"name": "checkout", "namespace": "payments", "annotations": ann},
			"spec": map[string]interface{}{
				"replicas": 3,
				"selector": map[string]interface{}{"matchLabels": map[string]interface{}{"app": "checkout"}},
				"template": map[string]interface{}{
					"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": "checkout"}},
					"spec": map[string]interface{}{"containers": []interface{}{
						map[string]interface{}{"name": "checkout", "image": "registry.example/checkout:" + version},
					}},
				},
			}},
	}
	out := make([]runtime.RawExtension, len(objs))
	for i, o := range objs {
		b, _ := json.Marshal(o)
		out[i] = runtime.RawExtension{Raw: b}
	}
	return out
}

func edit(ctx context.Context, c client.Client, fr *fleetv1.FleetRollout, res []runtime.RawExtension) error {
	for i := 0; i < 10; i++ {
		cur := &fleetv1.FleetRollout{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(fr), cur); err != nil {
			return err
		}
		cur.Spec.Resources = res
		if err := c.Update(ctx, cur); err == nil {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("could not update rollout")
}

// watch prints every phase and per-cluster state change until the rollout
// reaches want for the current spec generation.
func watch(ctx context.Context, c client.Client, fr *fleetv1.FleetRollout, want fleetv1.RolloutPhase) error {
	lastPhase := ""
	lastState := map[string]string{}
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		cur := &fleetv1.FleetRollout{}
		if err := c.Get(ctx, client.ObjectKeyFromObject(fr), cur); err != nil {
			return err
		}
		if cur.Status.ObservedGeneration != cur.Generation {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		cs := append([]fleetv1.ClusterRolloutStatus(nil), cur.Status.Clusters...)
		sort.SliceStable(cs, func(i, j int) bool { return cs[i].Wave < cs[j].Wave })
		for _, cl := range cs {
			key := string(cl.State) + cl.Revision
			if lastState[cl.Name] == key {
				continue
			}
			lastState[cl.Name] = key
			msg := ""
			if cl.State == fleetv1.ClusterFailed || cl.State == fleetv1.ClusterRolledBack {
				msg = "  " + cl.Message
			}
			rev := cl.Revision
			if rev == "" {
				rev = "-"
			}
			say("   %-6s wave %d  %-11s rev %-10s%s", cl.Name, cl.Wave+1, cl.State, rev, msg)
		}
		// Phase changes, plus message changes outside the bake countdown.
		p := string(cur.Status.Phase)
		if cur.Status.Phase != fleetv1.PhaseBaking {
			p += cur.Status.Message
		}
		if p != lastPhase {
			lastPhase = p
			say("%-11s %s", cur.Status.Phase, cur.Status.Message)
		}
		if cur.Status.Phase == want {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("timed out waiting for %s", want)
}

func images(ctx context.Context, f *testenv.Fleet) {
	var parts []string
	for _, m := range f.Members {
		d := &appsv1.Deployment{}
		if err := m.Client.Get(ctx, types.NamespacedName{Namespace: "payments", Name: "checkout"}, d); err != nil {
			parts = append(parts, m.Name+"=<none>")
			continue
		}
		img := d.Spec.Template.Spec.Containers[0].Image
		parts = append(parts, m.Name+"="+img[strings.LastIndex(img, ":")+1:])
	}
	say("   running in member clusters: %s", strings.Join(parts, "  "))
}
