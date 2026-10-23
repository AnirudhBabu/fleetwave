package controller_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/controller"
	"github.com/AnirudhBabu/fleetwave/internal/remote"
	"github.com/AnirudhBabu/fleetwave/internal/testenv"
)

// fleet is shared by every integration test: a hub plus three member
// clusters, each its own kube-apiserver and etcd.
var fleet *testenv.Fleet

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Println("KUBEBUILDER_ASSETS not set; skipping integration tests (run `make test`)")
		os.Exit(0)
	}
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctrl.SetLogger(zap.New(zap.UseDevMode(true), zap.Level(zapLevel())))
	var err error
	fleet, err = testenv.Start("bkk-1", "sgp-1", "syd-1")
	if err != nil {
		fmt.Println("starting fleet:", err)
		return 1
	}
	defer fleet.Stop()
	fleet.StartKubeletSim(150 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, mem := range fleet.Members {
		labels := map[string]string{"env": "prod", "name": mem.Name}
		if mem.Name == "bkk-1" {
			labels["fleet.fleetwave.io/canary"] = "true"
		}
		if err := fleet.Register(ctx, mem, labels); err != nil {
			fmt.Println("register:", err)
			return 1
		}
	}

	mgr, err := ctrl.NewManager(fleet.HubConfig, ctrl.Options{
		Scheme:  testenv.Scheme(),
		Cache:   controller.CacheOptions(),
		Metrics: metricsserver.Options{BindAddress: "0"},
	})
	if err != nil {
		fmt.Println("manager:", err)
		return 1
	}
	remotes := &remote.Cache{Reader: mgr.GetAPIReader(), Timeout: 2 * time.Second}
	if err := (&controller.MemberClusterReconciler{
		Client: mgr.GetClient(), Remotes: remotes, ProbeInterval: time.Second, ProbeTimeout: 2 * time.Second,
	}).SetupWithManager(mgr); err != nil {
		fmt.Println(err)
		return 1
	}
	if err := (&controller.FleetRolloutReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(), Recorder: mgr.GetEventRecorderFor("fleetwave"),
		Remotes: remotes, PollInterval: 300 * time.Millisecond,
	}).SetupWithManager(mgr); err != nil {
		fmt.Println(err)
		return 1
	}
	mgrDone := make(chan struct{})
	go func() {
		defer close(mgrDone)
		if err := mgr.Start(ctx); err != nil {
			fmt.Println("manager stopped:", err)
		}
	}()
	// Stop the manager before the API servers go away (deferred fleet.Stop).
	defer func() { cancel(); <-mgrDone }()
	// Wait until the controllers are actually reconciling (every member has
	// been probed) so no test races controller start-up.
	if err := waitMembersReady(ctx, 30*time.Second); err != nil {
		fmt.Println(err)
		return 1
	}
	return m.Run()
}

func zapLevel() zapcore.Level {
	if os.Getenv("FLEETWAVE_TEST_DEBUG") != "" {
		return zapcore.DebugLevel
	}
	return zapcore.ErrorLevel
}

func waitMembersReady(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		var list fleetv1.MemberClusterList
		if err := fleet.HubClient.List(ctx, &list); err == nil {
			ready := 0
			for _, mc := range list.Items {
				if meta.IsStatusConditionTrue(mc.Status.Conditions, fleetv1.ConditionReady) {
					ready++
				}
			}
			if ready == len(fleet.Members) {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	return fmt.Errorf("member clusters did not become Ready within %s", timeout)
}
