// Command fleetwave runs the hub controller manager.
package main

import (
	"flag"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/controller"
	"github.com/AnirudhBabu/fleetwave/internal/remote"
)

func main() {
	var (
		metricsAddr      string
		probeAddr        string
		leaderElect      bool
		leaderElectNS    string
		pollInterval     time.Duration
		driftInterval    time.Duration
		probeInterval    time.Duration
		applyConcurrency int
		memberQPS        float64
	)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Address for the Prometheus metrics endpoint.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Address for liveness and readiness probes.")
	flag.BoolVar(&leaderElect, "leader-elect", false, "Enable leader election so only one replica reconciles.")
	flag.StringVar(&leaderElectNS, "leader-election-namespace", "", "Namespace for the leader election lease. Defaults to the pod's namespace in-cluster; required when running outside a cluster.")
	flag.DurationVar(&pollInterval, "poll-interval", 5*time.Second, "How often in-flight clusters are re-checked.")
	flag.DurationVar(&driftInterval, "drift-interval", 10*time.Minute, "Re-apply finished rollouts this often to undo manual edits (0 disables).")
	flag.DurationVar(&probeInterval, "probe-interval", 30*time.Second, "How often member clusters are probed.")
	flag.IntVar(&applyConcurrency, "apply-concurrency", 10, "Parallel member cluster operations per rollout reconcile.")
	flag.Float64Var(&memberQPS, "member-qps", 20, "Client QPS limit per member cluster.")
	opts := zap.Options{Development: false}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))
	log := ctrl.Log.WithName("setup")

	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(fleetv1.AddToScheme(scheme))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                        scheme,
		Cache:                         controller.CacheOptions(),
		Metrics:                       metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress:        probeAddr,
		LeaderElection:                leaderElect,
		LeaderElectionID:              "fleetwave.fleet.fleetwave.io",
		LeaderElectionNamespace:       leaderElectNS,
		LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		log.Error(err, "unable to create manager")
		os.Exit(1)
	}

	remotes := &remote.Cache{Reader: mgr.GetAPIReader(), QPS: float32(memberQPS), Burst: int(memberQPS * 2)}

	if err := (&controller.MemberClusterReconciler{
		Client:        mgr.GetClient(),
		Remotes:       remotes,
		ProbeInterval: probeInterval,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up controller", "controller", "MemberCluster")
		os.Exit(1)
	}
	if err := (&controller.FleetRolloutReconciler{
		Client:           mgr.GetClient(),
		Scheme:           mgr.GetScheme(),
		Recorder:         mgr.GetEventRecorderFor("fleetwave"),
		Remotes:          remotes,
		PollInterval:     pollInterval,
		DriftInterval:    driftInterval,
		ApplyConcurrency: applyConcurrency,
	}).SetupWithManager(mgr); err != nil {
		log.Error(err, "unable to set up controller", "controller", "FleetRollout")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "manager exited with error")
		os.Exit(1)
	}
}
