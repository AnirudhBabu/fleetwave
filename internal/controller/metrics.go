package controller

import (
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	memberReady = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetwave_member_cluster_ready",
		Help: "1 if the hub can reach the member cluster's API server, else 0.",
	}, []string{"cluster"})

	rolloutClusters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetwave_rollout_clusters",
		Help: "Clusters in a rollout by state.",
	}, []string{"namespace", "rollout", "state"})

	rolloutPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetwave_rollout_phase",
		Help: "1 for the rollout's current phase, 0 for the others.",
	}, []string{"namespace", "rollout", "phase"})

	rolloutWave = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetwave_rollout_current_wave",
		Help: "Wave being rolled out (1-based).",
	}, []string{"namespace", "rollout"})

	rolloutTotalWaves = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetwave_rollout_total_waves",
		Help: "Number of waves after resolving percentages.",
	}, []string{"namespace", "rollout"})

	applyDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "fleetwave_cluster_apply_duration_seconds",
		Help:    "Time to server-side apply one revision to one member cluster.",
		Buckets: prometheus.ExponentialBuckets(0.05, 2, 10),
	}, []string{"result"})

	rollbacksTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fleetwave_rollbacks_total",
		Help: "Automatic rollbacks triggered.",
	}, []string{"namespace", "rollout"})
)

func init() {
	metrics.Registry.MustRegister(memberReady, rolloutClusters, rolloutPhase, rolloutWave, rolloutTotalWaves, applyDuration, rollbacksTotal)
}
