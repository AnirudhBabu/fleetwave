// Package planner holds the rollout state machine. It is pure: no clients, no
// clocks, no goroutines. The controller gathers observations, calls Plan, and
// executes the returned actions. Keeping the decisions here is what makes the
// interesting behaviour (waves, bake, failure budget, rollback) unit-testable
// without a cluster.
package planner

import (
	"fmt"
	"math"
	"sort"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/health"
)

// DefaultWaves is used when a rollout does not set strategy.waves: one
// canary cluster, then half the fleet, then everything.
func DefaultWaves() []intstr.IntOrString {
	return []intstr.IntOrString{intstr.FromString("1"), intstr.FromString("50%"), intstr.FromString("100%")}
}

// Member is a candidate cluster for wave assignment.
type Member struct {
	Name   string
	Canary bool
}

// AssignWaves orders clusters (canaries first, then by name, so the plan is
// stable across reconciles) and cuts them into waves from cumulative targets.
// Waves that would add no clusters are dropped, and the last wave always
// covers every cluster.
func AssignWaves(members []Member, waves []intstr.IntOrString) ([][]string, error) {
	n := len(members)
	if n == 0 {
		return nil, nil
	}
	ordered := append([]Member(nil), members...)
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Canary != ordered[j].Canary {
			return ordered[i].Canary
		}
		return ordered[i].Name < ordered[j].Name
	})
	if len(waves) == 0 {
		waves = []intstr.IntOrString{intstr.FromString("100%")}
	}

	var out [][]string
	prev := 0
	for i, w := range waves {
		target, err := resolve(w, n)
		if err != nil {
			return nil, fmt.Errorf("wave %d: %w", i, err)
		}
		if i == len(waves)-1 {
			target = n
		}
		if target <= prev {
			continue
		}
		names := make([]string, 0, target-prev)
		for _, m := range ordered[prev:target] {
			names = append(names, m.Name)
		}
		out = append(out, names)
		prev = target
	}
	return out, nil
}

func resolve(w intstr.IntOrString, n int) (int, error) {
	var t int
	if w.Type == intstr.Int {
		t = w.IntValue()
	} else {
		s := w.StrVal
		if len(s) > 0 && s[len(s)-1] == '%' {
			var pct float64
			if _, err := fmt.Sscanf(s[:len(s)-1], "%g", &pct); err != nil {
				return 0, fmt.Errorf("invalid percentage %q", s)
			}
			if pct <= 0 || pct > 100 {
				return 0, fmt.Errorf("percentage %q must be in (0, 100]", s)
			}
			t = int(math.Ceil(pct * float64(n) / 100))
		} else {
			if _, err := fmt.Sscanf(s, "%d", &t); err != nil {
				return 0, fmt.Errorf("invalid wave size %q", s)
			}
		}
	}
	if t < 1 {
		return 0, fmt.Errorf("wave size must be at least 1, got %d", t)
	}
	if t > n {
		t = n
	}
	return t, nil
}

// Cluster is everything the planner knows about one cluster this reconcile.
type Cluster struct {
	Name      string
	Wave      int
	Revision  string               // revision currently applied by this rollout
	State     fleetv1.ClusterState // state recorded last reconcile
	AppliedAt time.Time            // when Revision was applied
	Reachable bool                 // MemberCluster Ready=True
	Cordoned  bool                 // MemberCluster spec.cordoned
	Health    health.Result        // observed now; Unknown if not checked
	Message   string               // output only
}

// Input is a snapshot of the rollout. The controller resets CurrentWave,
// WaveCompletedAt and Phase when the desired revision changes.
type Input struct {
	Now             time.Time
	Desired         string
	Stable          string
	Phase           fleetv1.RolloutPhase
	CurrentWave     int
	WaveCompletedAt *time.Time
	TotalWaves      int
	Paused          bool

	BakeTime         time.Duration
	ProgressDeadline time.Duration
	MaxFailures      int
	AutoRollback     bool
	PollInterval     time.Duration
	DriftInterval    time.Duration

	Clusters []Cluster
}

// Event is something worth telling a human about.
type Event struct {
	Warning bool
	Reason  string
	Message string
}

// Output is the decision for this reconcile.
type Output struct {
	Phase           fleetv1.RolloutPhase
	CurrentWave     int
	WaveCompletedAt *time.Time
	StableRevision  string
	Clusters        []Cluster
	Message         string

	// Apply: clusters that should receive the desired revision now.
	Apply []string
	// Reapply: healthy clusters due a drift-correcting re-apply.
	Reapply []string
	// Rollback: clusters that should receive the stable revision now.
	Rollback []string

	RequeueAfter time.Duration
	Events       []Event
}

func done(s fleetv1.ClusterState) bool {
	return s == fleetv1.ClusterHealthy || s == fleetv1.ClusterFailed || s == fleetv1.ClusterSkipped
}

// Plan decides what to do next.
func Plan(in Input) Output {
	out := Output{
		Phase:           in.Phase,
		CurrentWave:     in.CurrentWave,
		WaveCompletedAt: in.WaveCompletedAt,
		StableRevision:  in.Stable,
		Clusters:        append([]Cluster(nil), in.Clusters...),
	}
	if in.PollInterval <= 0 {
		in.PollInterval = 5 * time.Second
	}

	// Halted and RolledBack are sticky for a revision: a human (or a GitOps
	// commit) has to change the spec before anything moves again.
	if in.Phase == fleetv1.PhaseHalted {
		return out
	}
	if in.Phase == fleetv1.PhaseRolledBack {
		// Keep retrying clusters whose rollback apply did not land yet.
		for _, c := range out.Clusters {
			if c.Revision != in.Desired {
				continue
			}
			out.RequeueAfter = in.PollInterval
			if c.Reachable {
				out.Rollback = append(out.Rollback, c.Name)
			}
		}
		return out
	}
	if in.TotalWaves == 0 || len(out.Clusters) == 0 {
		out.Phase = fleetv1.PhaseSucceeded
		out.Message = "no member clusters match the selector"
		return out
	}
	if out.CurrentWave >= in.TotalWaves {
		out.CurrentWave = in.TotalWaves - 1
	}

	// 1. Derive each cluster's state from what was observed.
	failures := 0
	for i := range out.Clusters {
		c := &out.Clusters[i]
		c.Message = ""
		if c.Revision != in.Desired {
			switch {
			case c.Cordoned:
				c.State = fleetv1.ClusterSkipped
				c.Message = "cluster is cordoned"
			case !c.Reachable:
				c.State = fleetv1.ClusterPending
				c.Message = "waiting: cluster is not Ready"
			default:
				c.State = fleetv1.ClusterPending
				c.Message = fmt.Sprintf("waiting for wave %d", c.Wave)
			}
			continue
		}
		prev := c.State
		age := in.Now.Sub(c.AppliedAt)
		switch {
		case prev == fleetv1.ClusterFailed:
			// sticky; keep the message the controller restored
			c.Message = c.Health.Message
		case c.Health.Status == health.Healthy:
			c.State = fleetv1.ClusterHealthy
		case c.Health.Status == health.Degraded:
			c.State = fleetv1.ClusterFailed
			c.Message = c.Health.Message
		case c.Health.Status == health.Progressing && prev == fleetv1.ClusterHealthy && in.Phase != fleetv1.PhaseSucceeded:
			c.State = fleetv1.ClusterFailed
			c.Message = "regressed after becoming healthy: " + c.Health.Message
		case c.Health.Status == health.Progressing || c.Health.Status == health.Unknown:
			if prev == fleetv1.ClusterHealthy {
				// Unknown on a healthy cluster (e.g. a blip reaching it) is not a regression.
				c.State = fleetv1.ClusterHealthy
				break
			}
			c.State = fleetv1.ClusterProgressing
			c.Message = c.Health.Message
			if c.Health.Status == health.Unknown && c.Message == "" {
				c.Message = "health unknown: cluster not reachable"
			}
			if in.ProgressDeadline > 0 && age > in.ProgressDeadline {
				c.State = fleetv1.ClusterFailed
				c.Message = fmt.Sprintf("progress deadline (%s) exceeded: %s", in.ProgressDeadline, c.Message)
			}
		}
		if c.State == fleetv1.ClusterFailed && prev != fleetv1.ClusterFailed {
			out.Events = append(out.Events, Event{true, "ClusterFailed", fmt.Sprintf("cluster %s failed revision %s: %s", c.Name, in.Desired, c.Message)})
		}
	}
	for _, c := range out.Clusters {
		if c.State == fleetv1.ClusterFailed {
			failures++
		}
	}

	// 2. Failure budget.
	if failures > in.MaxFailures {
		if in.AutoRollback && in.Stable != "" && in.Stable != in.Desired {
			for i := range out.Clusters {
				c := &out.Clusters[i]
				if c.Revision == in.Desired {
					out.Rollback = append(out.Rollback, c.Name)
					if c.State != fleetv1.ClusterFailed {
						c.Message = "rolled back with the rest of the rollout"
					}
				}
			}
			out.Phase = fleetv1.PhaseRolledBack
			out.Message = fmt.Sprintf("%d cluster(s) failed (budget %d); rolling back %d cluster(s) to %s", failures, in.MaxFailures, len(out.Rollback), in.Stable)
			out.Events = append(out.Events, Event{true, "RolledBack", out.Message})
		} else {
			out.Phase = fleetv1.PhaseHalted
			out.Message = fmt.Sprintf("%d cluster(s) failed (budget %d); rollout halted", failures, in.MaxFailures)
			if in.AutoRollback && in.Stable == "" {
				out.Message += "; no stable revision to roll back to"
			}
			out.Events = append(out.Events, Event{true, "Halted", out.Message})
		}
		out.WaveCompletedAt = nil
		return out
	}

	waveDone := func(maxWave int) bool {
		for _, c := range out.Clusters {
			if c.Wave <= maxWave && !done(c.State) {
				return false
			}
		}
		return true
	}
	applyPending := func(maxWave int) {
		for _, c := range out.Clusters {
			if c.Wave <= maxWave && c.State == fleetv1.ClusterPending && c.Reachable {
				out.Apply = append(out.Apply, c.Name)
			}
		}
	}

	// 3. Current wave still in flight.
	w := out.CurrentWave
	if !waveDone(w) {
		out.WaveCompletedAt = nil
		if in.Paused {
			out.Phase = fleetv1.PhasePaused
			out.Message = fmt.Sprintf("paused in wave %d of %d", w+1, in.TotalWaves)
		} else {
			applyPending(w)
			out.Phase = fleetv1.PhaseProgressing
			out.Message = fmt.Sprintf("wave %d of %d in progress", w+1, in.TotalWaves)
		}
		out.RequeueAfter = in.PollInterval
		return out
	}

	// 4. Every wave done: success.
	if w == in.TotalWaves-1 {
		if in.Phase != fleetv1.PhaseSucceeded {
			out.Events = append(out.Events, Event{false, "Succeeded", fmt.Sprintf("revision %s is on every selected cluster", in.Desired)})
		}
		out.Phase = fleetv1.PhaseSucceeded
		out.StableRevision = in.Desired
		out.WaveCompletedAt = nil
		out.Message = fmt.Sprintf("revision %s rolled out to %d cluster(s)", in.Desired, countState(out.Clusters, fleetv1.ClusterHealthy))
		if f := countState(out.Clusters, fleetv1.ClusterFailed); f > 0 {
			out.Message += fmt.Sprintf(", %d failure(s) within budget", f)
		}
		if in.DriftInterval > 0 {
			for _, c := range out.Clusters {
				if c.State == fleetv1.ClusterHealthy && c.Reachable && in.Now.Sub(c.AppliedAt) >= in.DriftInterval {
					out.Reapply = append(out.Reapply, c.Name)
				}
			}
			out.RequeueAfter = in.DriftInterval
		}
		return out
	}

	// 5. Wave done, bake before widening.
	if out.WaveCompletedAt == nil {
		t := in.Now
		out.WaveCompletedAt = &t
		out.Events = append(out.Events, Event{false, "WaveCompleted", fmt.Sprintf("wave %d of %d healthy; baking for %s", w+1, in.TotalWaves, in.BakeTime)})
	}
	if in.Paused {
		out.Phase = fleetv1.PhasePaused
		out.Message = fmt.Sprintf("paused after wave %d of %d", w+1, in.TotalWaves)
		out.RequeueAfter = in.PollInterval
		return out
	}
	if left := out.WaveCompletedAt.Add(in.BakeTime).Sub(in.Now); left > 0 {
		out.Phase = fleetv1.PhaseBaking
		out.Message = fmt.Sprintf("wave %d of %d healthy; baking, %s left", w+1, in.TotalWaves, left.Round(time.Second))
		out.RequeueAfter = min(left, in.PollInterval)
		return out
	}

	// 6. Bake passed: start the next wave.
	out.CurrentWave = w + 1
	out.WaveCompletedAt = nil
	applyPending(out.CurrentWave)
	out.Phase = fleetv1.PhaseProgressing
	out.Message = fmt.Sprintf("wave %d of %d in progress", out.CurrentWave+1, in.TotalWaves)
	out.Events = append(out.Events, Event{false, "WaveStarted", fmt.Sprintf("starting wave %d of %d: %v", out.CurrentWave+1, in.TotalWaves, namesInWave(out.Clusters, out.CurrentWave))})
	out.RequeueAfter = in.PollInterval
	return out
}

func countState(cs []Cluster, s fleetv1.ClusterState) int {
	n := 0
	for _, c := range cs {
		if c.State == s {
			n++
		}
	}
	return n
}

func namesInWave(cs []Cluster, w int) []string {
	var out []string
	for _, c := range cs {
		if c.Wave == w {
			out = append(out, c.Name)
		}
	}
	return out
}
