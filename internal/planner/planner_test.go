package planner

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/intstr"

	fleetv1 "github.com/AnirudhBabu/fleetwave/api/v1alpha1"
	"github.com/AnirudhBabu/fleetwave/internal/health"
)

func members(names ...string) []Member {
	out := make([]Member, len(names))
	for i, n := range names {
		out[i] = Member{Name: n}
	}
	return out
}

func waves(ss ...string) []intstr.IntOrString {
	out := make([]intstr.IntOrString, len(ss))
	for i, s := range ss {
		out[i] = intstr.Parse(s)
	}
	return out
}

func TestAssignWaves(t *testing.T) {
	tests := []struct {
		name    string
		members []Member
		waves   []intstr.IntOrString
		want    [][]string
		wantErr string
	}{
		{
			name:    "default canary then half then all",
			members: members("d", "c", "b", "a"),
			waves:   DefaultWaves(),
			want:    [][]string{{"a"}, {"b"}, {"c", "d"}},
		},
		{
			name:    "canary label goes first regardless of name",
			members: []Member{{Name: "a"}, {Name: "z", Canary: true}, {Name: "m"}},
			waves:   waves("1", "100%"),
			want:    [][]string{{"z"}, {"a", "m"}},
		},
		{
			name:    "percent rounds up",
			members: members("a", "b", "c", "d", "e", "f", "g", "h", "i", "j"),
			waves:   waves("5%", "25%", "100%"),
			want:    [][]string{{"a"}, {"b", "c"}, {"d", "e", "f", "g", "h", "i", "j"}},
		},
		{
			name:    "waves that add nothing are dropped",
			members: members("a", "b"),
			waves:   waves("1", "50%", "100%"),
			want:    [][]string{{"a"}, {"b"}},
		},
		{
			name:    "single cluster collapses to one wave",
			members: members("only"),
			waves:   DefaultWaves(),
			want:    [][]string{{"only"}},
		},
		{
			name:    "last wave is widened to everything",
			members: members("a", "b", "c"),
			waves:   waves("1", "2"),
			want:    [][]string{{"a"}, {"b", "c"}},
		},
		{
			name:    "counts larger than the fleet are clamped",
			members: members("a", "b"),
			waves:   waves("10", "100%"),
			want:    [][]string{{"a", "b"}},
		},
		{
			name:    "non-monotonic targets never shrink a wave",
			members: members("a", "b", "c", "d"),
			waves:   waves("3", "1", "100%"),
			want:    [][]string{{"a", "b", "c"}, {"d"}},
		},
		{name: "no members", members: nil, waves: DefaultWaves(), want: nil},
		{name: "zero percent rejected", members: members("a"), waves: waves("0%", "100%"), wantErr: "must be in (0, 100]"},
		{name: "junk rejected", members: members("a"), waves: waves("abc"), wantErr: "invalid wave size"},
		{name: "zero count rejected", members: members("a"), waves: waves("0", "100%"), wantErr: "at least 1"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AssignWaves(tt.members, tt.waves)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

var t0 = time.Date(2027, 1, 4, 9, 0, 0, 0, time.UTC)

// fleet builds input for clusters a(wave0) b,c(wave1) d(wave2) on revision rev.
func baseInput() Input {
	return Input{
		Now:              t0,
		Desired:          "new",
		Stable:           "old",
		TotalWaves:       3,
		BakeTime:         time.Minute,
		ProgressDeadline: 5 * time.Minute,
		AutoRollback:     true,
		PollInterval:     5 * time.Second,
		Clusters: []Cluster{
			{Name: "a", Wave: 0, Revision: "old", State: fleetv1.ClusterHealthy, Reachable: true},
			{Name: "b", Wave: 1, Revision: "old", State: fleetv1.ClusterHealthy, Reachable: true},
			{Name: "c", Wave: 1, Revision: "old", State: fleetv1.ClusterHealthy, Reachable: true},
			{Name: "d", Wave: 2, Revision: "old", State: fleetv1.ClusterHealthy, Reachable: true},
		},
	}
}

func state(out Output, name string) Cluster {
	for _, c := range out.Clusters {
		if c.Name == name {
			return c
		}
	}
	panic("no cluster " + name)
}

func hasEvent(out Output, reason string) bool {
	for _, e := range out.Events {
		if e.Reason == reason {
			return true
		}
	}
	return false
}

func TestNewRevisionAppliesOnlyFirstWave(t *testing.T) {
	out := Plan(baseInput())
	if !reflect.DeepEqual(out.Apply, []string{"a"}) {
		t.Fatalf("apply = %v, want [a]", out.Apply)
	}
	if out.Phase != fleetv1.PhaseProgressing || out.RequeueAfter != 5*time.Second {
		t.Fatalf("phase %s requeue %s", out.Phase, out.RequeueAfter)
	}
	if s := state(out, "b"); s.State != fleetv1.ClusterPending || !strings.Contains(s.Message, "wave 1") {
		t.Fatalf("b = %+v", s)
	}
}

func TestFullProgressionWithBake(t *testing.T) {
	in := baseInput()
	apply := func(out Output) {
		for _, n := range out.Apply {
			for i := range in.Clusters {
				if in.Clusters[i].Name == n {
					in.Clusters[i].Revision = "new"
					in.Clusters[i].AppliedAt = in.Now
					in.Clusters[i].State = fleetv1.ClusterProgressing
				}
			}
		}
	}
	step := func() Output {
		out := Plan(in)
		apply(out)
		in.Phase, in.CurrentWave, in.WaveCompletedAt, in.Stable = out.Phase, out.CurrentWave, out.WaveCompletedAt, out.StableRevision
		for i := range in.Clusters {
			in.Clusters[i].State = state(out, in.Clusters[i].Name).State
			if in.Clusters[i].Revision == "new" {
				in.Clusters[i].Health = health.Result{Status: health.Healthy}
			}
		}
		return out
	}

	out := step() // apply a
	if !reflect.DeepEqual(out.Apply, []string{"a"}) {
		t.Fatalf("apply %v", out.Apply)
	}
	in.Now = in.Now.Add(10 * time.Second)
	out = step() // a healthy -> wave done, start bake
	if out.Phase != fleetv1.PhaseBaking || !hasEvent(out, "WaveCompleted") || len(out.Apply) != 0 {
		t.Fatalf("expected baking, got %s apply=%v", out.Phase, out.Apply)
	}
	in.Now = in.Now.Add(30 * time.Second)
	out = step()
	if out.Phase != fleetv1.PhaseBaking || len(out.Apply) != 0 {
		t.Fatalf("bake must hold wave 2 back; got %s apply=%v", out.Phase, out.Apply)
	}
	in.Now = in.Now.Add(31 * time.Second)
	out = step()
	if out.CurrentWave != 1 || !reflect.DeepEqual(out.Apply, []string{"b", "c"}) || !hasEvent(out, "WaveStarted") {
		t.Fatalf("expected wave 2 [b c], got wave %d apply %v", out.CurrentWave, out.Apply)
	}
	in.Now = in.Now.Add(10 * time.Second)
	step()
	in.Now = in.Now.Add(61 * time.Second)
	out = step()
	if !reflect.DeepEqual(out.Apply, []string{"d"}) {
		t.Fatalf("expected [d], got %v", out.Apply)
	}
	in.Now = in.Now.Add(10 * time.Second)
	out = step()
	if out.Phase != fleetv1.PhaseSucceeded || out.StableRevision != "new" || !hasEvent(out, "Succeeded") {
		t.Fatalf("expected success, got %s stable=%s", out.Phase, out.StableRevision)
	}
}

func TestProgressDeadlineFailsAndRollsBack(t *testing.T) {
	in := baseInput()
	in.CurrentWave = 1
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterHealthy, Reachable: true,
		Health: health.Result{Status: health.Healthy}, AppliedAt: t0.Add(-10 * time.Minute)}
	in.Clusters[1] = Cluster{Name: "b", Wave: 1, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		Health: health.Result{Status: health.Progressing, Message: "Deployment/api: 0 of 3 updated replicas available"}, AppliedAt: t0.Add(-6 * time.Minute)}

	out := Plan(in)
	b := state(out, "b")
	if b.State != fleetv1.ClusterFailed || !strings.Contains(b.Message, "progress deadline") || !strings.Contains(b.Message, "0 of 3") {
		t.Fatalf("b = %+v", b)
	}
	if out.Phase != fleetv1.PhaseRolledBack {
		t.Fatalf("phase = %s", out.Phase)
	}
	// Both clusters that got "new" go back, including the healthy canary.
	if !reflect.DeepEqual(out.Rollback, []string{"a", "b"}) {
		t.Fatalf("rollback = %v", out.Rollback)
	}
	if len(out.Apply) != 0 {
		t.Fatalf("must not apply while rolling back: %v", out.Apply)
	}
	if !hasEvent(out, "ClusterFailed") || !hasEvent(out, "RolledBack") {
		t.Fatalf("events = %+v", out.Events)
	}
}

func TestDegradedFailsImmediately(t *testing.T) {
	in := baseInput()
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		AppliedAt: t0, Health: health.Result{Status: health.Degraded, Message: "ProgressDeadlineExceeded"}}
	out := Plan(in)
	if state(out, "a").State != fleetv1.ClusterFailed || out.Phase != fleetv1.PhaseRolledBack {
		t.Fatalf("got %s / %s", state(out, "a").State, out.Phase)
	}
}

func TestFailureBudgetToleratesFailures(t *testing.T) {
	in := baseInput()
	in.MaxFailures = 1
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		AppliedAt: t0, Health: health.Result{Status: health.Degraded, Message: "boom"}}
	out := Plan(in)
	if out.Phase == fleetv1.PhaseRolledBack || out.Phase == fleetv1.PhaseHalted {
		t.Fatalf("one failure is within budget, got %s", out.Phase)
	}
	// A tolerated failure counts as done: the wave completes and bakes.
	if out.Phase != fleetv1.PhaseBaking {
		t.Fatalf("phase = %s, want Baking", out.Phase)
	}
}

func TestNoStableRevisionHalts(t *testing.T) {
	in := baseInput()
	in.Stable = ""
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		AppliedAt: t0, Health: health.Result{Status: health.Degraded, Message: "boom"}}
	out := Plan(in)
	if out.Phase != fleetv1.PhaseHalted || len(out.Rollback) != 0 || !strings.Contains(out.Message, "no stable revision") {
		t.Fatalf("got %s rollback=%v msg=%q", out.Phase, out.Rollback, out.Message)
	}
}

func TestAutoRollbackDisabledHalts(t *testing.T) {
	in := baseInput()
	in.AutoRollback = false
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		AppliedAt: t0, Health: health.Result{Status: health.Degraded, Message: "boom"}}
	out := Plan(in)
	if out.Phase != fleetv1.PhaseHalted || len(out.Rollback) != 0 {
		t.Fatalf("got %s rollback=%v", out.Phase, out.Rollback)
	}
}

func TestHaltedIsSticky(t *testing.T) {
	in := baseInput()
	in.Phase = fleetv1.PhaseHalted
	out := Plan(in)
	if out.Phase != fleetv1.PhaseHalted || len(out.Apply) != 0 || out.RequeueAfter != 0 {
		t.Fatalf("halted rollout moved: %+v", out)
	}
}

func TestRolledBackRetriesUnfinishedRollbacks(t *testing.T) {
	in := baseInput()
	in.Phase = fleetv1.PhaseRolledBack
	in.Clusters[0].Revision = "new" // rollback apply failed last time
	out := Plan(in)
	if !reflect.DeepEqual(out.Rollback, []string{"a"}) || out.RequeueAfter == 0 {
		t.Fatalf("rollback=%v requeue=%s", out.Rollback, out.RequeueAfter)
	}
}

func TestRegressionDuringBakeFails(t *testing.T) {
	in := baseInput()
	completed := t0.Add(-30 * time.Second)
	in.WaveCompletedAt = &completed
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterHealthy, Reachable: true,
		AppliedAt: t0.Add(-time.Minute), Health: health.Result{Status: health.Progressing, Message: "0 of 2 available"}}
	out := Plan(in)
	a := state(out, "a")
	if a.State != fleetv1.ClusterFailed || !strings.Contains(a.Message, "regressed") {
		t.Fatalf("a = %+v", a)
	}
}

func TestUnknownHealthOnHealthyClusterIsNotARegression(t *testing.T) {
	in := baseInput()
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterHealthy, Reachable: true,
		AppliedAt: t0.Add(-time.Minute), Health: health.Result{Status: health.Unknown, Message: "timeout"}}
	out := Plan(in)
	if state(out, "a").State != fleetv1.ClusterHealthy {
		t.Fatalf("a = %+v", state(out, "a"))
	}
}

func TestPausedHoldsNewClustersButStillFails(t *testing.T) {
	in := baseInput()
	in.Paused = true
	out := Plan(in)
	if out.Phase != fleetv1.PhasePaused || len(out.Apply) != 0 {
		t.Fatalf("paused applied: %s %v", out.Phase, out.Apply)
	}
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterProgressing, Reachable: true,
		AppliedAt: t0, Health: health.Result{Status: health.Degraded, Message: "boom"}}
	out = Plan(in)
	if out.Phase != fleetv1.PhaseRolledBack {
		t.Fatalf("pause must not hide failures, got %s", out.Phase)
	}
}

func TestPausedDoesNotAdvancePastBake(t *testing.T) {
	in := baseInput()
	in.Paused = true
	done := t0.Add(-time.Hour)
	in.WaveCompletedAt = &done
	in.Clusters[0] = Cluster{Name: "a", Wave: 0, Revision: "new", State: fleetv1.ClusterHealthy, Reachable: true,
		AppliedAt: t0.Add(-2 * time.Hour), Health: health.Result{Status: health.Healthy}}
	out := Plan(in)
	if out.Phase != fleetv1.PhasePaused || out.CurrentWave != 0 || len(out.Apply) != 0 {
		t.Fatalf("got %s wave %d apply %v", out.Phase, out.CurrentWave, out.Apply)
	}
}

func TestUnreachableClusterBlocksWaveButCordonedIsSkipped(t *testing.T) {
	in := baseInput()
	in.Clusters[0].Reachable = false
	out := Plan(in)
	if len(out.Apply) != 0 || out.Phase != fleetv1.PhaseProgressing || !strings.Contains(state(out, "a").Message, "not Ready") {
		t.Fatalf("unreachable must block: %s %v %q", out.Phase, out.Apply, state(out, "a").Message)
	}
	in.Clusters[0].Cordoned = true
	out = Plan(in)
	if state(out, "a").State != fleetv1.ClusterSkipped || out.Phase != fleetv1.PhaseBaking {
		t.Fatalf("cordoned should be skipped and the wave done: %s %s", state(out, "a").State, out.Phase)
	}
}

func TestSucceededReappliesForDrift(t *testing.T) {
	in := baseInput()
	in.Phase = fleetv1.PhaseSucceeded
	in.Stable = "new"
	in.CurrentWave = 2
	in.DriftInterval = 10 * time.Minute
	for i := range in.Clusters {
		in.Clusters[i].Revision = "new"
		in.Clusters[i].AppliedAt = t0.Add(-time.Minute)
	}
	in.Clusters[3].AppliedAt = t0.Add(-11 * time.Minute)
	out := Plan(in)
	if out.Phase != fleetv1.PhaseSucceeded || !reflect.DeepEqual(out.Reapply, []string{"d"}) || out.RequeueAfter != 10*time.Minute {
		t.Fatalf("got %s reapply=%v requeue=%s", out.Phase, out.Reapply, out.RequeueAfter)
	}
	if hasEvent(out, "Succeeded") {
		t.Fatal("Succeeded event should fire once, not on every resync")
	}
}

func TestLateJoinerGetsStableRevision(t *testing.T) {
	in := baseInput()
	in.Phase = fleetv1.PhaseSucceeded
	in.Stable = "new"
	in.CurrentWave = 2
	for i := range in.Clusters {
		in.Clusters[i].Revision = "new"
	}
	in.Clusters = append(in.Clusters, Cluster{Name: "e", Wave: 2, Reachable: true})
	out := Plan(in)
	if !reflect.DeepEqual(out.Apply, []string{"e"}) {
		t.Fatalf("apply = %v", out.Apply)
	}
}

func TestNoClusters(t *testing.T) {
	out := Plan(Input{Desired: "x"})
	if out.Phase != fleetv1.PhaseSucceeded || !strings.Contains(out.Message, "no member clusters") {
		t.Fatalf("got %s %q", out.Phase, out.Message)
	}
}
