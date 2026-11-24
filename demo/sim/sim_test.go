package sim

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/kabilaymen/quorum/raft"
)

func stepUntil(s *Sim, maxTicks int, cond func() bool) bool {
	for i := 0; i < maxTicks; i++ {
		if cond() {
			return true
		}
		if !s.Step(1) {
			return false
		}
	}
	return cond()
}

func mustLeader(t *testing.T, s *Sim, within int) raft.NodeID {
	t.Helper()
	if !stepUntil(s, within, func() bool { return s.Leader() != 0 }) {
		t.Fatalf("no leader after %d ticks (violation: %q)", within, s.violation)
	}
	return s.Leader()
}

func TestElectsALeader(t *testing.T) {
	s := New(DefaultConfig(1))
	mustLeader(t, s, 60)
	if s.State().Events[len(s.State().Events)-1].Kind != "leader" {
		t.Fatal("the election is not in the event feed")
	}
}

func TestNewLeaderAfterLeaderIsKilled(t *testing.T) {
	s := New(DefaultConfig(2))
	old := mustLeader(t, s, 60)
	term := s.nodes[old].Term()
	s.Kill(old)
	if !stepUntil(s, 60, func() bool { return s.Leader() != 0 && s.Leader() != old }) {
		t.Fatalf("no new leader within 60 ticks after killing node %d", old)
	}
	if got := s.nodes[s.Leader()].Term(); got <= term {
		t.Fatalf("new leader in term %d, want > %d", got, term)
	}
}

func TestNewLeaderAfterLeaderIsIsolated(t *testing.T) {
	s := New(DefaultConfig(3))
	old := mustLeader(t, s, 60)
	s.Isolate(old, true)
	if !stepUntil(s, 80, func() bool { return s.Leader() != 0 && s.Leader() != old }) {
		t.Fatal("the majority did not elect a new leader")
	}
	// CheckQuorum: the isolated old leader steps down by itself.
	if !stepUntil(s, 80, func() bool { return s.nodes[old].State() != raft.Leader }) {
		t.Fatal("the isolated leader never stepped down")
	}
}

func TestIsolatedMinorityCannotCommitThenCatchesUp(t *testing.T) {
	s := New(DefaultConfig(4))
	lead := mustLeader(t, s, 60)
	var f raft.NodeID
	for _, id := range s.ids {
		if id != lead {
			f = id
			break
		}
	}
	s.Isolate(f, true)
	before := s.nodes[f].CommitIndex()
	for i := 0; i < 5; i++ {
		if !s.Put(fmt.Sprintf("k%d", i), "v") {
			t.Fatal("put refused although the majority has a leader")
		}
		s.Step(10)
	}
	s.Step(50)
	if got := s.nodes[s.Leader()].CommitIndex(); got < before+5 {
		t.Fatalf("majority commit index %d, want at least %d", got, before+5)
	}
	if got := s.nodes[f].CommitIndex(); got != before {
		t.Fatalf("isolated node committed %d -> %d", before, got)
	}
	s.Heal()
	leadCommit := func() uint64 { return s.nodes[s.Leader()].CommitIndex() }
	if !stepUntil(s, 200, func() bool { return s.Leader() != 0 && s.nodes[f].CommitIndex() >= leadCommit() }) {
		t.Fatal("the healed node did not catch up")
	}
	if got := s.State().Nodes[f-1].Keys; len(got) != 5 {
		t.Fatalf("healed node holds %d keys, want 5", len(got))
	}
}

func TestRestartKeepsTermAndVote(t *testing.T) {
	s := New(DefaultConfig(5))
	lead := mustLeader(t, s, 60)
	var f raft.NodeID
	for _, id := range s.ids {
		if id != lead {
			f = id
			break
		}
	}
	// What the node really had before the crash, not what the simulation
	// stored: comparing to the disk would pass even if nothing were persisted.
	live, _ := s.nodes[f].Persistent()
	term, vote := live.Term, live.Vote
	if term == 0 || vote == 0 {
		t.Fatalf("node %d has term %d and vote %d after an election: the test proves nothing", f, term, vote)
	}
	s.Kill(f)
	s.Step(5)
	s.Restart(f)
	if s.nodes[f].Term() != term {
		t.Fatalf("restarted with term %d, persisted %d", s.nodes[f].Term(), term)
	}
	if hs, _ := s.nodes[f].Persistent(); hs.Vote != vote {
		t.Fatalf("restarted with vote %d, persisted %d", hs.Vote, vote)
	}
}

// TestRandomChaosKeepsSafety is the demo's promise: whatever the visitor does,
// the two safety properties hold after every step, and the cluster recovers.
func TestRandomChaosKeepsSafety(t *testing.T) {
	seeds := 20
	if testing.Short() {
		seeds = 3
	}
	for seed := int64(1); seed <= int64(seeds); seed++ {
		cfg := DefaultConfig(seed)
		cfg.DropRate = 0.1
		cfg.SnapshotEvery = 20 // lagging nodes regularly need a snapshot
		s := New(cfg)
		r := rand.New(rand.NewSource(seed))
		for round := 0; round < 1000; round++ {
			id := raft.NodeID(1 + r.Intn(cfg.Nodes))
			switch x := r.Float64(); {
			case x < 0.03:
				downs := 0
				for _, d := range s.down {
					if d {
						downs++
					}
				}
				if downs < 2 { // keep a majority alive
					s.Kill(id)
				}
			case x < 0.06:
				s.Restart(id)
			case x < 0.08:
				s.Isolate(id, true)
			case x < 0.11:
				s.Heal()
			case x < 0.4:
				s.Put(fmt.Sprintf("k%d", r.Intn(20)), fmt.Sprint(round))
			}
			if !s.Step(10) {
				t.Fatalf("seed %d, round %d: %s", seed, round, s.violation)
			}
		}
		// Liveness: everyone back, network healthy -> a leader, and a final
		// write reaches every node.
		s.Heal()
		s.SetDropRate(0)
		for _, id := range s.ids {
			s.Restart(id)
		}
		mustLeader(t, s, 300)
		for !s.Put("final", "x") {
			s.Step(10)
		}
		ok := stepUntil(s, 1000, func() bool {
			for _, n := range s.State().Nodes {
				if n.Keys["final"] != "x" {
					return false
				}
			}
			return true
		})
		if !ok {
			// The proposal may have been lost to a leader change: retry once, like a client.
			s.Put("final", "x")
			if !stepUntil(s, 1000, func() bool {
				for _, n := range s.State().Nodes {
					if n.Keys["final"] != "x" {
						return false
					}
				}
				return true
			}) {
				t.Fatalf("seed %d: the final write did not reach every node", seed)
			}
		}
		if s.snapshots == 0 {
			t.Fatalf("seed %d: no snapshot was ever installed, the test does not cover that path", seed)
		}
	}
}

func TestSameSeedSameRun(t *testing.T) {
	run := func() string {
		s := New(DefaultConfig(42))
		s.Step(40)
		s.Kill(s.Leader())
		s.Step(60)
		s.Put("a", "1")
		s.Isolate(2, true)
		s.Step(100)
		s.Heal()
		s.Step(100)
		b, _ := json.Marshal(s.State())
		return string(b)
	}
	if a, b := run(), run(); a != b {
		t.Fatal("two runs with the same seed and actions differ")
	}
}

// The diagram draws the links that carried messages recently: a healthy
// cluster shows the leader talking to every follower, a killed node none.
func TestFlowsFollowTheLeader(t *testing.T) {
	s := New(DefaultConfig(7))
	lead := mustLeader(t, s, 60)
	s.Step(20)
	talks := map[raft.NodeID]bool{}
	for _, f := range s.State().Flows {
		if f.From == lead && f.Kind == "append" {
			talks[f.To] = true
		}
	}
	if len(talks) != 4 {
		t.Fatalf("leader %d sent appends to %d followers recently, want 4", lead, len(talks))
	}
	s.Kill(lead)
	s.Step(FlowTicks + 5)
	for _, f := range s.State().Flows {
		if f.From == lead || f.To == lead {
			t.Fatalf("a killed node still shows traffic: %+v", f)
		}
	}
}
