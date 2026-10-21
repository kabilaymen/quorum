package raft

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
)

// cluster runs N nodes over a simulated network, one round = one tick for every
// live node followed by message delivery. Everything is driven by one seed.
type cluster struct {
	t       *testing.T
	rand    *rand.Rand
	ids     []NodeID
	nodes   map[NodeID]*Node
	down    map[NodeID]bool        // crashed nodes
	disk    map[NodeID]*savedState // built only from Ready.HardState / Ready.Entries
	cut     map[[2]NodeID]bool     // directed links that drop everything
	inbox   []inflight             // messages in the network
	dropP   float64                // probability of losing a message
	maxLag  int                    // max extra rounds a message may be delayed
	applied map[NodeID][]Entry     // what each state machine applied, in order
	leaders map[uint64]NodeID      // term -> leader (election safety)
	round   int
	preVote bool
	noCheck bool // disable CheckQuorum (and with it the leader lease on votes)
}

type savedState struct {
	hs      HardState
	entries []Entry
}

type inflight struct {
	m       Message
	deliver int
}

func newCluster(t *testing.T, n int, seed int64) *cluster {
	return newClusterWith(t, n, seed, false)
}

func newClusterWith(t *testing.T, n int, seed int64, preVote bool) *cluster {
	c := &cluster{
		t: t, rand: rand.New(rand.NewSource(seed)), preVote: preVote,
		nodes: map[NodeID]*Node{}, down: map[NodeID]bool{}, disk: map[NodeID]*savedState{},
		cut: map[[2]NodeID]bool{}, applied: map[NodeID][]Entry{}, leaders: map[uint64]NodeID{},
	}
	for i := 1; i <= n; i++ {
		c.ids = append(c.ids, NodeID(i))
	}
	for _, id := range c.ids {
		c.nodes[id] = NewNode(c.config(id, seed))
		c.disk[id] = &savedState{}
	}
	return c
}

func (c *cluster) config(id NodeID, seed int64) Config {
	return Config{ID: id, Peers: c.ids, ElectionTick: 10, HeartbeatTick: 2, MaxBatch: 8, Seed: seed, CheckQuorum: !c.noCheck, PreVote: c.preVote}
}

func (c *cluster) step() {
	c.round++
	for _, id := range c.ids {
		if !c.down[id] {
			c.nodes[id].Tick()
		}
	}
	c.collect()
	// deliver due messages in random order
	var due, later []inflight
	for _, f := range c.inbox {
		if f.deliver <= c.round {
			due = append(due, f)
		} else {
			later = append(later, f)
		}
	}
	c.inbox = later
	c.rand.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
	for _, f := range due {
		if c.down[f.m.To] || c.cut[[2]NodeID{f.m.From, f.m.To}] {
			continue
		}
		c.nodes[f.m.To].Step(f.m)
	}
	c.collect()
	c.check()
}

func (c *cluster) collect() {
	for _, id := range c.ids {
		if c.down[id] {
			continue
		}
		rd := c.nodes[id].Ready()
		// Persist exactly what Ready asks for, before any message leaves the node.
		// Crashes restart from this disk, so a Ready that forgets something
		// (a vote, an acknowledged entry) shows up as a safety violation.
		d := c.disk[id]
		if rd.HardState != nil {
			d.hs = *rd.HardState
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].Index
			if first > uint64(len(d.entries))+1 {
				c.t.Fatalf("node %d: Ready.Entries start at %d but disk ends at %d", id, first, len(d.entries))
			}
			d.entries = append(d.entries[:first-1], rd.Entries...)
		}
		c.applied[id] = append(c.applied[id], rd.Committed...)
		for _, m := range rd.Messages {
			if c.rand.Float64() < c.dropP {
				continue
			}
			lag := 0
			if c.maxLag > 0 {
				lag = c.rand.Intn(c.maxLag + 1)
			}
			c.inbox = append(c.inbox, inflight{m: m, deliver: c.round + 1 + lag})
		}
	}
}

// check verifies Raft's safety properties after every round.
func (c *cluster) check() {
	c.t.Helper()
	for _, id := range c.ids {
		n := c.nodes[id]
		if c.down[id] || n.State() != Leader {
			continue
		}
		// Election Safety: at most one leader per term.
		if other, ok := c.leaders[n.Term()]; ok && other != id {
			c.t.Fatalf("round %d: two leaders in term %d: %d and %d", c.round, n.Term(), other, id)
		}
		c.leaders[n.Term()] = id
	}
	// State Machine Safety: no two nodes apply different entries at the same index.
	var ref []Entry
	for _, id := range c.ids {
		a := c.applied[id]
		for i := range min(len(a), len(ref)) {
			if a[i].Index != ref[i].Index || a[i].Term != ref[i].Term || !bytes.Equal(a[i].Data, ref[i].Data) {
				c.t.Fatalf("round %d: node %d applied %+v at position %d, another node applied %+v", c.round, id, a[i], i, ref[i])
			}
		}
		if len(a) > len(ref) {
			ref = a
		}
	}
}

func (c *cluster) leader() NodeID {
	var lead NodeID
	var term uint64
	for _, id := range c.ids {
		n := c.nodes[id]
		if !c.down[id] && n.State() == Leader && n.Term() >= term {
			lead, term = id, n.Term()
		}
	}
	return lead
}

func (c *cluster) runUntil(maxRounds int, cond func() bool) bool {
	for i := 0; i < maxRounds; i++ {
		if cond() {
			return true
		}
		c.step()
	}
	return cond()
}

func (c *cluster) waitLeader() NodeID {
	c.t.Helper()
	if !c.runUntil(500, func() bool { return c.leader() != 0 }) {
		c.t.Fatalf("no leader after 500 rounds")
	}
	return c.leader()
}

func (c *cluster) crash(id NodeID) {
	c.down[id] = true
}

func (c *cluster) restart(id NodeID) {
	s := c.disk[id]
	c.nodes[id] = Restart(c.config(id, int64(c.round)), s.hs, append([]Entry(nil), s.entries...))
	c.applied[id] = nil // the state machine is rebuilt by replaying the log
	c.down[id] = false
}

// isolate cuts every link to and from id.
func (c *cluster) isolate(id NodeID) {
	for _, o := range c.ids {
		if o != id {
			c.cut[[2]NodeID{id, o}] = true
			c.cut[[2]NodeID{o, id}] = true
		}
	}
}

func (c *cluster) heal() { c.cut = map[[2]NodeID]bool{} }

func (c *cluster) propose(data string) bool {
	l := c.leader()
	if l == 0 {
		return false
	}
	_, _, ok := c.nodes[l].Propose([]byte(data))
	return ok
}

// appliedData returns the non-empty commands applied by a node.
func (c *cluster) appliedData(id NodeID) []string {
	var out []string
	for _, e := range c.applied[id] {
		if len(e.Data) > 0 {
			out = append(out, string(e.Data))
		}
	}
	return out
}

func (c *cluster) allApplied(n int) func() bool {
	return func() bool {
		for _, id := range c.ids {
			if !c.down[id] && len(c.appliedData(id)) < n {
				return false
			}
		}
		return true
	}
}

// ---------------------------------------------------------------- tests

func TestElectsOneLeader(t *testing.T) {
	for _, size := range []int{1, 3, 5} {
		t.Run(fmt.Sprintf("%d nodes", size), func(t *testing.T) {
			c := newCluster(t, size, 1)
			lead := c.waitLeader()
			c.runUntil(50, func() bool { return false }) // stays stable
			if c.leader() != lead {
				t.Fatalf("leadership changed without failures: %d -> %d", lead, c.leader())
			}
			for _, id := range c.ids {
				if c.nodes[id].Leader() != lead {
					t.Errorf("node %d follows %d, want %d", id, c.nodes[id].Leader(), lead)
				}
			}
		})
	}
}

func TestReplicatesInOrder(t *testing.T) {
	c := newCluster(t, 5, 2)
	c.waitLeader()
	for i := 0; i < 30; i++ {
		if !c.propose(fmt.Sprintf("cmd-%d", i)) {
			t.Fatal("propose failed on leader")
		}
		c.step()
	}
	if !c.runUntil(200, c.allApplied(30)) {
		t.Fatal("not all commands applied everywhere")
	}
	got := c.appliedData(1)
	for i, d := range got {
		if d != fmt.Sprintf("cmd-%d", i) {
			t.Fatalf("position %d: %s", i, d)
		}
	}
}

func TestNewLeaderAfterLeaderCrash(t *testing.T) {
	c := newCluster(t, 5, 3)
	old := c.waitLeader()
	oldTerm := c.nodes[old].Term()
	c.propose("before")
	c.runUntil(100, c.allApplied(1))

	c.crash(old)
	lead := c.waitLeader()
	if lead == old || c.nodes[lead].Term() <= oldTerm {
		t.Fatalf("expected a new leader in a higher term, got %d (term %d)", lead, c.nodes[lead].Term())
	}
	c.propose("after")
	if !c.runUntil(200, c.allApplied(2)) {
		t.Fatal("survivors did not apply the new command")
	}
	c.restart(old)
	if !c.runUntil(300, c.allApplied(2)) {
		t.Fatal("restarted node did not catch up")
	}
}

func TestMinorityLeaderCannotCommit(t *testing.T) {
	c := newCluster(t, 5, 4)
	old := c.waitLeader()
	c.isolate(old)
	_, _, ok := c.nodes[old].Propose([]byte("lost"))
	if !ok {
		t.Fatal("isolated leader still believes it leads, propose should be accepted locally")
	}
	c.runUntil(100, func() bool { return false })
	if len(c.appliedData(old)) != 0 {
		t.Fatal("isolated leader committed without a majority")
	}

	// The majority side elects a leader and makes progress.
	c.runUntil(300, func() bool { l := c.leader(); return l != 0 && l != old })
	newLead := c.leader()
	if newLead == old {
		t.Fatal("majority did not elect a new leader")
	}
	c.nodes[newLead].Propose([]byte("kept"))
	// Wait until the majority has committed it: an uncommitted entry could
	// legitimately be lost when the healed node, whose term grew during its
	// isolation (no PreVote), forces a new election.
	if !c.runUntil(100, func() bool { return len(c.appliedData(newLead)) == 1 }) {
		t.Fatal("majority side did not commit")
	}
	c.heal()
	if !c.runUntil(300, c.allApplied(1)) {
		t.Fatal("cluster did not converge after healing")
	}
	for _, id := range c.ids {
		if d := c.appliedData(id); len(d) != 1 || d[0] != "kept" {
			t.Fatalf("node %d applied %v, want [kept] (the minority entry must be discarded)", id, d)
		}
	}
}

func TestRestartKeepsTermAndVote(t *testing.T) {
	c := newCluster(t, 3, 5)
	lead := c.waitLeader()
	term := c.nodes[lead].Term()
	c.crash(lead)
	c.restart(lead)
	if got := c.nodes[lead].Term(); got != term {
		t.Fatalf("term after restart = %d, want %d", got, term)
	}
}

// TestRandomizedFaults drives clusters through random message loss, delays,
// partitions and crash/restarts while proposals keep flowing, checking safety
// after every round, then checks liveness once the faults stop.
func TestRandomizedFaults(t *testing.T) {
	runRandomized(t, 5, 0.01, 0.03, false)
}

func TestRandomizedFaultsPreVote(t *testing.T) {
	runRandomized(t, 5, 0.01, 0.03, true)
}

// TestRandomizedCrashChurn: 3 nodes crashing and restarting constantly, the
// setting where forgetting a vote or an entry on restart breaks safety.
func TestRandomizedCrashChurn(t *testing.T) {
	runRandomized(t, 3, 0.08, 0.25, false)
}

func TestRandomizedCrashChurnPreVote(t *testing.T) {
	runRandomized(t, 3, 0.08, 0.25, true)
}

func runRandomized(t *testing.T, size int, crashP, restartP float64, preVote bool) {
	seeds := 40
	if testing.Short() {
		seeds = 5
	}
	if v, err := strconv.Atoi(os.Getenv("RAFT_SEEDS")); err == nil {
		seeds = v // e.g. RAFT_SEEDS=1000 go test -run Randomized ./raft
	}
	for seed := int64(100); seed < 100+int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			c := newClusterWith(t, size, seed, preVote)
			c.dropP = 0.1
			c.maxLag = 3
			proposed := 0
			for r := 0; r < 1500; r++ {
				switch x := c.rand.Float64(); {
				case x < crashP: // crash a live node, keeping a majority up
					alive := 0
					for _, id := range c.ids {
						if !c.down[id] {
							alive++
						}
					}
					if id := c.ids[c.rand.Intn(len(c.ids))]; !c.down[id] && alive > size/2+1 {
						c.crash(id)
					}
				case x < restartP: // restart a crashed node
					if id := c.ids[c.rand.Intn(len(c.ids))]; c.down[id] {
						c.restart(id)
					}
				case x < restartP+0.01:
					c.isolate(c.ids[c.rand.Intn(len(c.ids))])
				case x < restartP+0.03:
					c.heal()
				case x < restartP+0.27:
					if c.propose(fmt.Sprintf("s%d-%d", seed, proposed)) {
						proposed++
					}
				}
				c.step()
			}

			// Liveness: once the network is healthy and everyone is up, the cluster
			// elects a leader and commits a final command on every node.
			c.heal()
			c.dropP, c.maxLag = 0, 0
			for _, id := range c.ids {
				if c.down[id] {
					c.restart(id)
				}
			}
			c.waitLeader()
			// An uncommitted proposal can legitimately be lost when its leader is
			// deposed (e.g. by a restarted node's election): like a real client,
			// retry until it is committed.
			marker := fmt.Sprintf("final-%d", seed)
			waited := 0
			ok := c.runUntil(1000, func() bool {
				if waited%50 == 0 {
					c.propose(marker)
				}
				waited++
				for _, id := range c.ids {
					d := c.appliedData(id)
					if len(d) == 0 || d[len(d)-1] != marker {
						return false
					}
				}
				return true
			})
			if !ok {
				for _, id := range c.ids {
					n := c.nodes[id]
					d := c.appliedData(id)
					last := ""
					if len(d) > 0 {
						last = d[len(d)-1]
					}
					t.Logf("node %d: %s term=%d lead=%d last=%d commit=%d applied=%d lastApplied=%q", id, n.State(), n.Term(), n.Leader(), n.LastIndex(), n.CommitIndex(), len(d), last)
				}
				t.Fatalf("cluster did not converge after faults stopped (proposed %d)", proposed)
			}
		})
	}
}

func TestIsolatedLeaderStepsDown(t *testing.T) {
	c := newCluster(t, 5, 7)
	old := c.waitLeader()
	c.isolate(old)
	// ElectionTick = 10: within two check periods the isolated leader must give up.
	c.runUntil(25, func() bool { return c.nodes[old].State() != Leader })
	if c.nodes[old].State() == Leader {
		t.Fatal("isolated leader still leads after two quorum-check periods")
	}
	if c.nodes[old].Leader() != 0 {
		t.Fatalf("isolated node still names a leader: %d", c.nodes[old].Leader())
	}
}
