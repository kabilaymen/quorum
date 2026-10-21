package raft

import "testing"

// deaf cuts the links towards id only: id can still send, nobody reaches it.
// This is what the demo VM showed after a leader restart (the peers kept
// writing into their dead connections to it).
func (c *cluster) deaf(id NodeID) {
	for _, o := range c.ids {
		if o != id {
			c.cut[[2]NodeID{o, id}] = true
		}
	}
}

func (c *cluster) stableLeader(t *testing.T) (NodeID, uint64) {
	t.Helper()
	lead := c.waitLeader()
	c.runUntil(30, func() bool { return false })
	if c.leader() != lead {
		t.Fatalf("leadership not stable before the test: %d -> %d", lead, c.leader())
	}
	return lead, c.nodes[lead].Term()
}

func someFollower(c *cluster, lead NodeID) NodeID {
	for _, id := range c.ids {
		if id != lead {
			return id
		}
	}
	return 0
}

// TestDeafNodeDisruptsWithoutProtection documents the production incident: a
// node that hears nothing keeps calling elections, and each one deposes the
// leader. Both protections are off here: no PreVote, and no CheckQuorum (whose
// leader lease makes nodes ignore vote requests while they hear a leader).
func TestDeafNodeDisruptsWithoutProtection(t *testing.T) {
	c := newCluster(t, 3, 11)
	c.noCheck = true
	for _, id := range c.ids {
		c.nodes[id] = NewNode(c.config(id, 11))
	}
	lead, term := c.stableLeader(t)
	c.deaf(someFollower(c, lead))
	c.runUntil(100, func() bool { return false })
	highest := uint64(0)
	for _, id := range c.ids {
		highest = max(highest, c.nodes[id].Term())
	}
	if highest == term {
		t.Fatal("expected the deaf node to force new terms; did the harness change?")
	}
}

// TestLeaseAloneStopsDeafNode: with CheckQuorum, nodes that still hear the
// leader ignore the deaf node's vote requests, even without PreVote.
func TestLeaseAloneStopsDeafNode(t *testing.T) {
	c := newCluster(t, 3, 11)
	lead, term := c.stableLeader(t)
	c.deaf(someFollower(c, lead))
	c.runUntil(100, func() bool { return false })
	if c.leader() != lead || c.nodes[lead].Term() != term {
		t.Fatalf("leader %d term %d disturbed: now leader %d term %d", lead, term, c.leader(), c.nodes[c.leader()].Term())
	}
}

func TestPreVoteDeafNodeCannotDisrupt(t *testing.T) {
	c := newClusterWith(t, 3, 11, true)
	lead, term := c.stableLeader(t)
	f := someFollower(c, lead)
	c.deaf(f)
	c.runUntil(100, func() bool { return false })
	if c.leader() != lead || c.nodes[lead].Term() != term {
		t.Fatalf("leader %d term %d disturbed by a deaf node: now leader %d term %d", lead, term, c.leader(), c.nodes[c.leader()].Term())
	}
	if got := c.nodes[f].Term(); got != term {
		t.Fatalf("deaf node's term grew to %d (was %d): pre-votes must not change it", got, term)
	}
	if !c.propose("still-works") || !c.runUntil(50, func() bool { return len(c.appliedData(lead)) == 1 }) {
		t.Fatal("the leader stopped committing")
	}
}

func TestPreVoteIsolatedNodeRejoinsQuietly(t *testing.T) {
	c := newClusterWith(t, 5, 12, true)
	lead, term := c.stableLeader(t)
	f := someFollower(c, lead)
	c.isolate(f)
	c.runUntil(200, func() bool { return false })
	if got := c.nodes[f].Term(); got != term {
		t.Fatalf("isolated node's term grew to %d (was %d)", got, term)
	}
	c.heal()
	c.runUntil(50, func() bool { return false })
	if c.leader() != lead || c.nodes[lead].Term() != term {
		t.Fatalf("healing caused an election: leader %d term %d -> leader %d term %d", lead, term, c.leader(), c.nodes[c.leader()].Term())
	}
	if c.nodes[f].Leader() != lead {
		t.Fatalf("healed node follows %d, want %d", c.nodes[f].Leader(), lead)
	}
}

func TestPreVoteStillElectsAfterLeaderCrash(t *testing.T) {
	c := newClusterWith(t, 5, 13, true)
	lead, term := c.stableLeader(t)
	c.crash(lead)
	next := c.waitLeader()
	if next == lead || c.nodes[next].Term() != term+1 {
		t.Fatalf("want a new leader in term %d, got %d in term %d", term+1, next, c.nodes[next].Term())
	}
}

func TestPreVoteSingleNode(t *testing.T) {
	c := newClusterWith(t, 1, 14, true)
	c.waitLeader()
}

// TestPreVoteRefusedWhileLeaderIsHeard: f loses its links to the leader only
// and still talks to the other followers. They keep hearing the leader, so they
// refuse f's pre-votes and f never bumps the term. CheckQuorum is off so that
// this exercises the pre-vote rule itself, not the leader lease.
func TestPreVoteRefusedWhileLeaderIsHeard(t *testing.T) {
	c := newClusterWith(t, 5, 15, true)
	c.noCheck = true
	for _, id := range c.ids {
		c.nodes[id] = NewNode(c.config(id, 15))
	}
	lead, term := c.stableLeader(t)
	f := someFollower(c, lead)
	c.cut[[2]NodeID{lead, f}] = true
	c.cut[[2]NodeID{f, lead}] = true
	c.runUntil(200, func() bool { return false })
	if c.leader() != lead || c.nodes[lead].Term() != term {
		t.Fatalf("leader %d term %d disturbed: now leader %d term %d", lead, term, c.leader(), c.nodes[c.leader()].Term())
	}
	if got := c.nodes[f].Term(); got != term {
		t.Fatalf("partly cut node's term grew to %d (was %d)", got, term)
	}
}
