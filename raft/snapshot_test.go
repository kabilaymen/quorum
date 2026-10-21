package raft

import (
	"fmt"
	"testing"
)

// follower3 returns node 2 of a 3-node cluster holding entries 1..3 of term 1,
// as if appended by a leader of term 1 (node 1).
func follower3(t *testing.T) *Node {
	t.Helper()
	n := NewNode(cfg3(2))
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, Entries: []Entry{
		{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("a")}, {Term: 1, Index: 3, Data: []byte("b")}}})
	n.Ready()
	return n
}

func lastResp(t *testing.T, n *Node) Message {
	t.Helper()
	msgs := n.Ready().Messages
	if len(msgs) == 0 {
		t.Fatal("no response")
	}
	return msgs[len(msgs)-1]
}

func TestSnapshotKeepsMatchingSuffix(t *testing.T) {
	n := follower3(t)
	n.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 2, Term: 1, Data: []byte("s")}})
	if n.FirstIndex() != 3 || n.LastIndex() != 3 {
		t.Fatalf("log = [%d, %d], want [3, 3]: entry 3 follows the snapshot's last entry and must be kept", n.FirstIndex(), n.LastIndex())
	}
}

// The follower's entry 2 is from term 1 but the snapshot says term 2: the log
// diverged, and everything after index 2 must go too.
func TestSnapshotDropsConflictingSuffix(t *testing.T) {
	n := follower3(t)
	n.Step(Message{Type: MsgSnap, From: 3, To: 2, Term: 2, Snapshot: &Snapshot{Index: 2, Term: 2, Data: []byte("s")}})
	rd := n.Ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 2 {
		t.Fatalf("Ready.Snapshot = %+v, want the installed snapshot", rd.Snapshot)
	}
	if n.LastIndex() != 2 {
		t.Fatalf("last index = %d, want 2: the conflicting entry 3 was kept", n.LastIndex())
	}
	if n.CommitIndex() != 2 || n.AppliedIndex() != 2 {
		t.Fatalf("commit/applied = %d/%d, want 2/2", n.CommitIndex(), n.AppliedIndex())
	}
}

// A delayed append that only covers entries already in our snapshot must be
// acknowledged up to the snapshot, never up to entries the leader has not
// checked (here entry 3, which may conflict with the leader's log).
func TestStaleAppendBelowSnapshotDoesNotOverclaim(t *testing.T) {
	n := follower3(t)
	n.Step(Message{Type: MsgSnap, From: 1, To: 2, Term: 1, Snapshot: &Snapshot{Index: 2, Term: 1}})
	n.Ready()
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, PrevIndex: 0, PrevTerm: 0, Entries: []Entry{{Term: 1, Index: 1}}})
	if m := lastResp(t, n); !m.Success || m.Match != 2 {
		t.Fatalf("response = success %v match %d, want success at 2", m.Success, m.Match)
	}
}

func TestSnapshotCatchesUpLaggingFollower(t *testing.T) {
	c := newClusterWith(t, 3, 21, true)
	c.snapEvery = 10
	lead := c.waitLeader()
	f := someFollower(c, lead)
	c.crash(f)
	for i := 0; i < 50; i++ {
		c.propose(fmt.Sprintf("cmd-%d", i))
		c.step()
	}
	if !c.runUntil(200, func() bool { return len(c.appliedData(lead)) == 50 }) {
		t.Fatal("leader did not commit")
	}
	if c.nodes[lead].FirstIndex() <= 2 {
		t.Fatal("leader did not compact its log")
	}
	c.restart(f)
	if !c.runUntil(300, c.allApplied(50)) {
		t.Fatalf("lagging follower did not catch up (applied %d)", len(c.appliedData(f)))
	}
	if c.installs[f] == 0 {
		t.Fatal("follower caught up without a snapshot: the test did not exercise them")
	}
	for i, d := range c.appliedData(f) {
		if d != fmt.Sprintf("cmd-%d", i) {
			t.Fatalf("follower position %d = %s", i, d)
		}
	}
}

func TestRestartFromSnapshotKeepsState(t *testing.T) {
	c := newClusterWith(t, 3, 22, true)
	c.snapEvery = 10
	lead := c.waitLeader()
	for i := 0; i < 30; i++ {
		c.propose(fmt.Sprintf("x%d", i))
		c.step()
	}
	c.runUntil(200, c.allApplied(30))
	for _, id := range c.ids {
		c.crash(id)
	}
	for _, id := range c.ids {
		if c.disk[id].snap.Index == 0 {
			t.Fatalf("node %d never snapshotted", id)
		}
		c.restart(id)
	}
	c.waitLeader()
	if !c.runUntil(200, c.allApplied(30)) {
		t.Fatal("state lost after a full restart from snapshots")
	}
	c.propose("after")
	if !c.runUntil(200, c.allApplied(31)) {
		t.Fatal("cluster does not make progress after restarting from snapshots")
	}
	_ = lead
}
