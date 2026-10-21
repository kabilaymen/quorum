package raft

import "testing"

func cfg3(id NodeID) Config {
	return Config{ID: id, Peers: []NodeID{1, 2, 3}, ElectionTick: 10, HeartbeatTick: 2, Seed: 1}
}

// TestVoteSurvivesRestart: a node that voted in term T, crashed and restarted
// from what Ready asked it to persist must not vote for someone else in T.
// Without this, two candidates can both win term T.
func TestVoteSurvivesRestart(t *testing.T) {
	n := NewNode(cfg3(3))
	n.Step(Message{Type: MsgVote, From: 1, To: 3, Term: 1})
	rd := n.Ready()
	if len(rd.Messages) != 1 || !rd.Messages[0].Granted {
		t.Fatalf("expected a granted vote, got %+v", rd.Messages)
	}
	if rd.HardState == nil || rd.HardState.Term != 1 || rd.HardState.Vote != 1 {
		t.Fatalf("Ready must ask to persist term 1 / vote 1, got %+v", rd.HardState)
	}

	restarted := Restart(cfg3(3), *rd.HardState, rd.Entries)
	restarted.Step(Message{Type: MsgVote, From: 2, To: 3, Term: 1})
	for _, m := range restarted.Ready().Messages {
		if m.Type == MsgVoteResp && m.Granted {
			t.Fatal("voted twice in term 1 after a restart")
		}
	}
}

// TestAcknowledgedEntriesArePersisted: entries a follower acknowledged must
// be in the Ready that precedes the acknowledgement.
func TestAcknowledgedEntriesArePersisted(t *testing.T) {
	n := NewNode(cfg3(2))
	n.Step(Message{Type: MsgApp, From: 1, To: 2, Term: 1, PrevIndex: 0, PrevTerm: 0,
		Entries: []Entry{{Term: 1, Index: 1}, {Term: 1, Index: 2, Data: []byte("x")}}})
	rd := n.Ready()
	if len(rd.Entries) != 2 || rd.Entries[1].Index != 2 {
		t.Fatalf("entries to persist = %+v", rd.Entries)
	}
	if len(rd.Messages) != 1 || !rd.Messages[0].Success || rd.Messages[0].Match != 2 {
		t.Fatalf("ack = %+v", rd.Messages)
	}
	// A conflicting append from a new leader rewrites index 2: Ready must hand
	// back the rewritten suffix starting at the truncation point.
	n.Step(Message{Type: MsgApp, From: 3, To: 2, Term: 2, PrevIndex: 1, PrevTerm: 1,
		Entries: []Entry{{Term: 2, Index: 2, Data: []byte("y")}}})
	rd = n.Ready()
	if len(rd.Entries) != 1 || rd.Entries[0].Index != 2 || string(rd.Entries[0].Data) != "y" {
		t.Fatalf("after truncation, entries to persist = %+v", rd.Entries)
	}
}
