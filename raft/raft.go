// Package raft implements the Raft consensus algorithm (Ongaro & Ousterhout, 2014)
// as a deterministic state machine.
//
// A Node never touches the network, the disk or the clock. Time advances through
// Tick(), incoming messages are fed with Step(), and Ready() hands back the
// messages to send and the entries that became committed. This keeps the
// algorithm testable: a test can run a whole cluster in a single goroutine, drop
// or reorder messages, partition nodes and crash them, deterministically from a seed.
package raft

import (
	"fmt"
	"math/rand"
)

type NodeID uint64

type State int

const (
	Follower State = iota
	Candidate
	Leader
	PreCandidate // asking whether it could win, before bumping its term
)

func (s State) String() string {
	return [...]string{"follower", "candidate", "leader", "pre-candidate"}[s]
}

type MsgType int

const (
	MsgVote MsgType = iota
	MsgVoteResp
	MsgApp
	MsgAppResp
	// MsgPreVote carries the term the sender would campaign in (its term + 1)
	// without the sender having moved to it.
	MsgPreVote
	MsgPreVoteResp
)

type Entry struct {
	Term  uint64
	Index uint64
	Data  []byte
}

type Message struct {
	Type MsgType
	From NodeID
	To   NodeID
	Term uint64

	// MsgVote
	LastLogIndex uint64
	LastLogTerm  uint64
	// MsgVoteResp
	Granted bool

	// MsgApp
	PrevIndex uint64
	PrevTerm  uint64
	Entries   []Entry
	Commit    uint64
	// MsgAppResp: on success, the last index now matching the leader;
	// on failure, a hint telling the leader where to retry from.
	Success bool
	Match   uint64
}

// HardState is what must be on stable storage before messages are sent.
type HardState struct {
	Term uint64
	Vote NodeID
}

type Config struct {
	ID    NodeID
	Peers []NodeID // all members, including ID
	// ElectionTick: a follower that hears nothing for a random duration in
	// [ElectionTick, 2*ElectionTick) ticks starts an election.
	ElectionTick  int
	HeartbeatTick int // leader heartbeat period, must be < ElectionTick
	MaxBatch      int // max entries per append message
	Seed          int64
	// CheckQuorum makes a leader step down when it has not heard from a
	// majority for ElectionTick ticks. Without it, a leader isolated in a
	// minority keeps believing it leads (it can never commit, but it keeps
	// serving as a source of truth to anyone who asks it for its state).
	CheckQuorum bool
	// PreVote (Ongaro's thesis, §9.6): before starting an election, a node asks
	// the others whether they would vote for it, without incrementing its
	// term. They say no while they still hear from a leader. A node cut off
	// from the leader, or restarted and not yet reached by it, therefore
	// cannot inflate its term and depose a healthy leader.
	PreVote bool
}

// Ready is the output of the node since the previous call. The caller must
// durably persist HardState and Entries BEFORE sending Messages: a vote or an
// acknowledged entry that is forgotten after a crash breaks Raft's safety.
type Ready struct {
	// HardState is non-nil when term or vote changed.
	HardState *HardState
	// Entries to append to the stable log. If Entries[0].Index is not the next
	// index on disk, the stored log must first be truncated before it.
	Entries   []Entry
	Messages  []Message
	Committed []Entry // to apply to the state machine, in order
}

type Node struct {
	cfg  Config
	rand *rand.Rand

	state State
	term  uint64
	vote  NodeID
	lead  NodeID
	log   []Entry // log[0] is a sentinel; log[i].Index == i

	commit  uint64
	applied uint64
	stable  uint64    // last log index handed to the caller for persistence
	savedHS HardState // last HardState handed to the caller

	votes map[NodeID]bool
	next  map[NodeID]uint64
	match map[NodeID]uint64

	electionElapsed   int
	active            map[NodeID]bool // peers heard from since the last quorum check
	commitDirty       bool            // commit advanced: tell followers at the next Ready
	heartbeatElapsed  int
	randomizedTimeout int

	msgs []Message
}

func NewNode(cfg Config) *Node {
	return Restart(cfg, HardState{}, nil)
}

// Restart rebuilds a node from its persisted state after a crash. Volatile
// state (commit index, role, leader) is lost and relearned from the cluster.
func Restart(cfg Config, hs HardState, entries []Entry) *Node {
	if cfg.MaxBatch == 0 {
		cfg.MaxBatch = 64
	}
	if cfg.HeartbeatTick <= 0 || cfg.HeartbeatTick >= cfg.ElectionTick {
		panic("raft: need 0 < HeartbeatTick < ElectionTick")
	}
	n := &Node{
		cfg:  cfg,
		rand: rand.New(rand.NewSource(cfg.Seed ^ int64(cfg.ID))),
		term: hs.Term,
		vote: hs.Vote,
		log:  append([]Entry{{}}, entries...),
	}
	n.stable = n.LastIndex()
	n.savedHS = hs
	n.becomeFollower(n.term, 0)
	return n
}

func (n *Node) ID() NodeID           { return n.cfg.ID }
func (n *Node) State() State         { return n.state }
func (n *Node) Term() uint64         { return n.term }
func (n *Node) Leader() NodeID       { return n.lead }
func (n *Node) CommitIndex() uint64  { return n.commit }
func (n *Node) LastIndex() uint64    { return uint64(len(n.log) - 1) }
func (n *Node) AppliedIndex() uint64 { return n.applied }

// Persistent returns a copy of the state a real deployment writes to disk.
func (n *Node) Persistent() (HardState, []Entry) {
	return HardState{Term: n.term, Vote: n.vote}, append([]Entry(nil), n.log[1:]...)
}

func (n *Node) termAt(i uint64) uint64 { return n.log[i].Term }
func (n *Node) lastTerm() uint64       { return n.log[len(n.log)-1].Term }
func (n *Node) quorum() int            { return len(n.cfg.Peers)/2 + 1 }

func (n *Node) send(m Message) {
	m.From = n.cfg.ID
	if m.Term == 0 {
		m.Term = n.term // pre-votes and granted pre-vote responses set their own
	}
	n.msgs = append(n.msgs, m)
}

// heardFromLeader reports whether a leader was heard from less than a
// minimum election timeout ago. A leader counts itself.
func (n *Node) heardFromLeader() bool {
	return n.lead != 0 && n.electionElapsed < n.cfg.ElectionTick
}

// ---------------------------------------------------------------- roles

func (n *Node) resetElectionTimer() {
	n.electionElapsed = 0
	n.randomizedTimeout = n.cfg.ElectionTick + n.rand.Intn(n.cfg.ElectionTick)
}

func (n *Node) becomeFollower(term uint64, lead NodeID) {
	if term > n.term {
		n.term = term
		n.vote = 0
	}
	n.state = Follower
	n.lead = lead
	n.resetElectionTimer()
}

func (n *Node) campaign() {
	if n.cfg.PreVote {
		n.becomePreCandidate()
	} else {
		n.becomeCandidate()
	}
}

// becomePreCandidate polls the cluster without touching term or vote, so
// nothing needs to be persisted and nobody is disturbed if the poll fails.
func (n *Node) becomePreCandidate() {
	n.state = PreCandidate
	n.lead = 0
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	if n.quorum() == 1 {
		n.becomeCandidate()
		return
	}
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(Message{Type: MsgPreVote, To: p, Term: n.term + 1, LastLogIndex: n.LastIndex(), LastLogTerm: n.lastTerm()})
		}
	}
}

func (n *Node) becomeCandidate() {
	n.state = Candidate
	n.term++
	n.vote = n.cfg.ID
	n.lead = 0
	n.votes = map[NodeID]bool{n.cfg.ID: true}
	n.resetElectionTimer()
	if n.quorum() == 1 {
		n.becomeLeader()
		return
	}
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.send(Message{Type: MsgVote, To: p, LastLogIndex: n.LastIndex(), LastLogTerm: n.lastTerm()})
		}
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.lead = n.cfg.ID
	n.heartbeatElapsed = 0
	n.electionElapsed = 0
	n.active = map[NodeID]bool{}
	n.next = make(map[NodeID]uint64)
	n.match = make(map[NodeID]uint64)
	for _, p := range n.cfg.Peers {
		n.next[p] = n.LastIndex() + 1
		n.match[p] = 0
	}
	// A leader may only count replicas for entries of its own term (§5.4.2).
	// Appending an empty entry lets it commit everything before it right away.
	n.appendEntry(nil)
	n.broadcastAppend()
	n.maybeCommit()
}

// ---------------------------------------------------------------- inputs

// Tick advances logical time by one unit.
func (n *Node) Tick() {
	if n.state == Leader {
		n.heartbeatElapsed++
		if n.heartbeatElapsed >= n.cfg.HeartbeatTick {
			n.heartbeatElapsed = 0
			n.broadcastAppend()
		}
		if n.cfg.CheckQuorum {
			n.electionElapsed++
			if n.electionElapsed >= n.cfg.ElectionTick {
				n.electionElapsed = 0
				if len(n.active)+1 < n.quorum() {
					n.becomeFollower(n.term, 0)
					return
				}
				n.active = map[NodeID]bool{}
			}
		}
		return
	}
	n.electionElapsed++
	if n.electionElapsed >= n.randomizedTimeout {
		n.campaign()
	}
}

// Propose appends a command to the replicated log. It only succeeds on the
// leader; the returned index/term identify the entry, which may still be lost
// if this leader is deposed before the entry is committed.
func (n *Node) Propose(data []byte) (index, term uint64, ok bool) {
	if n.state != Leader {
		return 0, 0, false
	}
	e := n.appendEntry(data)
	n.broadcastAppend()
	n.maybeCommit()
	return e.Index, e.Term, true
}

func (n *Node) appendEntry(data []byte) Entry {
	e := Entry{Term: n.term, Index: n.LastIndex() + 1, Data: data}
	n.log = append(n.log, e)
	n.match[n.cfg.ID] = e.Index
	n.next[n.cfg.ID] = e.Index + 1
	return e
}

// Step processes one incoming message.
func (n *Node) Step(m Message) {
	switch {
	case m.Term > n.term:
		if (m.Type == MsgVote || m.Type == MsgPreVote) && n.cfg.CheckQuorum && n.heardFromLeader() {
			// The leader is alive as far as we know (and with CheckQuorum it
			// would have stepped down otherwise): this candidate is most likely
			// a node coming back from a partition or a restart. Ignoring it
			// keeps it from deposing a healthy leader.
			return
		}
		switch {
		case m.Type == MsgPreVote:
			// A pre-vote is a question about a future term: ours does not change.
		case m.Type == MsgPreVoteResp && m.Granted:
			// A granted pre-vote echoes the term the pre-candidate asked for.
		default:
			lead := NodeID(0)
			if m.Type == MsgApp {
				lead = m.From
			}
			n.becomeFollower(m.Term, lead)
		}
	case m.Term < n.term:
		// Tell stale candidates and leaders about the newer term so they step down.
		switch m.Type {
		case MsgVote:
			n.send(Message{Type: MsgVoteResp, To: m.From, Granted: false})
		case MsgPreVote:
			n.send(Message{Type: MsgPreVoteResp, To: m.From, Granted: false})
		case MsgApp:
			n.send(Message{Type: MsgAppResp, To: m.From, Success: false, Match: n.LastIndex()})
		}
		return
	}

	switch m.Type {
	case MsgVote:
		n.handleVote(m)
	case MsgPreVote:
		n.handlePreVote(m)
	case MsgVoteResp, MsgPreVoteResp:
		n.handleVoteResp(m)
	case MsgApp:
		if n.state == Leader {
			panic(fmt.Sprintf("raft: two leaders in term %d (%d and %d)", n.term, n.cfg.ID, m.From))
		}
		n.becomeFollower(m.Term, m.From)
		n.handleAppend(m)
	case MsgAppResp:
		if n.state == Leader {
			n.handleAppendResp(m)
		}
	}
}

// Ready drains the outgoing messages and the newly committed entries.
func (n *Node) Ready() Ready {
	if n.commitDirty && n.state == Leader {
		n.broadcastAppend() // let followers learn the new commit index quickly
	}
	n.commitDirty = false
	rd := Ready{Messages: n.msgs}
	n.msgs = nil
	if hs := (HardState{Term: n.term, Vote: n.vote}); hs != n.savedHS {
		rd.HardState = &hs
		n.savedHS = hs
	}
	if n.LastIndex() > n.stable {
		rd.Entries = append([]Entry(nil), n.log[n.stable+1:]...)
		n.stable = n.LastIndex()
	}
	if n.commit > n.applied {
		rd.Committed = append([]Entry(nil), n.log[n.applied+1:n.commit+1]...)
		n.applied = n.commit
	}
	return rd
}

// ---------------------------------------------------------------- election

// upToDate implements the election restriction (§5.4.1): only vote for a
// candidate whose log is at least as up to date as ours.
func (n *Node) upToDate(lastTerm, lastIndex uint64) bool {
	return lastTerm > n.lastTerm() || (lastTerm == n.lastTerm() && lastIndex >= n.LastIndex())
}

func (n *Node) handleVote(m Message) {
	grant := (n.vote == 0 || n.vote == m.From) && n.upToDate(m.LastLogTerm, m.LastLogIndex)
	if grant {
		n.vote = m.From
		n.resetElectionTimer()
	}
	n.send(Message{Type: MsgVoteResp, To: m.From, Granted: grant})
}

// handlePreVote answers "would you vote for me in term m.Term?". Nothing is
// recorded: a pre-vote is a question, not a promise.
func (n *Node) handlePreVote(m Message) {
	grant := m.Term > n.term && !n.heardFromLeader() && n.upToDate(m.LastLogTerm, m.LastLogIndex)
	resp := Message{Type: MsgPreVoteResp, To: m.From, Granted: grant}
	if grant {
		resp.Term = m.Term
	}
	n.send(resp)
}

func (n *Node) handleVoteResp(m Message) {
	want := Candidate
	if m.Type == MsgPreVoteResp {
		want = PreCandidate
		if m.Granted && m.Term != n.term+1 {
			return // granted for an older poll
		}
	}
	if n.state != want {
		return
	}
	n.votes[m.From] = m.Granted
	granted, rejected := 0, 0
	for _, v := range n.votes {
		if v {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= n.quorum() && want == PreCandidate:
		n.becomeCandidate()
	case granted >= n.quorum():
		n.becomeLeader()
	case rejected >= n.quorum():
		n.becomeFollower(n.term, 0)
	}
}

// ---------------------------------------------------------------- replication

func (n *Node) broadcastAppend() {
	for _, p := range n.cfg.Peers {
		if p != n.cfg.ID {
			n.sendAppend(p)
		}
	}
}

func (n *Node) sendAppend(to NodeID) {
	next := n.next[to]
	prev := next - 1
	end := min(n.LastIndex()+1, next+uint64(n.cfg.MaxBatch))
	entries := append([]Entry(nil), n.log[next:end]...)
	n.send(Message{Type: MsgApp, To: to, PrevIndex: prev, PrevTerm: n.termAt(prev), Entries: entries, Commit: n.commit})
	// Pipelining: assume the entries will arrive and continue from there,
	// instead of resending the whole unacknowledged window on every proposal.
	// If the message is lost, the next append (at worst the next heartbeat)
	// is rejected and the follower's hint moves next back.
	n.next[to] = end
}

func (n *Node) handleAppend(m Message) {
	if m.PrevIndex > n.LastIndex() {
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, Match: n.LastIndex()})
		return
	}
	if n.termAt(m.PrevIndex) != m.PrevTerm {
		// Skip back over the whole conflicting term instead of one entry per round trip.
		conflictTerm := n.termAt(m.PrevIndex)
		i := m.PrevIndex
		for i > n.commit+1 && n.termAt(i-1) == conflictTerm {
			i--
		}
		n.send(Message{Type: MsgAppResp, To: m.From, Success: false, Match: i - 1})
		return
	}
	for _, e := range m.Entries {
		if e.Index <= n.LastIndex() {
			if n.termAt(e.Index) == e.Term {
				continue // already have it
			}
			if e.Index <= n.commit {
				panic(fmt.Sprintf("raft: node %d asked to overwrite committed entry %d", n.cfg.ID, e.Index))
			}
			n.log = n.log[:e.Index] // drop the conflicting suffix
			n.stable = min(n.stable, e.Index-1)
		}
		n.log = append(n.log, e)
	}
	lastNew := m.PrevIndex + uint64(len(m.Entries))
	if m.Commit > n.commit {
		n.commit = min(m.Commit, lastNew)
	}
	n.send(Message{Type: MsgAppResp, To: m.From, Success: true, Match: lastNew})
}

func (n *Node) handleAppendResp(m Message) {
	n.active[m.From] = true
	if !m.Success {
		// never go back below what the follower is known to hold
		n.next[m.From] = max(n.match[m.From]+1, min(n.next[m.From]-1, m.Match+1))
		n.sendAppend(m.From)
		return
	}
	if m.Match > n.match[m.From] {
		n.match[m.From] = m.Match
	}
	if m.Match+1 > n.next[m.From] {
		n.next[m.From] = m.Match + 1
	}
	n.maybeCommit()
	if n.next[m.From] <= n.LastIndex() {
		n.sendAppend(m.From) // keep streaming the backlog
	}
}

// maybeCommit advances the commit index to the highest entry of the current
// term stored on a majority.
func (n *Node) maybeCommit() {
	for i := n.LastIndex(); i > n.commit; i-- {
		if n.termAt(i) != n.term {
			break
		}
		replicas := 0
		for _, p := range n.cfg.Peers {
			if n.match[p] >= i {
				replicas++
			}
		}
		if replicas >= n.quorum() {
			n.commit = i
			n.commitDirty = true // coalesced: one broadcast per Ready, not per ack
			return
		}
	}
}
