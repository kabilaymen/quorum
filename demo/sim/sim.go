// Package sim runs a whole Quorum cluster in one goroutine, for the browser
// demo: the real raft.Node and kv.Store, a simulated network that can lose,
// delay and cut messages, and a simulated clock. Nothing here touches the
// network, the disk or the wall clock, so a run replays exactly from its seed.
//
// After every step it checks Raft's two safety properties, as the
// fault-injection tests do: at most one leader per term, and no two nodes
// applying different entries at the same index.
package sim

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math/rand"
	"sort"

	"github.com/kabilaymen/quorum/kv"
	"github.com/kabilaymen/quorum/raft"
)

// TickMillis is the simulated duration of one raft tick, as in cmd/quorum.
const TickMillis = 10

type Config struct {
	Nodes         int
	Seed          int64
	DropRate      float64 // probability of losing a message
	MaxDelayTicks int     // a message takes 1 to 1+MaxDelayTicks ticks
	SnapshotEvery uint64  // compact the log every N applied entries
	MaxEvents     int     // events kept for the feed
}

func DefaultConfig(seed int64) Config {
	return Config{Nodes: 5, Seed: seed, MaxDelayTicks: 2, SnapshotEvery: 200, MaxEvents: 40}
}

// disk is what a node persisted through the Ready contract; a restart only
// sees this.
type disk struct {
	hs      raft.HardState
	snap    raft.Snapshot
	entries []raft.Entry // the log after snap.Index
}

type inflight struct {
	m       raft.Message
	deliver int
}

type applied struct {
	term uint64
	hash uint64
}

type Sim struct {
	cfg   Config
	rand  *rand.Rand
	ids   []raft.NodeID
	nodes map[raft.NodeID]*raft.Node
	store map[raft.NodeID]*kv.Store
	disk  map[raft.NodeID]*disk
	down  map[raft.NodeID]bool
	cut   map[raft.NodeID]bool // isolated nodes: every link to and from them drops
	inbox []inflight

	tick      int
	leaders   map[uint64]raft.NodeID // term -> leader (Election Safety)
	committed map[uint64]applied     // index -> what the first node applied there
	lastLead  raft.NodeID
	noLeader  int // tick at which the cluster last lost its leader, -1 if it has one
	hadLeader bool
	violation string
	checks    int

	delivered, dropped int
	flows              map[Flow]int // message kinds seen per link, with the tick of the last one
	snapshots          int
	puts               uint64
	events             []Event
}

type Event struct {
	Millis int    `json:"ms"`
	Kind   string `json:"kind"`
	Text   string `json:"text"`
}

func New(cfg Config) *Sim {
	s := &Sim{
		cfg: cfg, rand: rand.New(rand.NewSource(cfg.Seed)),
		nodes: map[raft.NodeID]*raft.Node{}, store: map[raft.NodeID]*kv.Store{},
		disk: map[raft.NodeID]*disk{}, down: map[raft.NodeID]bool{}, cut: map[raft.NodeID]bool{},
		leaders: map[uint64]raft.NodeID{}, committed: map[uint64]applied{}, noLeader: 0, flows: map[Flow]int{},
	}
	for i := 1; i <= cfg.Nodes; i++ {
		s.ids = append(s.ids, raft.NodeID(i))
	}
	for _, id := range s.ids {
		s.disk[id] = &disk{}
		s.nodes[id] = raft.NewNode(s.raftConfig(id))
		s.store[id] = kv.NewStore()
	}
	s.event("info", fmt.Sprintf("%d nodes started, no leader yet", cfg.Nodes))
	return s
}

func (s *Sim) raftConfig(id raft.NodeID) raft.Config {
	return raft.Config{
		ID: id, Peers: s.ids, ElectionTick: 15, HeartbeatTick: 3, // 150 to 300 ms at 10 ms ticks
		Seed: s.cfg.Seed + int64(s.tick)*31, CheckQuorum: true, PreVote: true,
	}
}

func (s *Sim) Millis() int { return s.tick * TickMillis }

func (s *Sim) event(kind, text string) {
	s.events = append(s.events, Event{Millis: s.Millis(), Kind: kind, Text: text})
	if n := s.cfg.MaxEvents; n > 0 && len(s.events) > n {
		s.events = s.events[len(s.events)-n:]
	}
}

// Step advances the cluster by n ticks. It stops early, and returns false,
// once a safety violation has been found (it should never happen).
func (s *Sim) Step(n int) bool {
	for i := 0; i < n && s.violation == ""; i++ {
		s.tick++
		for _, id := range s.ids {
			if !s.down[id] {
				s.nodes[id].Tick()
			}
		}
		s.collect()
		var due, later []inflight
		for _, f := range s.inbox {
			if f.deliver <= s.tick {
				due = append(due, f)
			} else {
				later = append(later, f)
			}
		}
		s.inbox = later
		s.rand.Shuffle(len(due), func(i, j int) { due[i], due[j] = due[j], due[i] })
		for _, f := range due {
			if s.down[f.m.To] || s.cut[f.m.To] || s.cut[f.m.From] {
				s.dropped++
				continue
			}
			s.delivered++
			s.flows[Flow{From: f.m.From, To: f.m.To, Kind: kindOf(f.m.Type)}] = s.tick
			s.nodes[f.m.To].Step(f.m)
		}
		s.collect()
		s.check()
	}
	return s.violation == ""
}

// collect drains every live node's Ready: persist first, then send, then
// apply, exactly in the order the real server does.
func (s *Sim) collect() {
	for _, id := range s.ids {
		if s.down[id] {
			continue
		}
		n, d, st := s.nodes[id], s.disk[id], s.store[id]
		rd := n.Ready()
		if sn := rd.Snapshot; sn != nil {
			// Same rule as the node: keep the stored suffix only if it holds the
			// snapshot's last entry with the same term.
			pos := int(sn.Index) - int(d.snap.Index) - 1
			if pos >= 0 && pos < len(d.entries) && d.entries[pos].Term == sn.Term {
				d.entries = append([]raft.Entry(nil), d.entries[pos+1:]...)
			} else {
				d.entries = nil
			}
			d.snap = *sn
			if err := st.Restore(sn.Data); err != nil {
				s.fail(fmt.Sprintf("node %d cannot load a snapshot: %v", id, err))
				return
			}
			s.snapshots++
			s.event("snapshot", fmt.Sprintf("node %d caught up from a snapshot at index %d", id, sn.Index))
		}
		if rd.HardState != nil {
			d.hs = *rd.HardState
		}
		if len(rd.Entries) > 0 {
			pos := rd.Entries[0].Index - d.snap.Index - 1
			d.entries = append(d.entries[:pos], rd.Entries...)
		}
		for _, m := range rd.Messages {
			if s.rand.Float64() < s.cfg.DropRate {
				s.dropped++
				continue
			}
			lag := 0
			if s.cfg.MaxDelayTicks > 0 {
				lag = s.rand.Intn(s.cfg.MaxDelayTicks + 1)
			}
			s.inbox = append(s.inbox, inflight{m: m, deliver: s.tick + 1 + lag})
		}
		for _, e := range rd.Committed {
			if _, _, err := st.Apply(e.Index, e.Data); err != nil {
				s.fail(fmt.Sprintf("node %d cannot apply entry %d: %v", id, e.Index, err))
				return
			}
			s.checkApplied(id, e)
		}
		if every := s.cfg.SnapshotEvery; every > 0 && st.AppliedIndex() >= n.FirstIndex()-1+every {
			sn, err := n.Compact(st.AppliedIndex(), st.Snapshot())
			if err != nil {
				s.fail(fmt.Sprintf("node %d cannot compact: %v", id, err))
				return
			}
			pos := sn.Index - d.snap.Index - 1
			d.entries = append([]raft.Entry(nil), d.entries[pos+1:]...)
			d.snap = sn
		}
	}
}

func hashData(b []byte) uint64 {
	h := fnv.New64a()
	h.Write(b)
	return h.Sum64()
}

// checkApplied is State Machine Safety: every node applies the same entry at
// a given index.
func (s *Sim) checkApplied(id raft.NodeID, e raft.Entry) {
	got := applied{term: e.Term, hash: hashData(e.Data)}
	if ref, ok := s.committed[e.Index]; !ok {
		s.committed[e.Index] = got
	} else if ref != got {
		s.fail(fmt.Sprintf("node %d applied a different entry at index %d", id, e.Index))
	}
}

// check is Election Safety (at most one leader per term), run after every
// step, plus the bookkeeping for the event feed.
func (s *Sim) check() {
	s.checks++
	lead := raft.NodeID(0)
	for _, id := range s.ids {
		n := s.nodes[id]
		if s.down[id] || n.State() != raft.Leader {
			continue
		}
		if other, ok := s.leaders[n.Term()]; ok && other != id {
			s.fail(fmt.Sprintf("two leaders in term %d: %d and %d", n.Term(), other, id))
			return
		}
		s.leaders[n.Term()] = id
		// The leader that a majority follows is the one with the highest term.
		if lead == 0 || n.Term() > s.nodes[lead].Term() {
			lead = id
		}
	}
	if lead != 0 && s.isolated(lead) {
		lead = 0 // an isolated "leader" serves nobody until CheckQuorum demotes it
	}
	switch {
	case lead != 0 && lead != s.lastLead:
		text := fmt.Sprintf("node %d became leader in term %d", lead, s.nodes[lead].Term())
		switch {
		case !s.hadLeader:
			text += fmt.Sprintf(", %d ms after start", s.Millis())
		case s.noLeader >= 0:
			text += fmt.Sprintf(", %d ms after the cluster lost its leader", (s.tick-s.noLeader)*TickMillis)
		}
		s.event("leader", text)
		s.noLeader = -1
		s.hadLeader = true
	case lead == 0 && s.lastLead != 0:
		s.noLeader = s.tick
	}
	s.lastLead = lead
}

func (s *Sim) isolated(id raft.NodeID) bool { return s.cut[id] }

func (s *Sim) fail(msg string) {
	if s.violation == "" {
		s.violation = msg
		s.event("violation", "SAFETY VIOLATION: "+msg)
	}
}

func (s *Sim) valid(id raft.NodeID) bool { return id >= 1 && int(id) <= len(s.ids) }

// Kill crashes a node: it stops taking steps, and loses everything that is
// not on its disk.
func (s *Sim) Kill(id raft.NodeID) {
	if !s.valid(id) || s.down[id] {
		return
	}
	s.down[id] = true
	s.event("kill", fmt.Sprintf("node %d was killed", id))
}

// Restart brings a killed node back from what it persisted.
func (s *Sim) Restart(id raft.NodeID) {
	if !s.valid(id) || !s.down[id] {
		return
	}
	d := s.disk[id]
	st := kv.NewStore()
	if d.snap.Index > 0 {
		if err := st.Restore(d.snap.Data); err != nil {
			s.fail(fmt.Sprintf("node %d cannot restore its snapshot: %v", id, err))
			return
		}
	}
	s.store[id] = st
	s.nodes[id] = raft.RestartFromSnapshot(s.raftConfig(id), d.hs, d.snap, append([]raft.Entry(nil), d.entries...))
	s.down[id] = false
	s.event("restart", fmt.Sprintf("node %d restarted from its disk (term %d, vote %d, %d log entries)", id, d.hs.Term, d.hs.Vote, len(d.entries)))
}

// Isolate cuts every link to and from a node (or restores them).
func (s *Sim) Isolate(id raft.NodeID, on bool) {
	if !s.valid(id) || s.cut[id] == on {
		return
	}
	s.cut[id] = on
	if on {
		s.event("partition", fmt.Sprintf("node %d was cut from the network", id))
	} else {
		s.event("heal", fmt.Sprintf("node %d is reachable again", id))
	}
}

// Heal restores every link.
func (s *Sim) Heal() {
	for _, id := range s.ids {
		s.Isolate(id, false)
	}
}

func (s *Sim) SetDropRate(p float64) {
	s.cfg.DropRate = max(0, min(p, 0.5))
}

// Leader returns the node a client would reach: the live, connected leader
// with the highest term (0 if none).
func (s *Sim) Leader() raft.NodeID { return s.lastLead }

// Put proposes a write to the current leader. It returns false when there is
// no leader to take it; a proposed write may still be lost if that leader is
// deposed before it commits, as in the real system.
func (s *Sim) Put(key, value string) bool {
	lead := s.lastLead
	if lead == 0 {
		s.event("put", fmt.Sprintf("put(%s) refused: no leader right now", key))
		return false
	}
	s.puts++
	index, term, ok := s.nodes[lead].Propose(kv.Encode(kv.Op{Type: kv.Put, Key: key, Value: value, Client: "demo", Seq: s.puts}))
	if !ok {
		return false
	}
	s.event("put", fmt.Sprintf("put(%s=%s) proposed to node %d at index %d, term %d", key, value, lead, index, term))
	return true
}

// ---------------------------------------------------------------- state for the page

// Flow is a link that carried a message recently, for the cluster diagram.
type Flow struct {
	From raft.NodeID `json:"from"`
	To   raft.NodeID `json:"to"`
	Kind string      `json:"kind"` // "append" (entries and heartbeats), "vote" or "snapshot"
}

// FlowTicks is how long a link stays drawn after its last message.
const FlowTicks = 8

func kindOf(t raft.MsgType) string {
	switch t {
	case raft.MsgVote, raft.MsgVoteResp, raft.MsgPreVote, raft.MsgPreVoteResp:
		return "vote"
	case raft.MsgSnap:
		return "snapshot"
	default:
		return "append"
	}
}

func plural(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

type LogEntry struct {
	Index uint64 `json:"index"`
	Term  uint64 `json:"term"`
	What  string `json:"what"`
}

type NodeState struct {
	ID         raft.NodeID       `json:"id"`
	Alive      bool              `json:"alive"`
	Isolated   bool              `json:"isolated"`
	Role       string            `json:"role"`
	Term       uint64            `json:"term"`
	Leader     raft.NodeID       `json:"leader"`
	Commit     uint64            `json:"commit"`
	FirstIndex uint64            `json:"firstIndex"`
	LastIndex  uint64            `json:"lastIndex"`
	Tail       []LogEntry        `json:"tail"`
	Keys       map[string]string `json:"keys"`
	Disk       string            `json:"disk"`
}

type State struct {
	Millis     int         `json:"ms"`
	Leader     raft.NodeID `json:"leader"`
	Nodes      []NodeState `json:"nodes"`
	Events     []Event     `json:"events"`
	Checks     int         `json:"checks"`
	Violation  string      `json:"violation"`
	Delivered  int         `json:"delivered"`
	Dropped    int         `json:"dropped"`
	DropRate   float64     `json:"dropRate"`
	Snapshots  int         `json:"snapshots"`
	Seed       int64       `json:"seed"`
	Puts       uint64      `json:"puts"`
	TickMillis int         `json:"tickMs"`
	Flows      []Flow      `json:"flows"`
}

func describe(e raft.Entry) string {
	if len(e.Data) == 0 {
		return "no-op"
	}
	op, err := kv.Decode(e.Data)
	if err != nil {
		return "?"
	}
	return fmt.Sprintf("%s %s=%s", op.Type, op.Key, op.Value)
}

func (s *Sim) State() State {
	st := State{Millis: s.Millis(), Leader: s.lastLead, Events: append([]Event{}, s.events...),
		Checks: s.checks, Violation: s.violation, Delivered: s.delivered, Dropped: s.dropped,
		DropRate: s.cfg.DropRate, Snapshots: s.snapshots, Seed: s.cfg.Seed, Puts: s.puts, TickMillis: TickMillis}
	for _, id := range s.ids {
		n, d := s.nodes[id], s.disk[id]
		ns := NodeState{ID: id, Alive: !s.down[id], Isolated: s.cut[id], Role: n.State().String(),
			Term: n.Term(), Leader: n.Leader(), Commit: n.CommitIndex(), FirstIndex: n.FirstIndex(),
			LastIndex: n.LastIndex(), Keys: map[string]string{},
			Disk: fmt.Sprintf("term %d, voted for %d, snapshot up to index %d, %s after it", d.hs.Term, d.hs.Vote, d.snap.Index, plural(len(d.entries), "entry", "entries"))}
		if s.down[id] {
			ns.Role = "down"
		}
		// The tail of the log as persisted: what the node would restart with.
		from := 0
		if len(d.entries) > 6 {
			from = len(d.entries) - 6
		}
		for _, e := range d.entries[from:] {
			ns.Tail = append(ns.Tail, LogEntry{Index: e.Index, Term: e.Term, What: describe(e)})
		}
		ns.Keys = s.keys(id)
		st.Nodes = append(st.Nodes, ns)
	}
	st.Flows = []Flow{}
	for f, at := range s.flows {
		if s.tick-at <= FlowTicks {
			st.Flows = append(st.Flows, f)
		}
	}
	sort.Slice(st.Flows, func(i, j int) bool {
		a, b := st.Flows[i], st.Flows[j]
		if a.From != b.From {
			return a.From < b.From
		}
		if a.To != b.To {
			return a.To < b.To
		}
		return a.Kind < b.Kind
	})
	return st
}

// keys reads a node's key-value map through its snapshot form (the store does
// not export its map otherwise).
func (s *Sim) keys(id raft.NodeID) map[string]string {
	var snap struct {
		Data map[string]struct {
			Value string `json:"v"`
		} `json:"data"`
	}
	out := map[string]string{}
	if err := json.Unmarshal(s.store[id].Snapshot(), &snap); err != nil {
		return out
	}
	names := make([]string, 0, len(snap.Data))
	for k := range snap.Data {
		names = append(names, k)
	}
	sort.Strings(names)
	for _, k := range names {
		out[k] = snap.Data[k].Value
	}
	return out
}
