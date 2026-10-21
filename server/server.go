// Package server runs a raft.Node in real time: one goroutine owns the node and
// serialises ticks, incoming messages and client proposals, then applies
// committed entries to the kv store and answers the waiting clients.
package server

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kabilaymen/quorum/kv"
	"github.com/kabilaymen/quorum/raft"
	"github.com/kabilaymen/quorum/wal"
)

var (
	ErrNotLeader = errors.New("not the leader")
	// ErrLost: the entry was overwritten by another leader's log. The command
	// was not applied; it is safe to retry.
	ErrLost    = errors.New("proposal lost after leader change")
	ErrStopped = errors.New("server stopped")
)

// Transport moves raft messages between nodes. Delivery is best effort:
// Raft tolerates loss, duplication and reordering.
type Transport interface {
	Send(msgs []raft.Message)
	Receive() <-chan raft.Message
	Close() error
}

type Config struct {
	Raft     raft.Config
	TickRate time.Duration // duration of one raft tick
	DataDir  string        // write-ahead log location; empty = in memory only
	NoSync   bool          // skip fsync (tests only)
}

type Status struct {
	ID          raft.NodeID `json:"id"`
	State       string      `json:"state"`
	Term        uint64      `json:"term"`
	Leader      raft.NodeID `json:"leader"`
	CommitIndex uint64      `json:"commit_index"`
	Applied     uint64      `json:"applied_index"`
	Keys        int         `json:"keys"`
}

type proposal struct {
	data []byte
	done chan outcome
}

type outcome struct {
	res kv.Result
	err error
}

type waiter struct {
	term uint64
	done chan outcome
}

type Server struct {
	cfg   Config
	node  *raft.Node
	store *kv.Store
	tr    Transport
	wal   *wal.WAL
	watch watchers
	m     *metrics

	propc   chan proposal
	stopc   chan struct{}
	stopped sync.WaitGroup
	waiters map[uint64]waiter // log index -> client waiting for it

	status   atomic.Pointer[Status]
	onApply  func(raft.Entry) // test hook
	stopOnce sync.Once
}

func New(cfg Config, tr Transport) (*Server, error) {
	if cfg.TickRate == 0 {
		cfg.TickRate = 10 * time.Millisecond
	}
	var (
		log     *wal.WAL
		hs      raft.HardState
		entries []raft.Entry
	)
	if cfg.DataDir != "" {
		var err error
		if log, hs, entries, err = wal.Open(cfg.DataDir, wal.Options{NoSync: cfg.NoSync}); err != nil {
			return nil, err
		}
	}
	m := newMetrics()
	s := &Server{
		cfg:     cfg,
		m:       m,
		watch:   watchers{m: m},
		wal:     log,
		node:    raft.Restart(cfg.Raft, hs, entries),
		store:   kv.NewStore(),
		tr:      tr,
		propc:   make(chan proposal),
		stopc:   make(chan struct{}),
		waiters: map[uint64]waiter{},
	}
	s.publishStatus()
	return s, nil
}

func (s *Server) Start() {
	s.stopped.Add(1)
	go s.run()
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() {
		close(s.stopc)
		s.stopped.Wait()
		s.tr.Close()
		if s.wal != nil {
			s.wal.Close()
		}
	})
}

func (s *Server) Status() Status { return *s.status.Load() }

func (s *Server) run() {
	defer s.stopped.Done()
	ticker := time.NewTicker(s.cfg.TickRate)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopc:
			for _, w := range s.waiters {
				w.done <- outcome{err: ErrStopped}
			}
			return
		case <-ticker.C:
			s.node.Tick()
		case m := <-s.tr.Receive():
			s.m.msgsRecv.Inc()
			s.node.Step(m)
		case p := <-s.propc:
			s.propose(p)
		}
		// Batch everything already queued before the next Ready: one WAL
		// fsync and one round of messages for many proposals (group commit).
	drain:
		for i := 0; i < 1024; i++ {
			select {
			case m := <-s.tr.Receive():
				s.m.msgsRecv.Inc()
				s.node.Step(m)
			case p := <-s.propc:
				s.propose(p)
			default:
				break drain
			}
		}
		s.handleReady()
	}
}

func (s *Server) propose(p proposal) {
	index, term, ok := s.node.Propose(p.data)
	if !ok {
		p.done <- outcome{err: ErrNotLeader}
		return
	}
	s.waiters[index] = waiter{term: term, done: p.done}
}

func (s *Server) handleReady() {
	rd := s.node.Ready()
	// Persist before sending: once a vote or an append acknowledgement leaves
	// this node, forgetting it after a crash would break Raft's safety.
	if s.wal != nil && (rd.HardState != nil || len(rd.Entries) > 0) {
		start := time.Now()
		if err := s.wal.Save(rd.HardState, rd.Entries); err != nil {
			panic("quorum: cannot persist raft state: " + err.Error())
		}
		s.m.walSync.Observe(time.Since(start).Seconds())
	}
	s.m.msgsSent.Add(float64(len(rd.Messages)))
	s.tr.Send(rd.Messages)
	for _, e := range rd.Committed {
		op, res, err := s.store.Apply(e.Index, e.Data)
		// Only fresh writes carry the entry's own index as version
		// (a deduplicated retry returns the original, older version).
		if err == nil && (op.Type == kv.Put || op.Type == kv.Delete) && res.Version == e.Index {
			s.watch.publish(Event{Type: "inv", Key: op.Key, Version: e.Index})
		}
		if s.onApply != nil {
			s.onApply(e)
		}
		if w, ok := s.waiters[e.Index]; ok {
			delete(s.waiters, e.Index)
			switch {
			case w.term != e.Term:
				w.done <- outcome{err: ErrLost}
			default:
				w.done <- outcome{res: res, err: err}
			}
		}
	}
	s.publishStatus()
}

func (s *Server) publishStatus() {
	defer func() { s.m.observeStatus(*s.status.Load()) }()
	s.status.Store(&Status{
		ID:          s.node.ID(),
		State:       s.node.State().String(),
		Term:        s.node.Term(),
		Leader:      s.node.Leader(),
		CommitIndex: s.node.CommitIndex(),
		Applied:     s.store.AppliedIndex(),
		Keys:        s.store.Len(),
	})
}

// Do replicates op and returns its result once it is committed and applied.
func (s *Server) Do(ctx context.Context, op kv.Op) (res kv.Result, err error) {
	start := time.Now()
	defer func() {
		outcome := "ok"
		switch {
		case errors.Is(err, ErrNotLeader):
			outcome = "not_leader"
		case errors.Is(err, ErrLost):
			outcome = "lost"
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			outcome = "timeout"
		case err != nil:
			outcome = "error"
		}
		s.m.requests.WithLabelValues(string(op.Type), outcome).Observe(time.Since(start).Seconds())
	}()
	p := proposal{data: kv.Encode(op), done: make(chan outcome, 1)}
	select {
	case s.propc <- p:
	case <-s.stopc:
		return kv.Result{}, ErrStopped
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
	select {
	case o := <-p.done:
		return o.res, o.err
	case <-ctx.Done():
		return kv.Result{}, ctx.Err()
	}
}
