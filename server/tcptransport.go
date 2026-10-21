package server

import (
	"encoding/gob"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/kabilaymen/quorum/raft"
)

// TCPTransport streams gob-encoded raft messages over one long-lived TCP
// connection per peer. Each peer has a bounded outgoing queue: when a peer is
// slow or unreachable, messages are dropped instead of blocking the raft loop
// (Raft retransmits through heartbeats anyway).
type TCPTransport struct {
	id    raft.NodeID
	ln    net.Listener
	recv  chan raft.Message
	peers map[raft.NodeID]*peer
	log   *slog.Logger
	done  chan struct{}
	wg    sync.WaitGroup
}

type peer struct {
	addr  string
	queue chan raft.Message
}

func NewTCPTransport(id raft.NodeID, listen string, peers map[raft.NodeID]string, log *slog.Logger) (*TCPTransport, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	t := &TCPTransport{id: id, ln: ln, recv: make(chan raft.Message, 4096), peers: map[raft.NodeID]*peer{}, log: log, done: make(chan struct{})}
	for pid, addr := range peers {
		if pid == id {
			continue
		}
		p := &peer{addr: addr, queue: make(chan raft.Message, 1024)}
		t.peers[pid] = p
		t.wg.Add(1)
		go t.sendLoop(pid, p)
	}
	t.wg.Add(1)
	go t.acceptLoop()
	return t, nil
}

func (t *TCPTransport) Send(msgs []raft.Message) {
	for _, m := range msgs {
		if p, ok := t.peers[m.To]; ok {
			select {
			case p.queue <- m:
			default:
			}
		}
	}
}

func (t *TCPTransport) Receive() <-chan raft.Message { return t.recv }

func (t *TCPTransport) Close() error {
	close(t.done)
	err := t.ln.Close()
	t.wg.Wait()
	return err
}

func (t *TCPTransport) sendLoop(id raft.NodeID, p *peer) {
	defer t.wg.Done()
	var conn net.Conn
	var enc *gob.Encoder
	// Outage bookkeeping, logged once when the peer becomes unreachable and
	// once when it is back, so failovers can be lined up with elections.
	var failedDials int
	var downSince time.Time
	for {
		select {
		case <-t.done:
			if conn != nil {
				conn.Close()
			}
			return
		case m := <-p.queue:
			if conn == nil {
				c, err := net.DialTimeout("tcp", p.addr, 500*time.Millisecond)
				if err != nil {
					if failedDials == 0 {
						downSince = time.Now()
						t.log.Warn("peer unreachable", "peer", id, "err", err)
					}
					failedDials++
					continue // peer down: drop, retry on the next message
				}
				if failedDials > 0 {
					t.log.Info("peer reconnected", "peer", id, "failed_dials", failedDials, "down_for", time.Since(downSince).Round(time.Millisecond), "queued", len(p.queue))
					failedDials = 0
				}
				conn, enc = c, gob.NewEncoder(c)
			}
			conn.SetWriteDeadline(time.Now().Add(time.Second))
			if err := enc.Encode(&m); err != nil {
				t.log.Info("send failed, closing connection", "peer", id, "err", err)
				conn.Close()
				conn = nil
			}
		}
	}
}

func (t *TCPTransport) acceptLoop() {
	defer t.wg.Done()
	for {
		conn, err := t.ln.Accept()
		if err != nil {
			return // listener closed
		}
		t.wg.Add(1)
		go t.readLoop(conn)
	}
}

func (t *TCPTransport) readLoop(conn net.Conn) {
	defer t.wg.Done()
	defer conn.Close()
	go func() { <-t.done; conn.Close() }()
	dec := gob.NewDecoder(conn)
	for {
		var m raft.Message
		if err := dec.Decode(&m); err != nil {
			return
		}
		select {
		case t.recv <- m:
		case <-t.done:
			return
		}
	}
}
