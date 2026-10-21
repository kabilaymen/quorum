package server

import (
	"sync"

	"github.com/kabilaymen/quorum/raft"
)

// MemNetwork connects in-process servers; links can be cut to simulate partitions.
type MemNetwork struct {
	mu    sync.Mutex
	boxes map[raft.NodeID]chan raft.Message
	cut   map[[2]raft.NodeID]bool
}

func NewMemNetwork() *MemNetwork {
	return &MemNetwork{boxes: map[raft.NodeID]chan raft.Message{}, cut: map[[2]raft.NodeID]bool{}}
}

func (n *MemNetwork) Join(id raft.NodeID) Transport {
	n.mu.Lock()
	defer n.mu.Unlock()
	box := make(chan raft.Message, 4096)
	n.boxes[id] = box
	return &memTransport{net: n, id: id, box: box}
}

func (n *MemNetwork) Isolate(id raft.NodeID, isolated bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.boxes {
		n.cut[[2]raft.NodeID{id, other}] = isolated
		n.cut[[2]raft.NodeID{other, id}] = isolated
	}
}

type memTransport struct {
	net *MemNetwork
	id  raft.NodeID
	box chan raft.Message
}

func (t *memTransport) Send(msgs []raft.Message) {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	for _, m := range msgs {
		if t.net.cut[[2]raft.NodeID{m.From, m.To}] {
			continue
		}
		if box, ok := t.net.boxes[m.To]; ok {
			select {
			case box <- m:
			default: // receiver overloaded: drop, like a congested network
			}
		}
	}
}

func (t *memTransport) Receive() <-chan raft.Message { return t.box }

func (t *memTransport) Close() error {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	delete(t.net.boxes, t.id)
	return nil
}
