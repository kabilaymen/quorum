package server

import (
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/kabilaymen/quorum/raft"
)

// TestTransportRedialsAfterPeerRestart reproduces what was seen on the demo
// VM: a peer is killed and restarted on the same address, and the sender's
// old connection gives no write error (the dead peer's FIN arrived, but no
// reset ever does). The sender must notice the close and redial, instead of
// waiting for TCP's backed-off retransmissions to fail.
func TestTransportRedialsAfterPeerRestart(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))

	// The first incarnation of node 2: accepts the connection, half-closes it
	// (FIN) and then never reads or resets it, like a process whose host
	// stopped answering.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			accepted <- c
		}
	}()

	a, err := NewTCPTransport(1, "127.0.0.1:0", map[raft.NodeID]string{1: "unused", 2: addr}, quiet)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.Send([]raft.Message{{Type: raft.MsgApp, From: 1, To: 2}})

	var old net.Conn
	select {
	case old = <-accepted:
	case <-time.After(2 * time.Second):
		t.Fatal("first connection never arrived")
	}
	defer old.Close()
	old.(*net.TCPConn).CloseWrite()
	ln.Close()

	// The second incarnation, on the same address.
	var b *TCPTransport
	for deadline := time.Now().Add(2 * time.Second); ; {
		b, err = NewTCPTransport(2, addr, map[raft.NodeID]string{1: "unused", 2: addr}, quiet)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	defer b.Close()

	// Heartbeat-like traffic: the restarted node must hear from node 1 quickly.
	stop := time.After(time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
			a.Send([]raft.Message{{Type: raft.MsgApp, From: 1, To: 2}})
		case m := <-b.Receive():
			if m.From != 1 {
				t.Fatalf("unexpected message %+v", m)
			}
			return
		case <-stop:
			t.Fatal("restarted peer received nothing within 1 s: the sender kept its dead connection")
		}
	}
}
