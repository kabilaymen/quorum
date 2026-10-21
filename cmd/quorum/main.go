// Command quorum runs one node of a replicated key-value store.
//
//	quorum -id 1 -peers 1=node1:7000,2=node2:7000,3=node3:7000 -http :8080
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kabilaymen/quorum/raft"
	"github.com/kabilaymen/quorum/server"
)

func parsePeers(s string) (map[raft.NodeID]string, []raft.NodeID, error) {
	peers := map[raft.NodeID]string{}
	var ids []raft.NodeID
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.ParseUint(id, 10, 64)
		if !ok || err != nil || n == 0 {
			return nil, nil, fmt.Errorf("bad peer %q (want id=host:port)", part)
		}
		peers[raft.NodeID(n)] = addr
		ids = append(ids, raft.NodeID(n))
	}
	return peers, ids, nil
}

func main() {
	id := flag.Uint64("id", 0, "this node's id (must appear in -peers)")
	peersFlag := flag.String("peers", "", "comma-separated id=host:port raft addresses of all members")
	raftAddr := flag.String("raft", ":7000", "listen address for raft traffic")
	httpAddr := flag.String("http", ":8080", "listen address for the client API")
	tick := flag.Duration("tick", 10*time.Millisecond, "raft tick duration")
	dataDir := flag.String("data", "", "directory for the write-ahead log (empty: in memory, not durable)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, nil)).With("node", *id)
	peers, ids, err := parsePeers(*peersFlag)
	if err != nil || *id == 0 {
		fmt.Fprintln(os.Stderr, "usage: quorum -id N -peers 1=host:port,2=host:port,...", err)
		os.Exit(2)
	}
	tr, err := server.NewTCPTransport(raft.NodeID(*id), *raftAddr, peers, log)
	if err != nil {
		log.Error("raft listener", "err", err)
		os.Exit(1)
	}
	s, err := server.New(server.Config{
		Raft: raft.Config{
			ID: raft.NodeID(*id), Peers: ids,
			ElectionTick: 15, HeartbeatTick: 3, // 150–300 ms election timeout at 10 ms ticks
			Seed: time.Now().UnixNano(), CheckQuorum: true, PreVote: true,
		},
		TickRate: *tick,
		DataDir:  *dataDir,
	}, tr)
	if err != nil {
		log.Error("open data dir", "dir", *dataDir, "err", err)
		os.Exit(1)
	}
	s.Start()

	srv := &http.Server{Addr: *httpAddr, Handler: server.Handler(s, 2*time.Second), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "err", err)
			os.Exit(1)
		}
	}()
	log.Info("started", "raft", *raftAddr, "http", *httpAddr, "peers", *peersFlag, "data", *dataDir)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	go func() {
		var last server.Status
		for t := time.NewTicker(time.Second); ; <-t.C {
			if ctx.Err() != nil {
				return
			}
			if st := s.Status(); st.State != last.State || st.Term != last.Term {
				log.Info("role", "state", st.State, "term", st.Term, "leader", st.Leader)
				last = st
			}
		}
	}()
	<-ctx.Done()
	shutdown, done := context.WithTimeout(context.Background(), 3*time.Second)
	defer done()
	srv.Shutdown(shutdown)
	s.Stop()
}
