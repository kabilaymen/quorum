package server_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
	"github.com/kabilaymen/quorum/server"
)

type testCluster struct {
	t       *testing.T
	ids     []raft.NodeID
	dir     string
	net     *server.MemNetwork
	servers map[raft.NodeID]*server.Server
	front   map[raft.NodeID]*swapHandler
	urls    map[raft.NodeID]string
}

// swapHandler keeps a node's URL stable across restarts; while the node is
// down it answers 503, like a crashed process behind a load balancer.
type swapHandler struct{ h atomic.Pointer[http.Handler] }

func (s *swapHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h := s.h.Load(); h != nil {
		(*h).ServeHTTP(w, r)
		return
	}
	http.Error(w, "node down", http.StatusServiceUnavailable)
}

func startCluster(t *testing.T, n int) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, dir: t.TempDir(), net: server.NewMemNetwork(), servers: map[raft.NodeID]*server.Server{},
		front: map[raft.NodeID]*swapHandler{}, urls: map[raft.NodeID]string{}}
	for i := 1; i <= n; i++ {
		tc.ids = append(tc.ids, raft.NodeID(i))
	}
	for _, id := range tc.ids {
		tc.front[id] = &swapHandler{}
		h := httptest.NewServer(tc.front[id])
		t.Cleanup(h.Close)
		tc.urls[id] = h.URL
		tc.start(id)
	}
	t.Cleanup(func() {
		for _, id := range tc.ids {
			tc.kill(id)
		}
	})
	return tc
}

func (tc *testCluster) start(id raft.NodeID) {
	tc.t.Helper()
	s, err := server.New(server.Config{
		Raft:     raft.Config{ID: id, Peers: tc.ids, ElectionTick: 10, HeartbeatTick: 2, Seed: time.Now().UnixNano(), CheckQuorum: true, PreVote: true},
		TickRate: 5 * time.Millisecond,
		DataDir:  filepath.Join(tc.dir, fmt.Sprint(id)),
		NoSync:   true, // the test simulates process crashes, not power loss
	}, tc.net.Join(id))
	if err != nil {
		tc.t.Fatal(err)
	}
	s.Start()
	var h http.Handler = server.Handler(s, time.Second)
	tc.front[id].h.Store(&h)
	tc.servers[id] = s
}

func (tc *testCluster) kill(id raft.NodeID) {
	if s, ok := tc.servers[id]; ok {
		tc.front[id].h.Store(nil)
		s.Stop()
		delete(tc.servers, id)
	}
}

func (tc *testCluster) waitLeader(t *testing.T, exclude raft.NodeID) raft.NodeID {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		for id, s := range tc.servers {
			if id != exclude && s.Status().State == "leader" {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("no leader elected")
	return 0
}

func TestPutGetThroughAnyNode(t *testing.T) {
	tc := startCluster(t, 3)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := client.New(tc.urls)
	if _, err := c.Put(ctx, "city", "Palaiseau"); err != nil {
		t.Fatal(err)
	}
	r, err := c.Get(ctx, "city")
	if err != nil || !r.Found || r.Value != "Palaiseau" {
		t.Fatalf("get = %+v, %v", r, err)
	}
	if r.Version == 0 {
		t.Fatal("expected a version")
	}
}

func TestSurvivesLeaderCrash(t *testing.T) {
	tc := startCluster(t, 3)
	old := tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c := client.New(tc.urls)
	for i := 0; i < 20; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	tc.kill(old)
	tc.waitLeader(t, old)
	for i := 20; i < 40; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatalf("after crash: %v", err)
		}
	}
	for i := 0; i < 40; i++ {
		if r, err := c.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || !r.Found {
			t.Fatalf("k%d lost: %+v %v", i, r, err)
		}
	}
}

func TestConcurrentClients(t *testing.T) {
	tc := startCluster(t, 5)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			c := client.New(tc.urls)
			for i := 0; i < 25; i++ {
				if _, err := c.Put(ctx, fmt.Sprintf("w%d-%d", w, i), "x"); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	// every replica converges to the same 200 keys
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ok := true
		for _, s := range tc.servers {
			if s.Status().Keys != 200 {
				ok = false
			}
		}
		if ok {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	for id, s := range tc.servers {
		t.Logf("node %d: %+v", id, s.Status())
	}
	t.Fatal("replicas did not converge to 200 keys")
}

// TestFullClusterRestart stops every node and starts them again from their
// write-ahead logs: committed data must survive.
func TestFullClusterRestart(t *testing.T) {
	tc := startCluster(t, 3)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(tc.urls)
	for i := 0; i < 50; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range tc.ids {
		tc.kill(id)
	}
	for _, id := range tc.ids {
		tc.start(id)
	}
	tc.waitLeader(t, 0)
	for i := 0; i < 50; i++ {
		r, err := c.Get(ctx, fmt.Sprintf("k%d", i))
		if err != nil || !r.Found || r.Value != fmt.Sprint(i) {
			t.Fatalf("k%d after full restart: %+v %v", i, r, err)
		}
	}
}

// TestRestartedNodeRejoins: a single node restarts from disk and catches up
// on what it missed while it was down.
func TestRestartedNodeRejoins(t *testing.T) {
	tc := startCluster(t, 3)
	lead := tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(tc.urls)
	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != lead {
			follower = id
			break
		}
	}
	c.Put(ctx, "before", "1")
	tc.kill(follower)
	for i := 0; i < 20; i++ {
		c.Put(ctx, fmt.Sprintf("during%d", i), "x")
	}
	tc.start(follower)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if st := tc.servers[follower].Status(); st.Keys == 21 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("restarted follower has %d keys, want 21", tc.servers[follower].Status().Keys)
}
