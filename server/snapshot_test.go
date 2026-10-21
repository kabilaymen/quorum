package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
)

func waitFor(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
		if cond() {
			return
		}
	}
	t.Fatal(what)
}

// TestSnapshotsBoundTheLog: with compaction on, every node keeps only the
// recent tail of the log, and the data is intact.
func TestSnapshotsBoundTheLog(t *testing.T) {
	tc := startClusterWith(t, 3, 20)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(tc.urls)
	for i := 0; i < 120; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("k%d", i%30), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, 5*time.Second, "nodes did not all apply the writes", func() bool {
		for _, s := range tc.servers {
			if st := s.Status(); st.Keys != 30 || st.Applied < 120 {
				return false
			}
		}
		return true
	})
	for id, s := range tc.servers {
		st := s.Status()
		if kept := st.CommitIndex - st.FirstIndex + 1; st.FirstIndex < 80 || kept > 2*20 {
			t.Errorf("node %d keeps %d entries (log starts at %d of %d)", id, kept, st.FirstIndex, st.CommitIndex)
		}
	}
	for i := 90; i < 120; i++ {
		if r, err := c.Get(ctx, fmt.Sprintf("k%d", i%30)); err != nil || r.Value != fmt.Sprint(i) {
			t.Fatalf("k%d = %+v %v, want %d", i%30, r, err, i)
		}
	}
}

// TestLaggingNodeCatchesUpWithSnapshot: a follower down while the others
// compacted past its log can only catch up through a snapshot.
func TestLaggingNodeCatchesUpWithSnapshot(t *testing.T) {
	tc := startClusterWith(t, 3, 20)
	lead := tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(tc.urls)
	var f raft.NodeID
	for _, id := range tc.ids {
		if id != lead {
			f = id
		}
	}
	c.Put(ctx, "before", "1")
	tc.kill(f)
	for i := 0; i < 100; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("during%d", i), "x"); err != nil {
			t.Fatal(err)
		}
	}
	if st := tc.servers[lead].Status(); st.FirstIndex < 50 {
		t.Fatalf("leader did not compact (log starts at %d)", st.FirstIndex)
	}
	tc.start(f)
	waitFor(t, 5*time.Second, "follower did not catch up", func() bool { return tc.servers[f].Status().Keys == 101 })
	if st := tc.servers[f].Status(); st.FirstIndex < 50 {
		t.Fatalf("follower caught up but its log starts at %d: no snapshot was installed", st.FirstIndex)
	}
}

// TestFullRestartFromSnapshots: every node restarts from its snapshot and the
// log tail on disk.
func TestFullRestartFromSnapshots(t *testing.T) {
	tc := startClusterWith(t, 3, 15)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c := client.New(tc.urls)
	for i := 0; i < 60; i++ {
		if _, err := c.Put(ctx, fmt.Sprintf("k%d", i), fmt.Sprint(i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range tc.ids {
		tc.kill(id)
	}
	for _, id := range tc.ids {
		tc.start(id)
		if st := tc.servers[id].Status(); st.FirstIndex < 30 || st.Keys < 30 {
			t.Fatalf("node %d restarted without its snapshot: %+v", id, st)
		}
	}
	tc.waitLeader(t, 0)
	for i := 0; i < 60; i++ {
		if r, err := c.Get(ctx, fmt.Sprintf("k%d", i)); err != nil || r.Value != fmt.Sprint(i) {
			t.Fatalf("k%d after restart = %+v %v", i, r, err)
		}
	}
}
