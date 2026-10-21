package server_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
)

type kvInput struct {
	put   bool
	key   string
	value string
}

type kvOutput struct {
	value string
	found bool
}

// kvModel is the sequential specification of a single-key register, checked
// per key (Porcupine partitions the history by key).
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return kvOutput{} },
	Step: func(state, input, output any) (bool, any) {
		st, in, out := state.(kvOutput), input.(kvInput), output.(kvOutput)
		if in.put {
			return true, kvOutput{value: in.value, found: true}
		}
		return out == st, st
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		if in.put {
			return fmt.Sprintf("put(%s, %s)", in.key, in.value)
		}
		return fmt.Sprintf("get(%s) -> %q", in.key, out.value)
	},
}

// TestLinearizableUnderPartitions runs concurrent clients against a 5-node
// cluster while a nemesis keeps isolating random nodes (often the leader), and
// checks that the recorded history is linearizable.
func TestLinearizableUnderPartitions(t *testing.T) {
	duration := 3 * time.Second
	if v, err := strconv.Atoi(os.Getenv("LIN_SECONDS")); err == nil {
		duration = time.Duration(v) * time.Second
	}
	if testing.Short() {
		duration = time.Second
	}
	tc := startClusterWith(t, 5, 10) // frequent compaction: lagging nodes need snapshots under partitions
	tc.waitLeader(t, 0)

	start := time.Now()
	stop := make(chan struct{})
	var (
		mu      sync.Mutex
		history []porcupine.Operation
		wg      sync.WaitGroup
	)
	// Checking linearizability is NP-complete: spreading operations over more
	// keys keeps each per-key sub-history small enough to check exhaustively.
	keys := []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"}
	for w := 0; w < 6; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			c := client.New(tc.urls)
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				in := kvInput{key: keys[r.Intn(len(keys))]}
				if r.Intn(2) == 0 {
					in.put, in.value = true, fmt.Sprintf("w%d-%d", w, i)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				call := time.Since(start).Nanoseconds()
				var out kvOutput
				var err error
				if in.put {
					_, err = c.Put(ctx, in.key, in.value)
				} else {
					res, e := c.Get(ctx, in.key)
					out, err = kvOutput{value: res.Value, found: res.Found}, e
				}
				ret := time.Since(start).Nanoseconds()
				cancel()
				if err != nil {
					if !in.put {
						continue // a failed read has no effect
					}
					// The write may or may not have been applied: it may take
					// effect at any later point, so it never "returns".
					ret = int64(1 << 62)
				}
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: w, Input: in, Call: call, Output: out, Return: ret})
				mu.Unlock()
				// Pace the clients: checking linearizability is NP-complete, and an
				// unthrottled run now produces too many operations to check.
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}

	// nemesis
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	isolated := raft.NodeID(0)
	for deadline := time.Now().Add(duration); time.Now().Before(deadline); time.Sleep(250 * time.Millisecond) {
		if isolated != 0 {
			tc.net.Isolate(isolated, false)
			isolated = 0
		} else if r.Intn(2) == 0 {
			isolated = raft.NodeID(1 + r.Intn(5))
			if lead := tc.waitLeader(t, 0); r.Intn(2) == 0 {
				isolated = lead // partition the leader half of the time
			}
			tc.net.Isolate(isolated, true)
		}
	}
	if isolated != 0 {
		tc.net.Isolate(isolated, false)
	}
	close(stop)
	wg.Wait()

	var installed uint64
	for _, srv := range tc.servers {
		installed += srv.Status().SnapshotsInstalled
	}
	t.Logf("snapshots installed by lagging nodes during the run: %d", installed)
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 2*time.Minute)
	t.Logf("%d operations checked", len(history))
	if len(history) < 50 {
		t.Fatalf("too few operations completed (%d): the cluster made little progress", len(history))
	}
	if res == porcupine.Unknown {
		t.Fatalf("checker timed out on %d operations (inconclusive, not a violation)", len(history))
	}
	if res != porcupine.Ok {
		f, _ := os.CreateTemp("", "quorum-history-*.html")
		porcupine.Visualize(kvModel, info, f)
		t.Fatalf("history is not linearizable (%v), visualization: %s", res, f.Name())
	}
}

// TestCheckerCatchesStaleRead makes sure the model actually rejects a
// non-linearizable history (a read that misses a completed write).
func TestCheckerCatchesStaleRead(t *testing.T) {
	history := []porcupine.Operation{
		{ClientId: 0, Input: kvInput{put: true, key: "a", value: "1"}, Output: kvOutput{}, Call: 0, Return: 10},
		{ClientId: 1, Input: kvInput{key: "a"}, Output: kvOutput{}, Call: 20, Return: 30},
	}
	if porcupine.CheckOperations(kvModel, history) {
		t.Fatal("stale read accepted as linearizable")
	}
	history[1].Output = kvOutput{value: "1", found: true}
	if !porcupine.CheckOperations(kvModel, history) {
		t.Fatal("correct read rejected")
	}
}
