package server_test

import (
	"context"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
)

func waitHealthy(t *testing.T, cc *client.CachedClient) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cc.Healthy() {
		if time.Now().After(deadline) {
			t.Fatal("cache never became healthy")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestCacheHitsAndInvalidation(t *testing.T) {
	tc := startCluster(t, 3)
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cc := client.NewCached(tc.urls)
	go cc.Run(ctx)
	waitHealthy(t, cc)
	writer := client.New(tc.urls)

	writer.Put(ctx, "k", "v1")
	cc.Get(ctx, "k") // miss, fills the cache
	for i := 0; i < 10; i++ {
		if r, _ := cc.Get(ctx, "k"); r.Value != "v1" {
			t.Fatalf("got %q", r.Value)
		}
	}
	if hits, _ := cc.Stats(); hits < 10 {
		t.Fatalf("hits = %d, want 10", hits)
	}

	writer.Put(ctx, "k", "v2") // another client: only the stream can tell us
	deadline := time.Now().Add(time.Second)
	for {
		r, err := cc.Get(ctx, "k")
		if err == nil && r.Value == "v2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("still reading %q one second after the write", r.Value)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// read-your-writes
	cc.Put(ctx, "k", "mine")
	if r, _ := cc.Get(ctx, "k"); r.Value != "mine" {
		t.Fatalf("read-your-writes violated: %q", r.Value)
	}
}

// TestCacheBoundedStaleness: one writer increments a counter while cached
// readers read it and a nemesis partitions nodes (including the one a reader
// is watching). Every read must be monotonic, and must not miss a write that
// completed more than `bound` before the read started.
func TestCacheBoundedStaleness(t *testing.T) {
	const bound = time.Second
	tc := startClusterWith(t, 5, 40) // compaction on: snapshots get installed under partitions
	tc.waitLeader(t, 0)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()

	type write struct {
		done    time.Duration
		version uint64
	}
	var (
		mu     sync.Mutex
		writes []write
		wg     sync.WaitGroup
	)
	wg.Add(1)
	go func() { // writer
		defer wg.Done()
		w := client.New(tc.urls)
		for i := 1; ctx.Err() == nil; i++ {
			octx, ocancel := context.WithTimeout(ctx, 3*time.Second)
			r, err := w.Put(octx, "x", strconv.Itoa(i))
			ocancel()
			if err == nil {
				mu.Lock()
				writes = append(writes, write{time.Since(start), r.Version})
				mu.Unlock()
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	type read struct {
		start   time.Duration
		version uint64
	}
	readers := make([][]read, 3)
	caches := make([]*client.CachedClient, 3)
	for i := range readers {
		cc := client.NewCached(tc.urls)
		caches[i] = cc
		go cc.Run(ctx)
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for ctx.Err() == nil {
				s := time.Since(start)
				octx, ocancel := context.WithTimeout(ctx, 3*time.Second)
				r, err := cc.Get(octx, "x")
				ocancel()
				if err == nil {
					readers[i] = append(readers[i], read{s, r.Version})
				}
				time.Sleep(200 * time.Microsecond)
			}
		}(i)
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	for deadline := time.Now().Add(4 * time.Second); time.Now().Before(deadline); time.Sleep(300 * time.Millisecond) {
		id := raft.NodeID(1 + rnd.Intn(5))
		tc.net.Isolate(id, true)
		time.Sleep(300 * time.Millisecond)
		tc.net.Isolate(id, false)
	}
	cancel()
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	if len(writes) < 20 {
		t.Fatalf("only %d writes completed", len(writes))
	}
	// writes are appended in completion order by a single writer, so versions
	// increase with done time: staleness of a read = how long the first newer
	// write had already been committed when the read started.
	totalReads := 0
	var stale []time.Duration
	for i, rs := range readers {
		totalReads += len(rs)
		var prev uint64
		for _, r := range rs {
			if r.version < prev {
				t.Fatalf("reader %d went back in time: version %d after %d", i, r.version, prev)
			}
			prev = r.version
			j := sort.Search(len(writes), func(k int) bool { return writes[k].version > r.version })
			if j < len(writes) && writes[j].done < r.start {
				age := r.start - writes[j].done
				stale = append(stale, age)
				if age > bound {
					t.Fatalf("reader %d at %v saw version %d, missing version %d committed %v earlier (bound %v)",
						i, r.start, r.version, writes[j].version, age, bound)
				}
			}
		}
	}
	sort.Slice(stale, func(a, b int) bool { return stale[a] < stale[b] })
	pct := func(q float64) time.Duration {
		if len(stale) == 0 {
			return 0
		}
		return stale[int(q*float64(len(stale)-1))]
	}
	t.Logf("stale reads: %d of %d (%.2f%%), staleness p50 %v, p99 %v, max %v",
		len(stale), totalReads, 100*float64(len(stale))/float64(max(totalReads, 1)), pct(0.5), pct(0.99), pct(1))
	var hits, misses uint64
	for _, cc := range caches {
		h, m := cc.Stats()
		hits, misses = hits+h, misses+m
	}
	t.Logf("%d writes, %d reads, cache hit rate %.0f%%", len(writes), totalReads, 100*float64(hits)/float64(hits+misses))
}
