package client

import (
	"bufio"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kabilaymen/quorum/kv"
	"github.com/kabilaymen/quorum/raft"
)

// CachedClient serves reads from a local cache kept coherent by the cluster's
// invalidation stream (GET /watch on any node).
//
// Guarantees:
//   - read-your-writes and monotonic reads per key (a cached entry is only
//     replaced by a newer version);
//   - bounded staleness: a cached value can miss a write only until that
//     write's invalidation reaches the client. If the stream is silent for
//     MaxSilence, or the watched node loses its leader, the cache is emptied
//     and reads go back to the (linearizable) cluster.
//
// Cached reads are therefore NOT linearizable; use Client for that.
type CachedClient struct {
	*Client
	MaxSilence time.Duration

	mu         sync.Mutex
	entries    map[string]kv.Result
	floor      map[string]uint64 // highest version announced per key
	healthy    bool
	lastEvent  time.Time
	epoch      uint64 // bumped on every reset: in-flight fills from an older epoch are discarded
	hits, miss atomic.Uint64
}

func NewCached(endpoints map[raft.NodeID]string) *CachedClient {
	return &CachedClient{
		Client:     New(endpoints),
		MaxSilence: 300 * time.Millisecond,
		entries:    map[string]kv.Result{},
		floor:      map[string]uint64{},
	}
}

// Run follows the invalidation stream until ctx is done, reconnecting to
// another node whenever the stream breaks.
func (c *CachedClient) Run(ctx context.Context) {
	stream := &http.Client{} // no timeout: the response is an endless stream
	for i := 0; ctx.Err() == nil; i++ {
		node := c.order[i%len(c.order)]
		c.follow(ctx, stream, c.endpoints[node])
		c.reset()
		select {
		case <-ctx.Done():
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func (c *CachedClient) follow(ctx context.Context, hc *http.Client, base string) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/watch", nil)
	if err != nil {
		return
	}
	resp, err := hc.Do(req)
	if err != nil {
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return
	}
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		var ev struct {
			Type    string `json:"type"`
			Key     string `json:"key"`
			Version uint64 `json:"version"`
			Healthy bool   `json:"healthy"`
		}
		if json.Unmarshal(sc.Bytes(), &ev) != nil {
			return
		}
		c.mu.Lock()
		c.lastEvent = time.Now()
		switch ev.Type {
		case "progress":
			if !ev.Healthy && c.healthy {
				c.clearLocked()
			}
			c.healthy = ev.Healthy
		case "inv":
			if ev.Version > c.floor[ev.Key] {
				c.floor[ev.Key] = ev.Version
			}
			if e, ok := c.entries[ev.Key]; ok && e.Version < ev.Version {
				delete(c.entries, ev.Key)
			}
		}
		c.mu.Unlock()
	}
}

func (c *CachedClient) reset() {
	c.mu.Lock()
	c.healthy = false
	c.clearLocked()
	c.mu.Unlock()
}

func (c *CachedClient) clearLocked() {
	c.entries = map[string]kv.Result{}
	c.floor = map[string]uint64{}
	c.epoch++
}

func (c *CachedClient) usableLocked() bool {
	return c.healthy && time.Since(c.lastEvent) < c.MaxSilence
}

// store caches r for key unless a newer write has already been announced or
// the cache was reset since the request started.
func (c *CachedClient) store(key string, r kv.Result, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if epoch != c.epoch || !c.usableLocked() || c.floor[key] > r.Version {
		return
	}
	if cur, ok := c.entries[key]; ok && cur.Version > r.Version {
		return
	}
	c.entries[key] = r
}

func (c *CachedClient) Get(ctx context.Context, key string) (kv.Result, error) {
	c.mu.Lock()
	if c.usableLocked() {
		if r, ok := c.entries[key]; ok {
			c.mu.Unlock()
			c.hits.Add(1)
			return r, nil
		}
	}
	epoch := c.epoch
	c.mu.Unlock()
	c.miss.Add(1)
	r, err := c.Client.Get(ctx, key)
	if err == nil {
		c.store(key, r, epoch)
	}
	return r, err
}

func (c *CachedClient) Put(ctx context.Context, key, value string) (kv.Result, error) {
	epoch := c.currentEpoch()
	r, err := c.Client.Put(ctx, key, value)
	if err == nil {
		c.store(key, r, epoch) // read-your-writes
	}
	return r, err
}

func (c *CachedClient) Delete(ctx context.Context, key string) (kv.Result, error) {
	epoch := c.currentEpoch()
	r, err := c.Client.Delete(ctx, key)
	if err == nil {
		c.store(key, kv.Result{Found: false, Version: r.Version}, epoch)
	}
	return r, err
}

func (c *CachedClient) currentEpoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Stats returns cache hits and misses since creation.
func (c *CachedClient) Stats() (hits, misses uint64) { return c.hits.Load(), c.miss.Load() }

// Healthy reports whether cached reads are currently allowed.
func (c *CachedClient) Healthy() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.usableLocked()
}
