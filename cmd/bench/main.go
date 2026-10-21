// Command bench starts a local Quorum cluster (real TCP transport, real WAL
// with fsync) and measures write and read throughput/latency, cached reads,
// and leader failover time.
//
//	go run ./cmd/bench -nodes 3 -clients 16 -duration 5s
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
	"github.com/kabilaymen/quorum/server"
)

type node struct {
	id      raft.NodeID
	srv     *server.Server
	http    *http.Server
	ln      net.Listener
	dataDir string
}

type cluster struct {
	ids       []raft.NodeID
	peers     map[raft.NodeID]string
	httpAddrs map[raft.NodeID]string
	nodes     map[raft.NodeID]*node
	dir       string
	fsync     bool
	log       *slog.Logger
	mu        sync.Mutex
}

func (c *cluster) start(id raft.NodeID) error {
	tr, err := server.NewTCPTransport(id, c.peers[id], c.peers, c.log)
	if err != nil {
		return err
	}
	dir := filepath.Join(c.dir, fmt.Sprint(id))
	s, err := server.New(server.Config{
		Raft: raft.Config{ID: id, Peers: c.ids, ElectionTick: 15, HeartbeatTick: 3,
			Seed: time.Now().UnixNano() + int64(id), CheckQuorum: true},
		TickRate: 10 * time.Millisecond,
		DataDir:  dir,
		NoSync:   !c.fsync,
	}, tr)
	if err != nil {
		tr.Close()
		return err
	}
	ln, err := net.Listen("tcp", c.httpAddrs[id])
	if err != nil {
		s.Stop()
		return err
	}
	hs := &http.Server{Handler: server.Handler(s, 2*time.Second)}
	go hs.Serve(ln)
	s.Start()
	c.mu.Lock()
	c.nodes[id] = &node{id: id, srv: s, http: hs, ln: ln, dataDir: dir}
	c.mu.Unlock()
	return nil
}

func (c *cluster) kill(id raft.NodeID) {
	c.mu.Lock()
	n := c.nodes[id]
	delete(c.nodes, id)
	c.mu.Unlock()
	n.http.Close()
	n.srv.Stop()
}

func (c *cluster) leader() raft.NodeID {
	c.mu.Lock()
	defer c.mu.Unlock()
	for id, n := range c.nodes {
		if n.srv.Status().State == "leader" {
			return id
		}
	}
	return 0
}

func (c *cluster) waitLeader(timeout time.Duration) (raft.NodeID, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if l := c.leader(); l != 0 {
			return l, nil
		}
		time.Sleep(5 * time.Millisecond)
	}
	return 0, errors.New("no leader")
}

func (c *cluster) endpoints() map[raft.NodeID]string {
	out := map[raft.NodeID]string{}
	for id, a := range c.httpAddrs {
		out[id] = "http://" + a
	}
	return out
}

// ---------------------------------------------------------------- measurement

type Stats struct {
	Ops        int     `json:"ops"`
	Errors     int     `json:"errors"`
	Throughput float64 `json:"ops_per_sec"`
	P50ms      float64 `json:"p50_ms"`
	P99ms      float64 `json:"p99_ms"`
	P999ms     float64 `json:"p999_ms"`
	// MeanMs comes from Little's law (clients / throughput): exact even when
	// the OS clock is too coarse to time a single fast operation.
	MeanMs  float64 `json:"mean_ms"`
	HitRate float64 `json:"hit_rate,omitempty"`
}

func summarize(lat []time.Duration, errs int, elapsed time.Duration, clients int) Stats {
	slices.Sort(lat)
	q := func(p float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[int(p*float64(len(lat)-1))].Nanoseconds()) / 1e6
	}
	tput := float64(len(lat)) / elapsed.Seconds()
	return Stats{Ops: len(lat), Errors: errs, Throughput: tput,
		P50ms: q(0.50), P99ms: q(0.99), P999ms: q(0.999), MeanMs: 1000 * float64(clients) / max(tput, 1e-9)}
}

// run executes op from `clients` goroutines for `d` and collects latencies.
func run(clients int, d time.Duration, op func(w int, r *rand.Rand) error) Stats {
	var (
		mu   sync.Mutex
		lat  []time.Duration
		errs atomic.Int64
		wg   sync.WaitGroup
	)
	start := time.Now()
	stop := start.Add(d)
	for w := 0; w < clients; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			r := rand.New(rand.NewSource(int64(w)))
			local := make([]time.Duration, 0, 4096)
			for time.Now().Before(stop) {
				t := time.Now()
				if err := op(w, r); err != nil {
					errs.Add(1)
					continue
				}
				local = append(local, time.Since(t))
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(w)
	}
	wg.Wait()
	return summarize(lat, int(errs.Load()), time.Since(start), clients)
}

type Results struct {
	Env       string          `json:"env"`
	Nodes     int             `json:"nodes"`
	Clients   int             `json:"clients"`
	Fsync     bool            `json:"fsync"`
	ValueSize int             `json:"value_size"`
	Writes    Stats           `json:"writes"`
	Reads     Stats           `json:"linearizable_reads"`
	Cached    Stats           `json:"cached_reads"`
	Failover  []float64       `json:"failover_ms"`
	WALSync   json.RawMessage `json:"-"`
}

func main() {
	nodes := flag.Int("nodes", 3, "cluster size")
	clients := flag.Int("clients", 16, "concurrent clients")
	duration := flag.Duration("duration", 5*time.Second, "duration of each workload")
	fsync := flag.Bool("fsync", true, "fsync the WAL (false: measures the protocol without disk)")
	keys := flag.Int("keys", 1000, "key space")
	valueSize := flag.Int("value-size", 64, "value size in bytes")
	failovers := flag.Int("failovers", 5, "number of leader kills to measure")
	basePort := flag.Int("port", 17000, "first port to use")
	out := flag.String("out", "", "write results as JSON to this file")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	dir, err := os.MkdirTemp("", "quorum-bench-*")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	c := &cluster{peers: map[raft.NodeID]string{}, httpAddrs: map[raft.NodeID]string{}, nodes: map[raft.NodeID]*node{},
		dir: dir, fsync: *fsync, log: log}
	for i := 1; i <= *nodes; i++ {
		id := raft.NodeID(i)
		c.ids = append(c.ids, id)
		c.peers[id] = fmt.Sprintf("127.0.0.1:%d", *basePort+i)
		c.httpAddrs[id] = fmt.Sprintf("127.0.0.1:%d", *basePort+100+i)
	}
	for _, id := range c.ids {
		if err := c.start(id); err != nil {
			fmt.Fprintln(os.Stderr, "start:", err)
			os.Exit(1)
		}
	}
	if _, err := c.waitLeader(5 * time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	res := Results{Env: fmt.Sprintf("%s/%s, %d CPUs, single machine (loopback)", runtime.GOOS, runtime.GOARCH, runtime.NumCPU()),
		Nodes: *nodes, Clients: *clients, Fsync: *fsync, ValueSize: *valueSize}
	value := strings.Repeat("x", *valueSize)
	ctx := context.Background()
	opCtx := func() (context.Context, context.CancelFunc) { return context.WithTimeout(ctx, 3*time.Second) }

	plain := make([]*client.Client, *clients)
	for i := range plain {
		plain[i] = client.New(c.endpoints())
	}
	fmt.Fprintf(os.Stderr, "writes...\n")
	res.Writes = run(*clients, *duration, func(w int, r *rand.Rand) error {
		cx, cancel := opCtx()
		defer cancel()
		_, err := plain[w].Put(cx, fmt.Sprintf("k%d", r.Intn(*keys)), value)
		return err
	})
	fmt.Fprintf(os.Stderr, "linearizable reads...\n")
	res.Reads = run(*clients, *duration, func(w int, r *rand.Rand) error {
		cx, cancel := opCtx()
		defer cancel()
		_, err := plain[w].Get(cx, fmt.Sprintf("k%d", r.Intn(*keys)))
		return err
	})

	fmt.Fprintf(os.Stderr, "cached reads (with a background writer at ~200 writes/s)...\n")
	cctx, ccancel := context.WithCancel(ctx)
	cached := make([]*client.CachedClient, *clients)
	for i := range cached {
		cached[i] = client.NewCached(c.endpoints())
		go cached[i].Run(cctx)
	}
	for _, cc := range cached {
		for !cc.Healthy() {
			time.Sleep(5 * time.Millisecond)
		}
	}
	go func() {
		w := client.New(c.endpoints())
		r := rand.New(rand.NewSource(99))
		for t := time.NewTicker(5 * time.Millisecond); cctx.Err() == nil; <-t.C {
			cx, cancel := opCtx()
			w.Put(cx, fmt.Sprintf("k%d", r.Intn(*keys)), value)
			cancel()
		}
	}()
	// Real workloads are skewed: a few keys get most reads (Zipf, s=1.1).
	zipfs := make([]*rand.Zipf, *clients)
	for i := range zipfs {
		zipfs[i] = rand.NewZipf(rand.New(rand.NewSource(int64(1000+i))), 1.1, 1, uint64(*keys-1))
	}
	cachedRead := func(w int, r *rand.Rand) error {
		cx, cancel := opCtx()
		defer cancel()
		_, err := cached[w].Get(cx, fmt.Sprintf("k%d", zipfs[w].Uint64()))
		return err
	}
	run(*clients, time.Second, cachedRead) // warm-up, not measured
	var h0, m0 uint64
	for _, cc := range cached {
		h, m := cc.Stats()
		h0, m0 = h0+h, m0+m
	}
	res.Cached = run(*clients, *duration, cachedRead)
	var hits, misses uint64
	for _, cc := range cached {
		h, m := cc.Stats()
		hits, misses = hits+h, misses+m
	}
	hits, misses = hits-h0, misses-m0
	res.Cached.HitRate = float64(hits) / float64(max(hits+misses, 1))
	ccancel()

	fmt.Fprintf(os.Stderr, "failover...\n")
	probe := client.New(c.endpoints())
	for i := 0; i < *failovers; i++ {
		lead, err := c.waitLeader(5 * time.Second)
		if err != nil {
			break
		}
		cx, cancel := opCtx()
		probe.Put(cx, "probe", "warm") // make sure the probe knows the current leader
		cancel()
		killed := time.Now()
		c.kill(lead)
		for {
			cx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
			_, err := probe.Put(cx, "probe", fmt.Sprint(i))
			cancel()
			if err == nil {
				break
			}
		}
		res.Failover = append(res.Failover, float64(time.Since(killed).Microseconds())/1000)
		if err := c.start(lead); err != nil { // comes back from its WAL
			fmt.Fprintln(os.Stderr, "restart:", err)
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}

	for _, id := range c.ids {
		if c.nodes[id] != nil {
			c.kill(id)
		}
	}
	printMarkdown(res)
	if *out != "" {
		b, _ := json.MarshalIndent(res, "", "  ")
		os.WriteFile(*out, b, 0o644)
	}
}

// clockResolutionMs is measured at startup: on Windows time.Now() advances in
// steps of ~0.4 ms, so per-operation percentiles below that are meaningless.
var clockResolutionMs = measureClock()

func measureClock() float64 {
	smallest := time.Hour
	for i := 0; i < 20; i++ {
		a := time.Now()
		b := time.Now()
		for b.Equal(a) {
			b = time.Now()
		}
		smallest = min(smallest, b.Sub(a))
	}
	return max(float64(smallest.Nanoseconds())/1e6, 0.001)
}

func printMarkdown(r Results) {
	fmt.Printf("Environment: %s — %d nodes, %d clients, %d-byte values, fsync=%v, clock resolution %.3f ms\n\n", r.Env, r.Nodes, r.Clients, r.ValueSize, r.Fsync, clockResolutionMs)
	fmt.Println("| Workload | ops/s | mean | p50 | p99 | p99.9 | notes |")
	fmt.Println("|---|---:|---:|---:|---:|---:|---|")
	ms := func(v float64) string {
		if v < clockResolutionMs {
			return fmt.Sprintf("< %.1f ms", clockResolutionMs)
		}
		return fmt.Sprintf("%.2f ms", v)
	}
	row := func(name string, s Stats, note string) {
		mean := fmt.Sprintf("%.3f ms", s.MeanMs)
		if s.MeanMs < 0.01 {
			mean = fmt.Sprintf("%.0f ns", s.MeanMs*1e6)
		}
		fmt.Printf("| %s | %.0f | %s | %s | %s | %s | %s |\n", name, s.Throughput, mean, ms(s.P50ms), ms(s.P99ms), ms(s.P999ms), note)
	}
	writes := "Writes (PUT, replicated to a majority)"
	if r.Fsync {
		writes = "Writes (PUT, replicated to a majority + fsync)"
	}
	row(writes, r.Writes, fmt.Sprintf("%d errors", r.Writes.Errors))
	row("Linearizable reads (through the log)", r.Reads, fmt.Sprintf("%d errors", r.Reads.Errors))
	row("Cached reads (CachedClient, Zipf keys)", r.Cached, fmt.Sprintf("hit rate %.1f%%, concurrent writer at ~200/s", 100*r.Cached.HitRate))
	if len(r.Failover) > 0 {
		f := slices.Clone(r.Failover)
		slices.Sort(f)
		fmt.Printf("\nFailover (leader killed → next successful write), %d runs: min %.0f ms, median %.0f ms, max %.0f ms\n",
			len(f), f[0], f[len(f)/2], f[len(f)-1])
	}
}
