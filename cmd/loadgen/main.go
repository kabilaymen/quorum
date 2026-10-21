// Command loadgen keeps a light, steady workload on a cluster so that the
// public dashboard always has traffic to show: a few writes and reads per
// second through the linearizable client, which follows leader changes.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kabilaymen/quorum/client"
	"github.com/kabilaymen/quorum/raft"
)

func main() {
	nodes := flag.String("nodes", "1=http://node1:8080,2=http://node2:8080,3=http://node3:8080", "comma-separated id=base URL")
	rate := flag.Float64("rate", 5, "operations per second")
	keys := flag.Int("keys", 20, "number of distinct keys")
	flag.Parse()

	endpoints := map[raft.NodeID]string{}
	for _, part := range strings.Split(*nodes, ",") {
		id, url, ok := strings.Cut(strings.TrimSpace(part), "=")
		n, err := strconv.ParseUint(id, 10, 64)
		if !ok || err != nil {
			fmt.Fprintf(os.Stderr, "bad node %q (want id=url)\n", part)
			os.Exit(2)
		}
		endpoints[raft.NodeID(n)] = url
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	c := client.New(endpoints)
	tick := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	defer tick.Stop()
	var ok, failed int
	report := time.NewTicker(time.Minute)
	defer report.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-report.C:
			slog.Info("load", "ok", ok, "failed", failed)
			ok, failed = 0, 0
		case <-tick.C:
			key := fmt.Sprintf("k%d", rand.IntN(*keys))
			opCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			var err error
			if rand.IntN(2) == 0 {
				_, err = c.Put(opCtx, key, strconv.FormatInt(time.Now().UnixMilli(), 10))
			} else {
				_, err = c.Get(opCtx, key)
			}
			cancel()
			if err != nil {
				failed++
			} else {
				ok++
			}
		}
	}
}
