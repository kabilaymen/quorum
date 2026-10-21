# Benchmark results

Produced by `go run ./cmd/bench`. Single machine, all nodes and clients on loopback: absolute
numbers depend on the disk (fsync) and CPU; compare rows, not machines. Run-to-run variance
is noticeable on a desktop (p99.9 and failover especially).

`mean` is derived from throughput (Little's law) and stays exact when the OS clock is too coarse
to time one fast operation (Windows: ~0.4 ms steps); percentiles below the clock resolution are
shown as `< x ms`.

## Current code on a GitHub runner

Run by the `bench` workflow on 2026-10-21
([actions/runs/39597977740](https://github.com/kabilaymen/quorum/actions/runs/39597977740)), with
PreVote and log compaction (snapshot every 1,000 entries) — Linux, 4 vCPU, 3 nodes and 16 clients
on one machine over loopback, 64-byte values:

| Workload | fsync | ops/s | mean | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|---:|
| Writes | yes | 6,070 | 2.64 ms | 2.41 ms | 7.37 ms | 31.2 ms |
| Linearizable reads | yes | 6,558 | 2.44 ms | 2.26 ms | 7.30 ms | 15.4 ms |
| Cached reads | yes | ~1,036,000 | 0.015 ms | < 0.01 ms | < 0.01 ms | 4.4 ms |
| Writes | no | 14,861 | 1.08 ms | 0.91 ms | 4.10 ms | 11.5 ms |
| Linearizable reads | no | 16,043 | 1.00 ms | 0.88 ms | 3.89 ms | 5.4 ms |

Failover (leader killed → next successful write), 7 runs with fsync: 202–275 ms, median 212 ms.

The sections below are the original measurements on a Windows desktop (20 CPUs, before PreVote
and snapshots). Compare rows within one environment, not across them.

## With fsync (durable)

Environment: windows/amd64, 20 CPUs, single machine (loopback) — 3 nodes, 16 clients, 64-byte values, fsync=true, clock resolution 0.514 ms

| Workload | ops/s | mean | p50 | p99 | p99.9 | notes |
|---|---:|---:|---:|---:|---:|---|
| Writes (PUT, replicated to a majority + fsync) | 3494 | 4.579 ms | 4.65 ms | 5.89 ms | 10.12 ms | 0 errors |
| Linearizable reads (through the log) | 3472 | 4.608 ms | 4.65 ms | 6.23 ms | 10.37 ms | 0 errors |
| Cached reads (CachedClient, Zipf keys) | 45064 | 0.355 ms | < 0.5 ms | 5.79 ms | 6.72 ms | hit rate 93.1%, concurrent writer at ~200/s |

Failover (leader killed → next successful write), 7 runs: min 203 ms, median 355 ms, max 614 ms

## Without fsync (protocol cost only)

Environment: windows/amd64, 20 CPUs, single machine (loopback) — 3 nodes, 16 clients, 64-byte values, fsync=false, clock resolution 0.507 ms

| Workload | ops/s | mean | p50 | p99 | p99.9 | notes |
|---|---:|---:|---:|---:|---:|---|
| Writes (PUT, replicated to a majority) | 41043 | 0.390 ms | 0.51 ms | 1.51 ms | 2.20 ms | 0 errors |
| Linearizable reads (through the log) | 40746 | 0.393 ms | 0.51 ms | 1.50 ms | 2.42 ms | 0 errors |
| Cached reads (CachedClient, Zipf keys) | 20970579 | 763 ns | < 0.5 ms | < 0.5 ms | < 0.5 ms | hit rate 100.0%, concurrent writer at ~200/s |

Failover (leader killed → next successful write), 7 runs: min 202 ms, median 472 ms, max 554 ms

## How these numbers were reached

The first benchmark run (same machine, fsync on) gave **842 writes/s at p50 18.8 ms**. Profiling
the path step by step showed raft itself at 34 µs per operation in isolation, but 3 ms per
operation with 16 concurrent clients. Three fixes, each re-validated with 1,000 fault-injection
seeds and the 20 s linearizability check:

| Fix | Effect (16 clients) |
|---|---|
| **Pipelined replication**: advance `next[follower]` optimistically after each append instead of resending the whole unacknowledged window on every proposal (a lost message is repaired by the next heartbeat's rejection hint) | raft path 5.3k → 172k ops/s |
| **Group commit**: drain every queued proposal and message before one `Ready` → one WAL fsync and one round of messages per batch; coalesce commit-index broadcasts | fsync paid once per batch instead of once per write |
| **HTTP connection pooling** in the client (Go's default keeps 2 idle connections per host) | HTTP path 5.6k → 30k ops/s |

End result with fsync: **842 → ~3,450 writes/s, p50 18.8 → 4.65 ms**.
