# Quorum

A replicated, linearizable key-value store built on a from-scratch implementation of
[Raft](https://raft.github.io/raft.pdf), tested against message loss, partitions and crashes.

> Status: Raft core (with CheckQuorum), replicated KV, write-ahead log, coherent client cache,
> HTTP API, TCP transport and Docker cluster working.
> Next: k3s deployment with public Grafana dashboards.

How it was built, the bugs found on the way and how they were fixed: [`JOURNAL.md`](JOURNAL.md).

## Highlights

- **Deterministic Raft core** (`raft/`): leader election with randomized timeouts, log
  replication with fast conflict backtracking, the election restriction (§5.4.1) and the
  current-term commit rule (§5.4.2, Figure 8). The node never touches the clock, the network or the
  disk — time advances through `Tick()`, messages through `Step()` — so whole clusters run in a
  single test goroutine, reproducibly from a seed.
- **Randomized fault injection**: 5-node clusters through 1,500 rounds of 10 % message loss,
  reordering, delays, partitions and crash/restarts, with **Election Safety** and **State Machine
  Safety** asserted after every round, then a liveness check. 1,000 seeds pass.
- **Linearizability checked, not assumed**: concurrent clients run against a 5-node cluster while a
  nemesis isolates random nodes (the leader half of the time); the recorded history is verified
  with [Porcupine](https://github.com/anishathalye/porcupine). 48,834 operations over 20 s of
  partitions: linearizable. A control test proves the checker rejects stale reads.
- **Durable, and the durability contract is tested**: `Ready` tells the caller exactly what to
  persist (term, vote, new entries) before any message is sent. In the fault-injection harness a
  crashed node restarts *only* from what it persisted through that contract — and mutation tests
  confirm the harness catches the classic bugs: forgetting the vote yields two leaders in one term,
  forgetting entries breaks state machine safety. The WAL (`wal/`) is an append-only log of
  CRC32C-checksummed records with fsync; a torn or corrupted tail is detected and cut on recovery,
  and the HardState is replaced atomically (temp file + rename).
- **Exactly-once commands**: every request carries `(client id, sequence)`; the state machine keeps
  the last result per client, so a retried write whose reply was lost is not applied twice.
- **Coherent client cache with push invalidation**: every node streams `(key, version)` for each
  write it applies (`GET /watch`); `client.CachedClient` serves reads locally and evicts an entry as
  soon as a newer version is announced. Races are closed with versions (a fill older than an
  announced write is discarded) and epochs (a fill started before a stream reset is discarded). If
  the stream goes silent or the watched node loses its leader, the cache empties itself and reads go
  back to the linearizable path. Measured under partitions: **86 % hit rate, 2.6 % stale reads,
  staleness p99 ≈ 98 ms, max ≈ 99 ms**, monotonic per reader. A mutation test (ignore
  invalidations) makes both cache tests fail.
- **CheckQuorum**: a leader that has not heard from a majority for one election timeout steps down,
  so an isolated node stops presenting itself as healthy.

## Performance

3 nodes, 16 clients, one desktop machine over loopback (full tables and method in
[`bench/RESULTS.md`](bench/RESULTS.md), reproduce with `make bench` / `go run ./cmd/bench`):

| Workload | ops/s | mean latency |
|---|---:|---:|
| Writes, replicated to a majority + fsync | ~3,450 | 4.6 ms |
| Writes without fsync (protocol cost) | ~41,000 | 0.39 ms |
| Cached reads (Zipf keys, concurrent writer) | ~45,000 | 0.36 ms (93 % hits) |
| Leader failover (kill → next successful write) | — | median ~355–470 ms |

Pipelined replication, group commit and connection pooling took durable writes from 842/s to
~3,450/s; each optimisation was re-validated against the fault-injection and linearizability
suites. Every node exports Prometheus metrics on `/metrics` (term, leadership, commit/applied
index, WAL fsync latency, request latency by operation and outcome, watch streams).

## Architecture

```
 client ──HTTP──▶ any node ──421 + X-Leader──▶ leader
                                                 │ Propose
                                          ┌──────▼───────┐   AppendEntries / RequestVote
                                          │  raft.Node   │◀────────────────────────────▶ peers
                                          │ (event loop) │        (TCP, gob)
                                          └──────┬───────┘
                                                 │ committed entries, in order
                                          ┌──────▼───────┐
                                          │   kv.Store   │  map + per-client dedup table
                                          └──────────────┘
```

| Package | Role |
|---|---|
| `raft` | Consensus state machine (pure, deterministic) |
| `kv` | Replicated key-value state machine, dedup, versions |
| `wal` | Write-ahead log: checksummed records, fsync, torn-write recovery |
| `server` | Real-time event loop, TCP and in-memory transports, HTTP API |
| `client` | Leader discovery, redirects, retries with the same sequence number |
| `cmd/quorum` | Node binary |

Reads through `Client` go through the log: linearizable by construction (ReadIndex/leases would
make them cheaper). Reads through `CachedClient` trade linearizability for latency: read-your-writes,
monotonic reads and bounded staleness.

## Run a 3-node cluster

```bash
docker compose up -d --build
curl -X PUT --data Palaiseau localhost:8081/kv/city   # 421 + X-Leader if node 1 is a follower
curl localhost:8082/status
docker compose kill node2                              # if node2 is the leader: re-election
```

Observed locally: a new leader is elected ~110 ms after the leader is killed
(election timeout 150–300 ms, 10 ms ticks); after `docker compose stop` of all three nodes and a
restart, every key is still there (each node replays its WAL from its volume).

## Tests

```bash
go test ./...                                          # unit + integration + 3 s linearizability + cache staleness
make test-long                                         # 1,000 fault seeds + 20 s linearizability
RAFT_SEEDS=1000 go test -run Randomized ./raft         # long fault-injection run
LIN_SECONDS=20 go test -run Linearizable ./server      # long linearizability run
```

## Known limitations

- No log compaction / snapshots yet: the log grows forever and a restart replays it entirely.
- No membership changes, no PreVote (a node returning from a partition forces an election).
- Benchmarks run on a single machine over loopback; numbers on a real network will differ.
