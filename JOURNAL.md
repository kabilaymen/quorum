# Engineering journal

What was built, in the order it was built, and what went wrong along the way. The numbers are the
ones measured at the time (one Windows desktop, loopback); the final ones are in
[`bench/RESULTS.md`](bench/RESULTS.md).

> **How this project was built.** I wrote it with an AI coding assistant (Claude): I set the goals
> and acceptance criteria, ran and read every test and benchmark, and reviewed the code; much of
> the first-draft code came from the assistant. The mistakes and fixes below are the real ones from
> that process, not a cleaned-up story.

## 1. A Raft core that never touches the clock, the network or the disk

Started from the paper (Figure 2) with the etcd/raft shape: `Tick()`, `Step(msg)`, `Propose()`,
`Ready()`. Time and messages are inputs, so a whole cluster runs in one goroutine and a failing run
replays exactly from its seed.

Implemented first: randomized election timeouts, AppendEntries with conflict backtracking, the
election restriction (§5.4.1) and the current-term commit rule (§5.4.2, Figure 8) with a no-op entry
at the start of each term.

## 2. Randomized fault injection — and the first "bug" was in the test

The harness drops 10 % of messages, delays and reorders them, partitions and crashes nodes, and
asserts Election Safety and State Machine Safety after every round, then liveness at the end.

**First run: 4 seeds failed the liveness check.** Each time, the final command had been proposed to
a leader that was deposed right after, so the entry was overwritten. That is correct Raft behaviour
(an uncommitted entry can be lost; the client retries). The test was wrong, not the protocol: it now
retries the proposal, like a real client.

## 3. Key-value store, client, transports

- KV state machine with a per-key version (the index of the last write).
- Exactly-once writes: each command carries `(client id, seq)`, and the store remembers the last
  result per client, so a retry whose reply was lost is not applied twice.
- Followers answer `421` with `X-Leader`; the client follows the redirect and remembers the leader.
- TCP transport with gob and one long-lived connection per peer. A slow peer gets its messages
  dropped from a bounded queue instead of blocking the event loop; Raft retransmits.
- Docker cluster: after `docker compose kill` of the leader, a new leader is elected in ~110 ms
  (timeouts 150–300 ms, 10 ms ticks).

## 4. Linearizability: checked, not assumed

Concurrent clients against 5 nodes while a nemesis isolates a random node (the leader half the
time); the history is checked with [Porcupine](https://github.com/anishathalye/porcupine).

- **With 3 keys the checker returned "Unknown"** (timeout) on ~44,000 operations: checking
  linearizability is NP-complete, and few keys mean long per-key histories. With 10 keys each
  partition of the history is small and the check concludes.
- An always-green checker proves nothing, so `TestCheckerCatchesStaleRead` feeds it a history with a
  stale read and expects a rejection.
- Writes that timed out are recorded with an open end (they may take effect any time later); failed
  reads are dropped (no effect).

Result at the time: 48,834 operations over 20 s of partitions, linearizable.

## 5. Durability: the contract, then the WAL, then proving the tests can fail

`Ready` returns what must be on disk (term, vote, new entries) **before** any message is sent. In
the fault harness a crashed node now restarts only from what it persisted through that contract.

Mutation tests, to check the harness catches the classic bugs:

- "forget the entries" → State Machine Safety violation. Caught.
- **"forget the vote" → not caught on 1,000 seeds.** The failure needs vote → crash → vote again in
  the same term, which the 5-node random schedule almost never produced. Added a targeted test and
  a 3-node crash-churn variant (`TestRandomizedCrashChurn`); both now report two leaders in one
  term when the vote is forgotten.

Lesson kept: a test is only evidence once you have seen it fail for the bug it is meant to catch.

The WAL: append-only records `[length][CRC32C][entry]` with fsync; a torn or corrupted tail is cut
on recovery; the hard state is replaced atomically (temp file, fsync, rename).

## 6. CheckQuorum, and a regression it caused

Needed for the cache (next section): an isolated leader kept presenting itself as healthy. With
CheckQuorum, a leader that hears from no majority for an election timeout steps down.

**It broke `TestMinorityLeaderCannotCommit`.** The isolated old leader now steps down and, without
PreVote, keeps starting elections, so its term grows. When the partition healed, its higher term
forced a new election, and the entry the majority had just proposed — not yet committed — was
legitimately lost. Raft was right; the test had healed too early. It now waits for the majority
side to commit before healing. PreVote, which prevents this disruption, is still not implemented.

## 7. Coherent client cache

Each node streams `(key, version)` invalidations on `GET /watch`, plus a progress beat every
100 ms. `CachedClient` serves reads locally and evicts on a newer version.

Two races, closed explicitly:
1. A read returns version 10 after an invalidation for version 12 was announced → keep the highest
   announced version per key and refuse to cache anything older.
2. The stream drops during a read → an epoch counter: a read started before a reset is not cached.

Safe fallback: stream silent for 300 ms, or the watched node has no leader → empty the cache, go
back to linearizable reads. Measured under partitions: 86 % hits, 2.6 % stale reads, staleness p99
≈ 98 ms. Mutation (ignore invalidations) → both cache tests fail.

## 8. Performance: 842 → ~3,450 durable writes/s

First benchmark: **842 writes/s, p50 18.8 ms** (3 nodes, 16 clients, fsync). Measured layer by layer
instead of guessing:

| Path | Cost |
|---|---|
| Raft alone, in memory, 1 client | 34 µs/op |
| TCP transport alone | 138 µs/op |
| 16 concurrent clients | 3 ms/op |

So the problem was concurrency, not one slow layer. Three causes:

1. **No pipelining.** The leader did not advance `next[follower]` after sending, so every proposal
   resent the whole unacknowledged window. Now `next` advances optimistically and a rejection walks
   it back, never below `match+1`. Raft path: 5.3k → 172k ops/s.
2. **No group commit.** One fsync per event. The event loop now drains everything queued, then
   handles one `Ready`: one fsync and one round of messages per batch, and one commit-index
   broadcast per `Ready` instead of one per acknowledgement.
3. **Go's HTTP client keeps 2 idle connections per host**, so 16 clients kept reopening TCP
   connections. The client now keeps up to 256. HTTP path: 5.6k → 30k ops/s.

After each change: 1,000 fault seeds and the 20 s linearizability run again.
End result: ~3,450 writes/s, p50 4.65 ms; ~41,000/s without fsync.

## 9. Measurement mistakes

- **Fast operations showed "0 µs".** On Windows `time.Now()` advances in ~0.4–0.5 ms steps. The bench
  now measures the clock at startup, prints `< x ms` below it, and reports the mean from Little's
  law (clients / throughput), which does not depend on the clock. Fine-grained numbers will be
  redone on Linux.
- **Faster cluster, bigger history: the linearizability check ran out of memory** (~20 GB). The test
  clients are now paced (2 ms between operations). Long runs (1,000 seeds, 20 s of partitions) run
  in CI; local defaults stay short.

## Still missing

- Snapshots / log compaction (the log grows forever; restart replays everything).
- PreVote (a node coming back from a partition forces an election).
- Membership changes.
- ReadIndex or leases for cheaper linearizable reads.
