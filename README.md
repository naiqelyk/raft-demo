# Raft Toy

A small, readable implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that replicates a key-value store across a cluster of simulated nodes. It includes tests for
the failure modes that matter and an interactive UI.

> **Status:** work in progress. The Raft core is done and tested, including an
> invariant checker and randomized chaos tests; the interactive UI is next.
> See [Progress](#progress).

## Quick start

```sh
npm install
npm test          # run the test suite
npm run typecheck # type-check without emitting
```

## Design

Everything runs in one process on a **deterministic simulated network**. Time is a tick
counter, and all randomness (election timeouts, message delays, drops) comes from one
seeded RNG, so any run, including a failing test, can be replayed exactly from its seed.

There are three pieces:

| Piece | File | Responsibility |
|---|---|---|
| `RaftNode` | `src/raft/node.ts` | Pure Raft logic. No I/O, no clock. Driven by `tick()` and `receive(msg)`, both of which **return** the messages to send. |
| `Network` | `src/sim/network.ts` | An unreliable mailbox: random delay (which also causes reordering), drop rate, and partitions. Decides whether and when each message arrives. |
| `Cluster` | `src/sim/cluster.ts` | Owns the clock and the nodes. Each `step()` delivers due messages to nodes, ticks every node, and sends what they return into the network. |

Nodes never talk to the network directly. That makes Raft logic easy to unit-test (call
`receive()`, check what it returns), and lets failures be injected entirely from the outside.

```
step():  now++
         network.deliverDue(now) ──▶ node.receive(msg) ──▶ network.send(...)
         node.tick() for every node ──────────────────────▶ network.send(...)
```

### Replication and the key-value store

Clients submit `put` / `delete` commands to the leader (`cluster.submit(cmd)`, or
`node.clientRequest(cmd)` directly). The leader appends the command to its log and sends
it out right away. After that, every heartbeat carries whatever entries each follower is
still missing, so a lost message is simply retried on the next heartbeat.

- **Consistency check:** a follower accepts entries only if its log already holds the
  entry just before them with the same term. If it rejects, the leader moves back one
  entry and retries, but never below the point that follower is already known to match.
- **Conflicts:** a follower deletes its own entries only from the first one whose term
  conflicts with the leader's. Entries it already matches are kept, so a delayed or
  duplicated message can't erase newer entries.
- **Commit:** the leader commits an entry once a majority stores it, **but only if the
  entry is from its own term**. Entries from earlier terms commit along with it (§5.4.2,
  Figure 8 of the paper). Followers learn the commit index from `leaderCommit`.
- **Apply:** each node applies committed commands in log order to its own `kv` map.

**Reads** come straight from any node's local `kv`. This is simple but can be stale: a
follower may lag behind, and a leader cut off by a partition may not know it has been
replaced. Linearizable reads would need to go through the log or confirm leadership
first. That's a deliberate trade-off for a toy.

A new leader doesn't append an empty entry when it takes over, so an entry left over from
an earlier term stays uncommitted until the next client write.

### Crash and restart

`cluster.crash(id)` stops a node: it stops ticking, and messages that reach it are lost.
Messages it sent before crashing are still delivered, just as they would be on a real
network. `cluster.restart(id)` builds a **new** `RaftNode` from a copy of the old one's
persistent state, so everything else starts fresh by construction:

| Survives a crash (on disk) | Lost on a crash (in memory) |
|---|---|
| `currentTerm`, `votedFor`, `log` | role, leader, `commitIndex`, `lastApplied`, the `kv` store, timers, `nextIndex` / `matchIndex` |

A restarted node starts with an empty store and `commitIndex = 0`. It learns the commit
index from the leader's next heartbeat and rebuilds the store by re-applying its log.

Losing any persistent field would break safety. Without `votedFor`, a node could vote
twice in one term and allow two leaders. Without `currentTerm`, it could accept an
outdated leader. Without the `log`, a committed entry could disappear.

Default timings (in ticks): message delay 1–3, heartbeat every 5, election timeout
randomized in 15–30.

### Invariants and chaos testing

`InvariantChecker` (`src/sim/invariants.ts`) checks the safety properties from Figure 3
of the paper. It keeps history, because two of them are about the past:

| Invariant | Meaning |
|---|---|
| Election safety | At most one leader per term, ever. |
| Log matching | If two logs have an entry with the same index and term, they're identical up to it. |
| Leader completeness | Every committed entry is in the log of every leader of a later term. |
| State machine safety | No two nodes ever commit (and so apply) different entries at the same index. |

The chaos test runs 50 seeds of 2000 ticks each, on 3 or 5 nodes with 5% message loss. On
random ticks it submits writes, crashes and restarts nodes (sometimes a majority), and
partitions or heals the network, and it **runs the checker after every tick**. A typical
seed sees about 100 writes, 10 crashes, 7 partitions, terms climbing past 30, and no
leader for about 20% of the time. Faults come from their own RNG derived from the seed,
so any failure replays exactly.

After the chaos it checks **liveness**: with faults stopped, a final write must reach
every node. Like a real client, the test resubmits when a new leader or term appears. A
write from an earlier term only commits once the new leader commits one of its own, so a
single unretried write can stall.

**How much does chaos catch on its own?** We broke one rule at a time and ran only the
chaos test:

| Deliberate bug | Caught by chaos (50 seeds) |
|---|---|
| Majority computed as ⌊n/2⌋ | 48 / 50 seeds |
| No up-to-date-log check when voting | 42 / 50 |
| Log not persisted across restart | 39 / 50 |
| Truncate even when entries match | 23 / 50 |
| `currentTerm` not persisted | 6 / 50 |
| No Figure 8 commit restriction | not caught |
| `votedFor` not persisted | not caught |
| Follower commit not capped by the message | can't happen here (see below) |

The misses need very specific interleavings, which is why the hand-written tests cover
them.
- Figure 8 needs a particular chain of leader changes.
- A lost `votedFor` only matters if a node crashes and restarts mid-election, between two
  candidates' requests.
- The commit cap can't bind in this implementation, because the leader always sends
  every entry up to the end of its log. It matters for implementations that send in
  batches.

Random testing and scenario tests complement each other: neither is enough alone.

## Progress

- [x] Project scaffold (TypeScript, Vitest)
- [x] Message types, seeded RNG, simulated network
- [x] Leader election + cluster harness
- [x] Log replication, commit rules, key-value state machine
- [x] Crash/restart with persistent state; scenario tests for failure modes
- [x] Invariant checker + randomized chaos tests
- [ ] Interactive UI (Vite + React)
- [ ] Final write-up

## What's tested so far

Tests are split by level: `test/unit_test/` checks the RNG, the network and individual
node rules by calling `receive()` directly, and `test/integration_test/` runs whole seeded
clusters, including the failure scenarios.

- **Network:** delivery within the delay range, reordering, drops, partitions and heal,
  determinism.
- **Election (node level):** election timeout, one vote per term, stale-term rejection,
  the up-to-date log check, winning on a majority, stepping down on a higher term,
  conceding to a leader of the same term, heartbeat timing.
- **Election (cluster level):** a single agreed leader; elections across sizes 3, 5 and 7
  and many seeds; a stable leader on a healthy network; re-election after the leader is
  partitioned away, with the old leader stepping down on heal; a minority partition
  cannot elect a leader; never two leaders in one term under 20% message loss.
- **Replication (node level):** rejecting entries that don't line up with the log,
  deleting only from the first conflicting entry (a stale message deletes nothing),
  capping the commit index at the end of the message, applying puts and deletes in order,
  committing only once a majority stores the entry, ignoring out-of-order replies, moving
  back one entry after a rejection, never committing an earlier term's entry by counting
  replicas (Figure 8), and a single-node cluster committing at once.
- **Replication (cluster level):** puts and deletes reach every node; a leader cut off in
  a minority accepts a write but can never commit it, and that entry is replaced after
  the partition heals; a lagging follower catches up after rejoining; every node ends up
  with the same store despite 20% message loss and longer delays, over several seeds.
- **Crash and restart:** a leader crash leads to a new leader, and the old one rejoins as
  a follower and catches up; a restarted node keeps its term and log and rebuilds its
  store; a node remembers its vote across a restart; with a majority crashed nothing
  commits, and nothing committed is lost once a node returns.
- **Failure scenarios:** a crashed leader's uncommitted entries are overwritten when it
  rejoins; re-delivering every old message (votes, AppendEntries, replies) changes
  nothing; and **Figure 8** of the paper, replayed with crashes and partitions on 5 nodes:
  an old-term entry that reaches a majority is not committed, and is later safely
  overwritten by a leader with a newer entry.
- **Invariant checker:** each of the four invariants is caught when a cluster's state is
  corrupted by hand, including a committed entry disappearing later, and a healthy run
  passes.
- **Chaos and liveness:** 50 seeds of random writes, crashes, restarts and partitions
  with all invariants checked every tick, then every node must apply a final write once
  faults stop.

## Out of scope

- Log compaction / snapshots
- Dynamic membership changes (joint consensus). The cluster size is chosen up front.
- Exactly-once client semantics (request deduplication)
- One-way (asymmetric) partitions: if A can't reach B, B can't reach A either
- PreVote: a node that was partitioned away rejoins with a higher term and forces the
  leader to step down. That's safe, but disruptive.
