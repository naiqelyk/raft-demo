# Raft Toy

An example implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that replicates a key-value store across a cluster of simulated nodes, with tests for different failure modes.

```sh
npm install
npm test          # run the test suite
npm run typecheck # type-check without emitting
npm run dev       # open the timelapse viewer
```

## Design

Everything runs in one process on a **deterministic simulated network**. Time is a tick
counter, and all randomness (timeouts, delays, drops) comes from one seeded RNG, so any
run, including a failing test, replays exactly from its seed.

| Piece | File | Role |
|---|---|---|
| `RaftNode` | `src/raft/node.ts` | Pure Raft logic, with no I/O and no clock. `tick()` and `receive(msg)` **return** the messages to send. |
| `Network` | `src/sim/network.ts` | Unreliable delivery: random delay (so reordering), drops, and partitions. |
| `Cluster` | `src/sim/cluster.ts` | Owns the clock. Each `step()` delivers due messages, ticks every node, and sends what they return. Also crash/restart. |

```
cluster.step():
  now++
  network.deliverDue(now) ──▶ node.receive(msg) ──┐
  node.tick() for every running node ─────────────┼──▶ network.send(msg, now)
cluster.submit(cmd) ──▶ leader.clientRequest(cmd) ┘
```

Nodes never touch the network. They **return** messages, and the cluster is the only
thing that moves them. The network decides each message's fate twice: drop and delay
when it's sent, and partitions when it's delivered, so messages already in flight when a
partition starts are lost. This keeps Raft rules unit-testable (call `receive()` and check
what it returns), and every fault is injected from outside.

## Key Raft decisions

- **Replication:** the leader sends each follower everything it's missing, on every
  heartbeat, so lost messages are simply retried. A follower only truncates its log at a
  real conflict, so a delayed or duplicated message can't erase newer entries.
- **Commit:** an entry commits once a majority stores it, but a leader only counts
  copies of entries **from its own term**. An older entry on a majority can still be
  overwritten by a node whose log ends in a newer term. Earlier entries commit along
  with a current-term entry instead.
- **Election no-op:** a new leader immediately appends an empty entry from its own term,
  so entries left over from earlier terms commit right away instead of waiting for the
  next client write.
- **Crash/restart:** only `currentTerm`, `votedFor` and the `log` survive a crash, as if
  on disk. A restart builds a fresh node from them, and it rebuilds its key-value store
  by re-applying the log once it learns the commit index.
- **Reads** come from any node's local store. That's simple but can be stale, which is
  a deliberate trade-off for a toy.

## Testing

- `test/unit_test/`: the RNG, the network, and individual node rules (elections,
  replication, commit) called directly.
- `test/integration_test/`: seeded clusters. This covers elections and replication under
  partitions and message loss, crash and restart, and failure scenarios: overwritten
  uncommitted entries, re-delivered old messages, and an old-term entry that reaches a
  majority during a series of leader changes. Most of these are **scenario scripts** in
  `test/util/scenarios/`: plain functions that act through a small `Sim` wrapper
  (`test/util/sim.ts`: partition, crash, submit, wait, check). The tests run them with
  each check as an assertion, and the UI will replay the same scripts as a timelapse.
- **Invariant checker** (`src/sim/invariants.ts`): checks Raft's four safety properties.
  - At most one leader per term.
  - Logs that agree at an entry agree on everything before it.
  - Every committed entry is in every later leader's log.
  - No two nodes commit different entries at the same index.
- **Chaos test:** 50 seeds of 2000 ticks with random writes, crashes, restarts and
  partitions, with the invariant checker run after **every tick**. Once faults stop,
  a final write must reach every node (liveness).

## Out of scope

Log compaction, membership changes, exactly-once client semantics, one-way partitions,
and PreVote. Without PreVote, a node returning from a partition with a higher term forces
the leader to step down, which is safe but disruptive.
