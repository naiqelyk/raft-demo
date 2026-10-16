# Raft Toy

A small, readable implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that replicates a key-value store across a cluster of simulated nodes. It includes tests for
the failure modes that matter and an interactive UI.

> **Status:** work in progress. Leader election is done; log replication is next.
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

Default timings (in ticks): message delay 1–3, heartbeat every 5, election timeout
randomized in 15–30.

## Progress

- [x] Project scaffold (TypeScript, Vitest)
- [x] Message types, seeded RNG, simulated network
- [x] Leader election + cluster harness
- [ ] Log replication, commit rules, key-value state machine
- [ ] Crash/restart with persistent state; scenario tests for failure modes
- [ ] Invariant checker + randomized chaos tests
- [ ] Interactive UI (Vite + React)
- [ ] Final write-up

## What's tested so far

- **Network:** delivery within the delay range, reordering, drops, partitions and heal,
  determinism.
- **Election (node level):** election timeout, one vote per term, stale-term rejection,
  the up-to-date log check, winning on a majority, stepping down on a higher term,
  conceding to a leader of the same term, heartbeat timing.
- **Election (cluster level):** a single agreed leader; elections across sizes 3, 5 and 7
  and many seeds; a stable leader on a healthy network; re-election after the leader is
  partitioned away, with the old leader stepping down on heal; a minority partition
  cannot elect a leader; never two leaders in one term under 20% message loss.

## Out of scope

- Log compaction / snapshots
- Dynamic membership changes (joint consensus). The cluster size is chosen up front.
- Exactly-once client semantics (request deduplication)
