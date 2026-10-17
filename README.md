# Raft Toy

A small, readable implementation of the [Raft consensus algorithm](https://raft.github.io/raft.pdf)
that replicates a key-value store across a cluster of simulated nodes. It includes tests for
the failure modes that matter and an interactive UI.

> **Status:** work in progress. Leader election and log replication are done;
> crash/restart and failure-mode scenarios are next.
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

Default timings (in ticks): message delay 1–3, heartbeat every 5, election timeout
randomized in 15–30.

## Progress

- [x] Project scaffold (TypeScript, Vitest)
- [x] Message types, seeded RNG, simulated network
- [x] Leader election + cluster harness
- [x] Log replication, commit rules, key-value state machine
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

## Out of scope

- Log compaction / snapshots
- Dynamic membership changes (joint consensus). The cluster size is chosen up front.
- Exactly-once client semantics (request deduplication)
