# CLAUDE.md

Take-home for Eight Sleep: "Build a toy implementation of a consensus algorithm of your
choice, use it to replicate state across a small cluster of nodes, and write tests that
put it through its paces, including the failure modes you think matter."

We chose **Raft**, replicating a **key-value store**, in **TypeScript**, on a
**deterministic simulated network**, with a **Vite + React** UI at the end.

## Commands

```sh
npm test           # vitest run
npm run test:watch
npm run typecheck  # tsc --noEmit (strict, noUncheckedIndexedAccess)
```

## How we work (important)

- **Work in small steps.** One step per commit, then stop for review.
- **The user commits themselves.** Never run `git commit`. At the end of each step,
  suggest a commit message and wait.
- **Explain the design before writing a step.** Cover the files, interfaces and key rules,
  and wait for a go-ahead. The user often asks follow-up questions first.
- **Prefer simplicity and readability.** The user asked "can this be simpler?" once and
  we cut features. Don't add anything speculative.
- Before starting a step, run `git status`/`git log` to confirm the previous step was committed.

## Architecture

- `src/raft/types.ts`: `NodeId`, `Command` (`put` / `delete`), `LogEntry`, and the four
  RPC messages from Figure 2 of the paper. `AppendEntriesResponse` adds `matchIndex`
  (not in Figure 2) so the leader can handle reordered or duplicate responses.
- `src/sim/rng.ts`: mulberry32 seeded PRNG. **Never use `Math.random()`.** One RNG is
  shared by the network and all nodes in a cluster.
- `src/sim/network.ts`: in-flight list with `send(msg, now)`, `deliverDue(now)`,
  `partition(groups)` / `heal()`, `canReach`, `pending`, and mutable `options`
  (`minDelay`, `maxDelay`, `dropRate`). Drops are decided at send time; partitions are
  checked at delivery time, so in-flight messages are lost when a partition starts.
  Unlisted nodes are isolated. No duplication setting: duplicates will be tested by
  re-injecting a message in a targeted test.
- `src/raft/node.ts`: `RaftNode`. Pure logic, never does I/O: `tick()` and `receive(msg)`
  return `Message[]`. The election timer resets only on granting a vote, starting an
  election, or a valid AppendEntries (not just on seeing a higher term). Log is 1-based:
  entry i is `log[i - 1]`.
- `src/sim/cluster.ts`: `Cluster({ size, seed, network, raft })`, with nodes `n1..nN`.
  `step()` = `now++`, deliver due messages, then tick all nodes. Also `run`,
  `runUntil(cond, maxTicks)` (throws on timeout), `node(id)`, `leaders()` (all nodes that
  think they lead) and `leader()` (highest term).
- Tests live in `test/`. Node rules get unit tests by calling `receive()` directly;
  behaviour gets cluster tests with seeds.

Defaults: delay 1–3 ticks, heartbeat 5, election timeout 15–30.

## Plan and status

1. ✅ Scaffold (committed)
2. ✅ Types, RNG, network (committed)
3. ✅ Leader election + `Cluster` (`src/raft/node.ts`, `src/sim/cluster.ts`,
   `test/election.test.ts`, 30 tests passing). **Check `git status`: it may still be
   uncommitted.** Suggested message: `feat: implement Raft leader election and cluster harness`
4. ⏭ Log replication + commit + KV state machine
5. Crash/restart (persistent `currentTerm`/`votedFor`/`log` survive; volatile state is
   wiped) + scenario tests: leader crash, lagging follower catch-up, conflicting entries
   overwritten, stale leader can't commit in a minority, duplicate/reordered messages,
   the Figure 8 case
6. Invariant checker (election safety, log matching, leader completeness, state machine
   safety) run after every tick + randomized chaos test over many seeds + a liveness check
   once faults stop
7. UI: Vite + React on the same core. Choose cluster size (3–7) then reset; no dynamic
   membership. Show roles/terms, timeout rings, messages moving between nodes,
   logs with commit marks, and the KV store per node. Controls: put/delete, crash/restart,
   partition, step/play/pause, speed, seed.
8. Final README write-up

Out of scope: snapshots, membership changes, exactly-once client semantics.

## Next up: step 4 design (to present to the user before coding)

Temporary code to replace: `broadcastAppendEntries` sends empty heartbeats with
`leaderCommit: 0`, `handleAppendEntries` returns `success: true` without checking the log,
and `handleAppendEntriesResponse` is a no-op. All three are marked in `node.ts`.

Planned:
- Leader volatile state: `nextIndex` / `matchIndex` per peer, initialised on
  `becomeLeader` (`nextIndex = lastLogIndex + 1`, `matchIndex = 0`).
- `clientRequest(cmd)`: if leader, append `{ term, command }` and return
  `{ ok: true, index, messages }`; otherwise `{ ok: false, leaderHint }`.
- Each peer gets AppendEntries with `prevLogIndex = nextIndex - 1`, the entries from
  `nextIndex` onward, and `leaderCommit`.
- Follower: reject if there's no entry at `prevLogIndex` with term `prevLogTerm`; otherwise
  truncate only on a *conflicting* entry (don't truncate on matching entries, so stale
  or reordered AppendEntries are harmless), append new entries, then set
  `commitIndex = min(leaderCommit, last new index)`.
- Leader on response: on success, `matchIndex = max(matchIndex, msg.matchIndex)` and
  `nextIndex = matchIndex + 1`; on failure, decrement `nextIndex` (min 1) and retry. Keep
  the simple one-at-a-time backoff unless the user wants faster backoff.
- Commit: advance `commitIndex` to the highest N replicated on a majority **and**
  `log[N].term === currentTerm` (§5.4.2 / Figure 8).
- Apply: `lastApplied` advances to `commitIndex`, applying commands to a
  `Map<string, string>` KV store on each node.
- `cluster.submit(cmd)` routes to `leader()`.
- Tests: replication to all nodes, commit needs a majority, follower conflict truncation,
  catch-up of a lagging follower, leader never commits a previous-term entry by counting.

Open question to raise with the user: should reads go through the log (linearizable, but
slower) or be served from the leader's local state (simpler, but can be stale)? Leaning
toward a simple `get` on any node's KV for the toy, and documenting the trade-off.
