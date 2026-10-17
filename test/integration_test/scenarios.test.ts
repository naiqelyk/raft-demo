import { describe, expect, it } from "vitest";
import { RaftNode } from "../../src/raft/node";
import type { Message, NodeId, RequestVote } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import {
  leaderCrash,
  majorityCrash,
  oldTermEntry,
  unsentEntriesOverwritten,
} from "../util/scenarios/failures";
import { Rng } from "../../src/sim/rng";
import { electedCluster, kvOf, runScenario } from "../util/helpers";

/** Submits a command and waits until every running node has applied it. */
function commit(cluster: Cluster, key: string, value: string): void {
  const result = cluster.submit({ op: "put", key, value });
  if (!result.ok) throw new Error("no leader to submit to");
  cluster.runUntil(() => cluster.upNodes().every((n) => n.lastApplied >= result.index));
}

describe("Crash and restart", () => {
  it("elects a new leader when the leader crashes; the old leader rejoins and catches up", () => {
    runScenario(leaderCrash);
  });

  it("a restarted node keeps its term and log, and rebuilds its store from the leader", () => {
    const cluster = electedCluster(3);
    for (const key of ["a", "b", "c"]) commit(cluster, key, "v");
    const followerId = cluster.upNodes().find((n) => n.role === "follower")!.id;
    const termBefore = cluster.node(followerId).currentTerm;

    cluster.crash(followerId);
    cluster.restart(followerId);
    const follower = cluster.node(followerId);

    // Persistent state survived; volatile state starts over.
    expect(follower.currentTerm).toBe(termBefore);
    expect(follower.log).toHaveLength(4); // the election no-op, then a, b, c
    expect(follower.commitIndex).toBe(0);
    expect(kvOf(follower)).toEqual({});

    // The next heartbeat's leaderCommit lets it re-apply the log.
    cluster.runUntil(() => follower.lastApplied === 4);
    expect(kvOf(follower)).toEqual({ a: "v", b: "v", c: "v" });
  });

  it("remembers its vote across a restart, so it can't vote twice in one term", () => {
    const requestVote = (from: NodeId): RequestVote => ({
      type: "RequestVote",
      from,
      to: "n1",
      term: 1,
      lastLogIndex: 0,
      lastLogTerm: 0,
    });
    const rng = new Rng(1);
    const node = new RaftNode("n1", ["n2", "n3"], rng);
    node.receive(requestVote("n2"));

    const restarted = new RaftNode("n1", ["n2", "n3"], rng, undefined, node.persistentState());
    const [reply] = restarted.receive(requestVote("n3"));

    expect(reply).toMatchObject({ type: "RequestVoteResponse", voteGranted: false });
    expect(restarted.votedFor).toBe("n2");
  });

  it("makes no progress with a majority crashed, and loses nothing once one returns", () => {
    runScenario(majorityCrash);
  });
});

describe("Failure scenarios", () => {
  it("overwrites a crashed leader's uncommitted entries when it rejoins", () => {
    runScenario(unsentEntriesOverwritten);
  });

  it("ignores duplicated and reordered old messages", () => {
    const cluster = new Cluster({ size: 3, seed: 1 });
    // Record every message sent during the election and the first few writes.
    const seen = new Set<Message>();
    const record = () => cluster.network.pending.forEach((p) => seen.add(p.msg));
    cluster.runUntil(() => (record(), cluster.leader() !== undefined));
    for (const key of ["a", "b", "c"]) {
      cluster.submit({ op: "put", key, value: "v" });
      cluster.runUntil(() => (record(), cluster.upNodes().every((n) => n.kv.has(key))));
    }
    const leader = cluster.leader()!;
    const before = cluster.upNodes().map((n) => ({ log: [...n.log], kv: kvOf(n) }));

    // Deliver all of them again: stale votes, stale AppendEntries, stale replies.
    for (const msg of seen) cluster.network.send(msg, cluster.now);
    cluster.run(50);

    expect(cluster.leader()).toBe(leader);
    expect(cluster.upNodes().map((n) => ({ log: n.log, kv: kvOf(n) }))).toEqual(before);

    // And the cluster still works.
    commit(cluster, "d", "v");
  });

  it("an old-term entry on a majority commits with the next leader's no-op, not by counting", () => {
    runScenario(oldTermEntry);
  });
});
