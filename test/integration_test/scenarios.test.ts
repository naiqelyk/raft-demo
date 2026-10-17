import { describe, expect, it } from "vitest";
import { RaftNode } from "../../src/raft/node";
import type { Message, NodeId, RequestVote } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import { Rng } from "../../src/sim/rng";

function kvOf(node: RaftNode): Record<string, string> {
  return Object.fromEntries(node.kv);
}

function electedCluster(size: number, seed = 1): Cluster {
  const cluster = new Cluster({ size, seed });
  cluster.runUntil(() => cluster.leader() !== undefined);
  return cluster;
}

function leaderId(cluster: Cluster): NodeId {
  const leader = cluster.leader();
  if (!leader) throw new Error("no leader");
  return leader.id;
}

/** Submits a command and waits until every running node has applied it. */
function commit(cluster: Cluster, key: string, value: string): void {
  const result = cluster.submit({ op: "put", key, value });
  if (!result.ok) throw new Error("no leader to submit to");
  cluster.runUntil(() => cluster.upNodes().every((n) => n.lastApplied >= result.index));
}

describe("Crash and restart", () => {
  it("elects a new leader when the leader crashes; the old leader rejoins and catches up", () => {
    const cluster = electedCluster(3);
    commit(cluster, "a", "1");
    const oldId = leaderId(cluster);

    cluster.crash(oldId);
    cluster.runUntil(() => cluster.leader() !== undefined);
    expect(leaderId(cluster)).not.toBe(oldId);
    commit(cluster, "b", "2");

    cluster.restart(oldId);
    const old = cluster.node(oldId);
    cluster.runUntil(() => old.lastApplied === 2);
    expect(old.role).toBe("follower");
    expect(kvOf(old)).toEqual({ a: "1", b: "2" });
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
    expect(follower.log).toHaveLength(3);
    expect(follower.commitIndex).toBe(0);
    expect(kvOf(follower)).toEqual({});

    // The next heartbeat's leaderCommit lets it re-apply the log.
    cluster.runUntil(() => follower.lastApplied === 3);
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
    const cluster = electedCluster(3);
    commit(cluster, "a", "1");
    const leader = cluster.leader()!;
    const followers = cluster.upNodes().filter((n) => n !== leader).map((n) => n.id);
    for (const id of followers) cluster.crash(id);

    expect(cluster.submit({ op: "put", key: "b", value: "2" }).ok).toBe(true);
    cluster.run(200);
    expect(leader.commitIndex).toBe(1);

    cluster.restart(followers[0]!);
    cluster.runUntil(() => cluster.upNodes().every((n) => n.kv.get("b") === "2"));
    expect(kvOf(leader)).toEqual({ a: "1", b: "2" });
  });
});

describe("Failure scenarios", () => {
  it("overwrites a crashed leader's uncommitted entries when it rejoins", () => {
    const cluster = electedCluster(3);
    commit(cluster, "a", "1");
    const oldId = leaderId(cluster);

    // The leader accepts two writes and crashes before sending them: the
    // messages clientRequest returns are simply never sent.
    cluster.node(oldId).clientRequest({ op: "put", key: "x", value: "lost" });
    cluster.node(oldId).clientRequest({ op: "put", key: "y", value: "lost" });
    expect(cluster.node(oldId).log).toHaveLength(3);
    cluster.crash(oldId);

    // The others elect a leader and commit something else at index 2.
    cluster.runUntil(() => cluster.leader() !== undefined);
    commit(cluster, "x", "kept");

    cluster.restart(oldId);
    const old = cluster.node(oldId);
    cluster.runUntil(() => old.lastApplied === 2);
    expect(old.log).toEqual(cluster.leader()!.log);
    expect(kvOf(old)).toEqual({ a: "1", x: "kept" });
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

  it("never commits an old-term entry just because it reached a majority (Figure 8)", () => {
    // Follows Figure 8 of the Raft paper, steering elections with partitions and crashes.
    const cluster = electedCluster(5);
    commit(cluster, "x", "1");

    // (a) Leader A replicates y=a to B only, then crashes. It can't commit (2 of 5).
    const a = leaderId(cluster);
    const b = cluster.upNodes().find((n) => n.id !== a)!.id;
    const rest = cluster.upNodes().map((n) => n.id).filter((id) => id !== a && id !== b);
    cluster.network.partition([[a, b], rest]);
    cluster.submit({ op: "put", key: "y", value: "a" });
    cluster.runUntil(() => cluster.node(b).log.length === 2);
    cluster.crash(a);
    const oldTerm = cluster.node(b).termAt(2);

    // (b) E is elected by the other three (B's longer log isn't needed), accepts
    // y=e at index 2 in a newer term, and crashes before sending it anywhere.
    cluster.runUntil(() => cluster.leaders().some((n) => rest.includes(n.id)));
    const e = leaderId(cluster);
    const [r1, r2] = rest.filter((id) => id !== e) as [NodeId, NodeId];
    cluster.node(e).clientRequest({ op: "put", key: "y", value: "e" });
    cluster.crash(e);

    // (c) A or B wins a later term and spreads the old y=a entry to a majority.
    // It must not count replicas for an earlier term's entry, so nothing commits.
    cluster.restart(a);
    cluster.network.heal();
    cluster.runUntil(() =>
      [r1, r2].every((id) => cluster.node(id).termAt(2) === oldTerm),
    );
    cluster.run(50);
    for (const node of cluster.upNodes()) expect(node.commitIndex).toBe(1);

    // (d) With A and B down, only E can win: its index-2 entry has the newest
    // term. It overwrites y=a, which was safe only because y=a never committed.
    cluster.crash(a);
    cluster.crash(b);
    cluster.restart(e);
    cluster.runUntil(() => cluster.leader()?.id === e);
    commit(cluster, "z", "1");
    for (const node of cluster.upNodes()) {
      expect(node.termAt(2)).toBeGreaterThan(oldTerm);
      expect(kvOf(node)).toEqual({ x: "1", y: "e", z: "1" });
    }
  });
});
