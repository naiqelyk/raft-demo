import { describe, expect, it } from "vitest";
import { RaftNode } from "../../src/raft/node";
import type { Message, NodeId, RequestVote } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import { Rng } from "../../src/sim/rng";
import { electedCluster, kvOf } from "../util/helpers";

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
    cluster.runUntil(() => old.kv.get("b") === "2");
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
    const cluster = electedCluster(3);
    commit(cluster, "a", "1");
    const leader = cluster.leader()!;
    const committed = leader.commitIndex;
    const followers = cluster.upNodes().filter((n) => n !== leader).map((n) => n.id);
    for (const id of followers) cluster.crash(id);

    expect(cluster.submit({ op: "put", key: "b", value: "2" }).ok).toBe(true);
    cluster.run(200);
    expect(leader.commitIndex).toBe(committed);

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
    expect(cluster.node(oldId).log).toHaveLength(4); // no-op, a, and the two lost writes
    cluster.crash(oldId);

    // The others elect a leader, whose no-op and new write take those indices.
    cluster.runUntil(() => cluster.leader() !== undefined);
    commit(cluster, "x", "kept");

    cluster.restart(oldId);
    const old = cluster.node(oldId);
    cluster.runUntil(() => old.kv.get("x") === "kept");
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

  it("an old-term entry on a majority commits with the next leader's no-op, not by counting", () => {
    // Replays the scenario behind the current-term commit rule (see the README),
    // steering elections with partitions and crashes. Without the election no-op,
    // the old-term entry y=a could reach a majority without committing and later
    // be overwritten. With it, y=a travels with the next leader's no-op and
    // commits, and the conflicting entry is the one discarded. (The rule itself
    // is unit tested in unit_test/replication.test.ts.)
    const cluster = electedCluster(5);
    commit(cluster, "x", "1"); // log: no-op, x

    // (a) Leader A replicates y=a (index 3) to B only, then crashes. It can't commit (2 of 5).
    const a = leaderId(cluster);
    const b = cluster.ids.find((id) => id !== a)!;
    const rest = cluster.ids.filter((id) => id !== a && id !== b);
    cluster.network.partition([[a, b], rest]);
    cluster.submit({ op: "put", key: "y", value: "a" });
    cluster.runUntil(() => cluster.node(b).log.length === 3);
    cluster.crash(a);
    const oldTerm = cluster.node(b).termAt(3);

    // (b) One of the other three, E, wins with just their votes and appends its
    // no-op at index 3 in a newer term. Cut E off before that reaches anyone,
    // then crash it. Partitions apply at delivery, so its in-flight messages are lost.
    cluster.runUntil(() => cluster.leaders().some((n) => rest.includes(n.id)));
    const e = leaderId(cluster);
    const [r1, r2] = rest.filter((id) => id !== e) as [NodeId, NodeId];
    cluster.network.partition([[e], [b, r1, r2]]);
    cluster.crash(e);
    cluster.run(5);
    expect(cluster.node(r1).log).toHaveLength(2);

    // (c) A or B wins a later term (r1 and r2 have shorter logs, so they can't),
    // and sends y=a together with its own no-op. Both commit.
    cluster.restart(a);
    cluster.network.heal();
    cluster.runUntil(() => cluster.upNodes().every((n) => n.kv.get("y") === "a"));
    expect(cluster.node(r1).termAt(3)).toBe(oldTerm);

    // (d) Even with A and B down, E can't win any more: r1 and r2 have a newer
    // last term. One of them leads instead and overwrites E's conflicting no-op.
    cluster.crash(a);
    cluster.crash(b);
    cluster.restart(e);
    cluster.runUntil(() => cluster.node(e).kv.get("y") === "a");
    expect(cluster.leader()?.id).not.toBe(e);
    expect(cluster.node(e).termAt(3)).toBe(oldTerm);
  });
});
