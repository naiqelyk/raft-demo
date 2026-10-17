import { describe, expect, it } from "vitest";
import { RaftNode } from "../src/raft/node";
import type { AppendEntries, LogEntry, Message, NodeId } from "../src/raft/types";
import { Cluster } from "../src/sim/cluster";
import { Rng } from "../src/sim/rng";

function newNode(id: NodeId = "n1", peers: NodeId[] = ["n2", "n3"]): RaftNode {
  return new RaftNode(id, peers, new Rng(1));
}

function entry(term: number, key = "k", value = `v${term}`): LogEntry {
  return { term, command: { op: "put", key, value } };
}

function appendEntries(fields: Partial<AppendEntries>): AppendEntries {
  return {
    type: "AppendEntries",
    from: "n2",
    to: "n1",
    term: 1,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
    ...fields,
  };
}

function success(msgs: Message[]): boolean {
  const [reply] = msgs;
  if (reply?.type !== "AppendEntriesResponse") throw new Error("expected an append response");
  return reply.success;
}

/** Times the node out and grants it peers' votes until it leads the next term. */
function makeLeader(node: RaftNode): void {
  for (let i = 0; i < 100 && node.role !== "candidate"; i++) node.tick();
  for (const peer of node.peers) {
    if (node.role === "leader") break;
    node.receive({
      type: "RequestVoteResponse",
      from: peer,
      to: node.id,
      term: node.currentTerm,
      voteGranted: true,
    });
  }
  if (node.role !== "leader") throw new Error("node did not become leader");
}

function ackFrom(from: NodeId, node: RaftNode, matchIndex: number): Message[] {
  return node.receive({
    type: "AppendEntriesResponse",
    from,
    to: node.id,
    term: node.currentTerm,
    success: true,
    matchIndex,
  });
}

function kvOf(node: RaftNode): Record<string, string> {
  return Object.fromEntries(node.kv);
}

function electedCluster(size: number, seed = 1): Cluster {
  const cluster = new Cluster({ size, seed });
  cluster.runUntil(() => cluster.leader() !== undefined);
  return cluster;
}

describe("RaftNode follower log handling", () => {
  it("rejects AppendEntries when it has no entry at prevLogIndex", () => {
    const node = newNode();
    node.log = [entry(1)];

    expect(success(node.receive(appendEntries({ prevLogIndex: 2, prevLogTerm: 1 })))).toBe(false);
  });

  it("rejects AppendEntries when the entry at prevLogIndex has a different term", () => {
    const node = newNode();
    node.currentTerm = 2;
    node.log = [entry(1)];

    const msg = appendEntries({ term: 2, prevLogIndex: 1, prevLogTerm: 2, entries: [entry(2)] });

    expect(success(node.receive(msg))).toBe(false);
    expect(node.log).toEqual([entry(1)]);
  });

  it("appends new entries and reports how far it matches", () => {
    const node = newNode();

    const [reply] = node.receive(appendEntries({ entries: [entry(1, "a"), entry(1, "b")] }));

    expect(reply).toMatchObject({ success: true, matchIndex: 2 });
    expect(node.log).toEqual([entry(1, "a"), entry(1, "b")]);
  });

  it("overwrites a conflicting suffix with the leader's entries", () => {
    const node = newNode();
    node.log = [entry(1, "a"), entry(1, "b"), entry(1, "c")];

    const msg = appendEntries({ term: 2, prevLogIndex: 1, prevLogTerm: 1, entries: [entry(2, "x")] });

    expect(success(node.receive(msg))).toBe(true);
    expect(node.log).toEqual([entry(1, "a"), entry(2, "x")]);
  });

  it("does not truncate on a stale AppendEntries whose entries already match", () => {
    const node = newNode();
    node.log = [entry(1, "a"), entry(1, "b"), entry(1, "c")];

    const [reply] = node.receive(appendEntries({ entries: [entry(1, "a")] }));

    expect(reply).toMatchObject({ success: true, matchIndex: 1 });
    expect(node.log).toHaveLength(3);
  });

  it("commits up to leaderCommit, but no further than the entries this message covers", () => {
    const node = newNode();
    node.log = [entry(1, "a"), entry(1, "b"), entry(1, "c")];

    // Entries 2 and 3 might not match the leader's log, so only 1 can be committed.
    node.receive(appendEntries({ entries: [entry(1, "a")], leaderCommit: 3 }));

    expect(node.commitIndex).toBe(1);
    expect(kvOf(node)).toEqual({ a: "v1" });
  });

  it("applies committed puts and deletes in log order", () => {
    const node = newNode();
    const entries: LogEntry[] = [
      entry(1, "a", "1"),
      entry(1, "b", "2"),
      { term: 1, command: { op: "delete", key: "a" } },
    ];

    node.receive(appendEntries({ entries, leaderCommit: 3 }));

    expect(node.lastApplied).toBe(3);
    expect(kvOf(node)).toEqual({ b: "2" });
  });
});

describe("RaftNode leader replication", () => {
  it("rejects client requests when not leader, with a hint", () => {
    const node = newNode();
    node.receive(appendEntries({}));

    expect(node.clientRequest({ op: "put", key: "k", value: "v" })).toEqual({
      ok: false,
      leaderHint: "n2",
    });
  });

  it("appends a client request and sends it to every peer", () => {
    const node = newNode();
    makeLeader(node);

    const result = node.clientRequest({ op: "put", key: "k", value: "v" });

    if (!result.ok) throw new Error("expected leader to accept");
    expect(result.index).toBe(1);
    expect(result.messages).toMatchObject([
      { to: "n2", prevLogIndex: 0, entries: [{ term: 1 }] },
      { to: "n3", prevLogIndex: 0, entries: [{ term: 1 }] },
    ]);
  });

  it("commits once a majority stores the entry, not before", () => {
    const node = newNode("n1", ["n2", "n3", "n4", "n5"]);
    makeLeader(node);
    node.clientRequest({ op: "put", key: "k", value: "v" });

    ackFrom("n2", node, 1);
    expect(node.commitIndex).toBe(0);

    ackFrom("n3", node, 1);
    expect(node.commitIndex).toBe(1);
    expect(kvOf(node)).toEqual({ k: "v" });
  });

  it("ignores reordered acknowledgements that would move matchIndex backwards", () => {
    const node = newNode();
    makeLeader(node);
    node.clientRequest({ op: "put", key: "a", value: "1" });
    node.clientRequest({ op: "put", key: "b", value: "2" });

    ackFrom("n2", node, 2);
    ackFrom("n2", node, 1); // late reply to the first request

    expect(node.commitIndex).toBe(2);
    // The next heartbeat to n2 starts after index 2: nothing to resend.
    let heartbeat: Message[] = [];
    while (heartbeat.length === 0) heartbeat = node.tick();
    expect(heartbeat[0]).toMatchObject({ to: "n2", prevLogIndex: 2, entries: [] });
  });

  it("backs off one entry on rejection and retries immediately", () => {
    const node = newNode();
    node.currentTerm = 1;
    node.log = [entry(1, "a"), entry(1, "b"), entry(1, "c")];
    makeLeader(node);

    const retry = node.receive({
      type: "AppendEntriesResponse",
      from: "n2",
      to: "n1",
      term: node.currentTerm,
      success: false,
      matchIndex: 0,
    });

    expect(retry).toMatchObject([{ to: "n2", prevLogIndex: 2, prevLogTerm: 1, entries: [{ term: 1 }] }]);
  });

  it("never commits an earlier term's entry by counting replicas (Figure 8)", () => {
    const node = newNode();
    node.currentTerm = 2;
    node.log = [entry(1, "a"), entry(2, "b")];
    makeLeader(node); // term 3

    // Index 2 is now on a majority, but it's from term 2, so it must not commit.
    ackFrom("n2", node, 2);
    expect(node.commitIndex).toBe(0);

    // Once an entry from term 3 reaches a majority, everything before it commits too.
    node.clientRequest({ op: "put", key: "c", value: "v3" });
    ackFrom("n2", node, 3);
    expect(node.commitIndex).toBe(3);
    expect(kvOf(node)).toEqual({ a: "v1", b: "v2", c: "v3" });
  });

  it("a single-node cluster commits immediately", () => {
    const node = newNode("n1", []);
    for (let i = 0; i < 100 && node.role !== "leader"; i++) node.tick();

    node.clientRequest({ op: "put", key: "k", value: "v" });

    expect(node.commitIndex).toBe(1);
    expect(kvOf(node)).toEqual({ k: "v" });
  });
});

describe("Cluster replication", () => {
  it("replicates puts and deletes to every node", () => {
    const cluster = electedCluster(3);

    cluster.submit({ op: "put", key: "a", value: "1" });
    cluster.submit({ op: "put", key: "b", value: "2" });
    cluster.submit({ op: "delete", key: "a" });
    cluster.runUntil(() => [...cluster.nodes.values()].every((n) => n.lastApplied === 3));

    for (const node of cluster.nodes.values()) {
      expect(kvOf(node)).toEqual({ b: "2" });
      expect(node.log).toEqual(cluster.leader()!.log);
    }
  });

  it("fails to submit when there is no leader", () => {
    const cluster = new Cluster({ size: 3 });
    expect(cluster.submit({ op: "put", key: "k", value: "v" }).ok).toBe(false);
  });

  it("a leader cut off in a minority cannot commit, and its entry is overwritten after healing", () => {
    const cluster = electedCluster(5);
    const oldLeader = cluster.leader()!;
    const minority = [oldLeader.id, [...cluster.nodes.keys()].find((id) => id !== oldLeader.id)!];
    const majority = [...cluster.nodes.keys()].filter((id) => !minority.includes(id));
    cluster.network.partition([minority, majority]);

    // The old leader still thinks it leads, so it accepts the write...
    expect(cluster.submit({ op: "put", key: "k", value: "lost" }).ok).toBe(true);
    // ...but can never commit it.
    cluster.runUntil(() => cluster.leader() !== oldLeader);
    cluster.run(100);
    expect(oldLeader.log).toHaveLength(1);
    expect(oldLeader.commitIndex).toBe(0);

    // The majority elects a new leader and commits a different value.
    cluster.submit({ op: "put", key: "k", value: "kept" });
    const newLeader = cluster.leader()!;
    cluster.runUntil(() => newLeader.commitIndex === 1);

    cluster.network.heal();
    cluster.runUntil(() => [...cluster.nodes.values()].every((n) => n.kv.get("k") === "kept"));
    expect(oldLeader.log).toEqual(newLeader.log);
  });

  it("a lagging follower catches up after rejoining", () => {
    const cluster = electedCluster(3);
    const leader = cluster.leader()!;
    const laggard = [...cluster.nodes.values()].find((n) => n !== leader)!;
    const rest = [...cluster.nodes.keys()].filter((id) => id !== laggard.id);
    cluster.network.partition([[laggard.id], rest]);

    for (let i = 1; i <= 5; i++) cluster.submit({ op: "put", key: `k${i}`, value: `${i}` });
    cluster.runUntil(() => leader.commitIndex === 5);
    expect(laggard.log).toHaveLength(0);

    cluster.network.heal();
    cluster.runUntil(() => laggard.lastApplied === 5);
    expect(kvOf(laggard)).toEqual(kvOf(leader));
  });

  it("converges despite message loss and delay", () => {
    for (const seed of [1, 2, 3, 4, 5]) {
      const cluster = new Cluster({ size: 5, seed, network: { dropRate: 0.2, maxDelay: 6 } });
      cluster.runUntil(() => cluster.leader() !== undefined);

      for (let i = 1; i <= 10; i++) {
        cluster.submit({ op: "put", key: `k${i % 3}`, value: `${i}` });
        cluster.run(3);
      }
      cluster.network.options.dropRate = 0;
      const nodes = [...cluster.nodes.values()];
      cluster.runUntil(() => {
        const leader = cluster.leader();
        return (
          leader !== undefined &&
          leader.commitIndex > 0 &&
          nodes.every((n) => n.lastApplied === leader.commitIndex)
        );
      }, 2000);

      for (const node of nodes) expect(kvOf(node)).toEqual(kvOf(nodes[0]!));
    }
  });
});
