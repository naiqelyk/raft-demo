import { describe, expect, it } from "vitest";
import { RaftNode } from "../src/raft/node";
import type { AppendEntries, Message, NodeId, RequestVote } from "../src/raft/types";
import { Cluster } from "../src/sim/cluster";
import { Rng } from "../src/sim/rng";

function newNode(id: NodeId = "n1", peers: NodeId[] = ["n2", "n3"]): RaftNode {
  return new RaftNode(id, peers, new Rng(1));
}

/** Ticks a node until it times out and starts a new election. */
function startElection(node: RaftNode): Message[] {
  const term = node.currentTerm;
  for (let i = 0; i < 100; i++) {
    const msgs = node.tick();
    if (node.currentTerm > term) return msgs;
  }
  throw new Error("node never started an election");
}

function requestVote(from: NodeId, term: number, lastLogIndex = 0, lastLogTerm = 0): RequestVote {
  return { type: "RequestVote", from, to: "n1", term, lastLogIndex, lastLogTerm };
}

function heartbeat(from: NodeId, term: number): AppendEntries {
  return {
    type: "AppendEntries",
    from,
    to: "n1",
    term,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
  };
}

function voteGranted(msgs: Message[]): boolean {
  const [reply] = msgs;
  if (reply?.type !== "RequestVoteResponse") throw new Error("expected a vote response");
  return reply.voteGranted;
}

describe("RaftNode elections", () => {
  it("stays a follower until its election timeout", () => {
    const node = newNode();
    for (let i = 0; i < 14; i++) expect(node.tick()).toEqual([]);
    expect(node.role).toBe("follower");
  });

  it("starts an election after its timeout: new term, votes for itself, requests votes", () => {
    const node = newNode();
    const msgs = startElection(node);

    expect(node.role).toBe("candidate");
    expect(node.currentTerm).toBe(1);
    expect(node.votedFor).toBe("n1");
    expect(msgs.map((m) => [m.type, m.to, m.term])).toEqual([
      ["RequestVote", "n2", 1],
      ["RequestVote", "n3", 1],
    ]);
  });

  it("grants at most one vote per term", () => {
    const node = newNode();

    expect(voteGranted(node.receive(requestVote("n2", 1)))).toBe(true);
    expect(voteGranted(node.receive(requestVote("n3", 1)))).toBe(false);
    // A retried request from the same candidate is granted again.
    expect(voteGranted(node.receive(requestVote("n2", 1)))).toBe(true);
    expect(node.votedFor).toBe("n2");
  });

  it("rejects candidates from stale terms", () => {
    const node = newNode();
    node.currentTerm = 5;

    const [reply] = node.receive(requestVote("n2", 3));

    expect(voteGranted([reply!])).toBe(false);
    expect(reply!.term).toBe(5);
  });

  it("rejects candidates whose log is less up-to-date", () => {
    const node = newNode();
    node.log = [{ term: 2, command: { op: "put", key: "x", value: "1" } }];

    // Older last term loses, even with a longer log.
    expect(voteGranted(node.receive(requestVote("n2", 3, 5, 1)))).toBe(false);
    // Same last term but shorter log loses.
    expect(voteGranted(node.receive(requestVote("n2", 3, 0, 2)))).toBe(false);
    // Same last term and same length wins.
    expect(voteGranted(node.receive(requestVote("n2", 3, 1, 2)))).toBe(true);
  });

  it("becomes leader on a majority and immediately sends heartbeats", () => {
    const node = newNode();
    startElection(node);

    const msgs = node.receive({
      type: "RequestVoteResponse",
      from: "n2",
      to: "n1",
      term: 1,
      voteGranted: true,
    });

    expect(node.role).toBe("leader");
    expect(node.leaderId).toBe("n1");
    expect(msgs.map((m) => [m.type, m.to])).toEqual([
      ["AppendEntries", "n2"],
      ["AppendEntries", "n3"],
    ]);
  });

  it("ignores denied votes and votes from earlier terms", () => {
    const node = newNode();
    startElection(node); // term 1
    startElection(node); // timed out again: term 2

    node.receive({ type: "RequestVoteResponse", from: "n2", to: "n1", term: 2, voteGranted: false });
    node.receive({ type: "RequestVoteResponse", from: "n3", to: "n1", term: 1, voteGranted: true });

    expect(node.role).toBe("candidate");
    expect(node.currentTerm).toBe(2);
  });

  it("steps down when it sees a higher term", () => {
    const node = newNode();
    startElection(node);
    node.receive({ type: "RequestVoteResponse", from: "n2", to: "n1", term: 1, voteGranted: true });
    expect(node.role).toBe("leader");

    node.receive(heartbeat("n3", 5));

    expect(node.role).toBe("follower");
    expect(node.currentTerm).toBe(5);
    expect(node.leaderId).toBe("n3");
  });

  it("concedes when a leader of the same term appears", () => {
    const node = newNode();
    startElection(node);

    const [reply] = node.receive(heartbeat("n2", 1));

    expect(node.role).toBe("follower");
    expect(node.leaderId).toBe("n2");
    expect(reply).toMatchObject({ type: "AppendEntriesResponse", success: true });
  });

  it("rejects heartbeats from a stale leader", () => {
    const node = newNode();
    node.currentTerm = 3;

    const [reply] = node.receive(heartbeat("n2", 2));

    expect(reply).toMatchObject({ type: "AppendEntriesResponse", success: false, term: 3 });
    expect(node.leaderId).toBeNull();
  });

  it("leader sends heartbeats every heartbeatInterval ticks", () => {
    const node = newNode();
    startElection(node);
    node.receive({ type: "RequestVoteResponse", from: "n2", to: "n1", term: 1, voteGranted: true });

    for (let i = 0; i < 4; i++) expect(node.tick()).toEqual([]);
    expect(node.tick().map((m) => m.to)).toEqual(["n2", "n3"]);
  });

  it("a single-node cluster elects itself", () => {
    const node = newNode("n1", []);
    const msgs = startElection(node);

    expect(node.role).toBe("leader");
    expect(msgs).toEqual([]);
  });
});

describe("Cluster elections", () => {
  it("elects exactly one leader that every node recognises", () => {
    const cluster = new Cluster({ seed: 1 });
    cluster.runUntil(() => cluster.leader() !== undefined);
    cluster.run(10); // let heartbeats reach everyone

    const leader = cluster.leader()!;
    expect(cluster.leaders()).toHaveLength(1);
    for (const node of cluster.nodes.values()) {
      expect(node.currentTerm).toBe(leader.currentTerm);
      expect(node.leaderId).toBe(leader.id);
    }
  });

  it("elects a leader across cluster sizes and seeds", () => {
    for (const size of [3, 5, 7]) {
      for (let seed = 1; seed <= 20; seed++) {
        const cluster = new Cluster({ size, seed });
        cluster.runUntil(() => cluster.leader() !== undefined, 300);
      }
    }
  });

  it("keeps a stable leader while the network is healthy", () => {
    const cluster = new Cluster({ seed: 2 });
    cluster.runUntil(() => cluster.leader() !== undefined);
    const { id, currentTerm } = cluster.leader()!;

    cluster.run(1000);

    expect(cluster.leader()?.id).toBe(id);
    expect(cluster.leader()?.currentTerm).toBe(currentTerm);
  });

  it("re-elects when the leader is partitioned away, and the old leader steps down on heal", () => {
    const cluster = new Cluster({ size: 5, seed: 3 });
    cluster.runUntil(() => cluster.leader() !== undefined);
    const oldLeader = cluster.leader()!;
    const others = [...cluster.nodes.keys()].filter((id) => id !== oldLeader.id);

    cluster.network.partition([[oldLeader.id], others]);
    cluster.runUntil(() => cluster.leader()?.id !== oldLeader.id);
    const newLeader = cluster.leader()!;

    expect(newLeader.currentTerm).toBeGreaterThan(oldLeader.currentTerm);
    // The isolated node has heard nothing, so it still believes it leads.
    expect(oldLeader.role).toBe("leader");

    cluster.network.heal();
    cluster.runUntil(() => oldLeader.role === "follower");

    expect(cluster.leaders()).toEqual([newLeader]);
    expect(oldLeader.leaderId).toBe(newLeader.id);
  });

  it("a minority partition never elects a leader", () => {
    const cluster = new Cluster({ size: 5, seed: 4 });
    cluster.network.partition([
      ["n1", "n2"],
      ["n3", "n4", "n5"],
    ]);

    for (let i = 0; i < 500; i++) {
      cluster.step();
      expect(cluster.node("n1").role).not.toBe("leader");
      expect(cluster.node("n2").role).not.toBe("leader");
    }
    expect(["n3", "n4", "n5"]).toContain(cluster.leader()?.id);
  });

  it("no leader is elected when every node is isolated", () => {
    const cluster = new Cluster({ seed: 5 });
    cluster.network.partition([["n1"], ["n2"], ["n3"]]);

    cluster.run(500);

    expect(cluster.leaders()).toHaveLength(0);
    // They keep trying, so terms keep climbing.
    expect(cluster.node("n1").currentTerm).toBeGreaterThan(5);
  });

  it("never has two leaders in the same term, even with message loss and delay", () => {
    for (let seed = 1; seed <= 30; seed++) {
      const cluster = new Cluster({ size: 5, seed, network: { dropRate: 0.2, maxDelay: 6 } });
      const leaderOfTerm = new Map<number, NodeId>();

      for (let i = 0; i < 500; i++) {
        cluster.step();
        for (const leader of cluster.leaders()) {
          const existing = leaderOfTerm.get(leader.currentTerm);
          expect(existing ?? leader.id, `seed ${seed}, term ${leader.currentTerm}`).toBe(leader.id);
          leaderOfTerm.set(leader.currentTerm, leader.id);
        }
      }
    }
  });
});
