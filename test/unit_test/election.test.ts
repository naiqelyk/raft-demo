import { describe, expect, it } from "vitest";
import type { RaftNode } from "../../src/raft/node";
import type { Message, NodeId, RequestVote } from "../../src/raft/types";
import { appendEntries, newNode } from "../util/helpers";

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

const heartbeat = (from: NodeId, term: number) => appendEntries({ from, term });

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
