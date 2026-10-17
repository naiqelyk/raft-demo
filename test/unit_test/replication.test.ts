import { describe, expect, it } from "vitest";
import type { RaftNode } from "../../src/raft/node";
import type { LogEntry, Message, NodeId } from "../../src/raft/types";
import { appendEntries, entry, kvOf, newNode } from "../util/helpers";

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
