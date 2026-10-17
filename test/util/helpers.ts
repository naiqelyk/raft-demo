// Helpers shared by more than one test file. Helpers used by a single file
// stay next to the tests that use them.
import { expect } from "vitest";
import { RaftNode } from "../../src/raft/node";
import type { AppendEntries, LogEntry, NodeId } from "../../src/raft/types";
import { Sim, type Scenario } from "./sim";
import { Cluster } from "../../src/sim/cluster";
import { Rng } from "../../src/sim/rng";

/** A standalone node for unit tests, by default n1 in a 3-node cluster. */
export function newNode(id: NodeId = "n1", peers: NodeId[] = ["n2", "n3"]): RaftNode {
  return new RaftNode(id, peers, new Rng(1));
}

/** A log entry that puts `key`. */
export function entry(term: number, key = "k", value = `v${term}`): LogEntry {
  return { term, command: { op: "put", key, value } };
}

/** An AppendEntries from n2 to n1 in term 1; with no overrides it's an empty heartbeat. */
export function appendEntries(fields: Partial<AppendEntries> = {}): AppendEntries {
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

/** A node's key-value store as a plain object, for `toEqual`. */
export function kvOf(node: RaftNode): Record<string, string> {
  return Object.fromEntries(node.kv);
}

/** A cluster that has already elected a leader. */
export function electedCluster(size: number, seed = 1): Cluster {
  const cluster = new Cluster({ size, seed });
  cluster.runUntil(() => cluster.leader() !== undefined);
  return cluster;
}

/** Runs a shared scenario with every `sim.check` turned into an `expect` assertion. */
export function runScenario(scenario: Scenario): Sim {
  const sim = new Sim(scenario, {
    checkInvariants: scenario.checkInvariants ?? false,
    onCheck: (label, actual, expected) => expect(actual, label).toEqual(expected),
  });
  scenario.run(sim);
  return sim;
}
