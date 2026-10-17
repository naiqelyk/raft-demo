import type { RaftNode } from "../raft/node";
import type { Command, LogEntry, NodeId } from "../raft/types";
import type { Cluster } from "./cluster";

/**
 * Checks Raft's safety properties (Figure 3 of the paper). Call `check()` after
 * every step. It keeps history, because two of the properties are about the
 * past: who led each term, and which entries have ever been committed.
 */
export class InvariantChecker {
  private leaderOfTerm = new Map<number, NodeId>();
  /**
   * Every committed entry seen so far, with the term of the node that first
   * reported it. That term is at or after the term the entry was committed in,
   * so leader completeness is only checked against leaders of a strictly later
   * term. That can miss a narrow case, but never reports a false violation.
   */
  private committed = new Map<number, { entry: LogEntry; term: number }>();

  /** Throws if any invariant is violated, naming it, the nodes involved and the tick. */
  check(cluster: Cluster): void {
    // Crashed nodes still count: their logs are on disk.
    const nodes = [...cluster.nodes.values()];
    const violation =
      this.electionSafety(cluster.leaders()) ??
      this.logMatching(nodes) ??
      this.stateMachineSafety(nodes) ??
      this.leaderCompleteness(cluster.leaders());
    if (violation) throw new Error(`Invariant violated at tick ${cluster.now}: ${violation}`);
  }

  /** At most one leader per term, ever. */
  private electionSafety(leaders: RaftNode[]): string | undefined {
    for (const leader of leaders) {
      const previous = this.leaderOfTerm.get(leader.currentTerm);
      if (previous !== undefined && previous !== leader.id) {
        return `Election safety: ${previous} and ${leader.id} both led term ${leader.currentTerm}`;
      }
      this.leaderOfTerm.set(leader.currentTerm, leader.id);
    }
  }

  /** If two logs share an entry's index and term, they are identical up to it. */
  private logMatching(nodes: RaftNode[]): string | undefined {
    for (const [i, a] of nodes.entries()) {
      for (const b of nodes.slice(i + 1)) {
        // The highest index where the terms agree; everything up to it must match.
        let last = Math.min(a.lastLogIndex(), b.lastLogIndex());
        while (last > 0 && a.termAt(last) !== b.termAt(last)) last--;
        for (let index = 1; index <= last; index++) {
          if (!sameEntry(a.log[index - 1], b.log[index - 1])) {
            return `Log matching: ${a.id} and ${b.id} agree at index ${last} but differ at ${index}`;
          }
        }
      }
    }
  }

  /** No two nodes ever commit (and so apply) different entries at the same index. */
  private stateMachineSafety(nodes: RaftNode[]): string | undefined {
    for (const node of nodes) {
      for (let index = 1; index <= node.commitIndex; index++) {
        const entry = node.log[index - 1];
        if (!entry) return `State machine safety: ${node.id} committed index ${index} but has no entry there`;
        const known = this.committed.get(index);
        if (!known) {
          this.committed.set(index, { entry, term: node.currentTerm });
        } else if (!sameEntry(entry, known.entry)) {
          return `State machine safety: ${node.id} committed a different entry at index ${index}`;
        }
      }
    }
  }

  /** Every committed entry is in the log of every leader of a later term. */
  private leaderCompleteness(leaders: RaftNode[]): string | undefined {
    for (const leader of leaders) {
      for (const [index, { entry, term }] of this.committed) {
        if (leader.currentTerm > term && !sameEntry(leader.log[index - 1], entry)) {
          return `Leader completeness: ${leader.id} (term ${leader.currentTerm}) is missing committed index ${index}`;
        }
      }
    }
  }
}

function sameEntry(a: LogEntry | undefined, b: LogEntry | undefined): boolean {
  if (!a || !b) return a === b;
  return a.term === b.term && sameCommand(a.command, b.command);
}

function sameCommand(a: Command, b: Command): boolean {
  if (a.op === "put" && b.op === "put") return a.key === b.key && a.value === b.value;
  return a.op === b.op && a.key === b.key;
}
