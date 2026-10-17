import { describe, expect, it } from "vitest";
import type { NodeId } from "../../src/raft/types";
import { firstElection, leaderPartitioned, minorityNoLeader } from "../util/scenarios/election";
import { Cluster } from "../../src/sim/cluster";
import { runScenario } from "../util/helpers";

describe("Cluster elections", () => {
  it("elects exactly one leader that every node recognises", () => {
    runScenario(firstElection);
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
    runScenario(leaderPartitioned);
  });

  it("a minority partition never elects a leader", () => {
    runScenario(minorityNoLeader);
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
