import { describe, expect, it } from "vitest";
import type { NodeId } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";

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
