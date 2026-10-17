import { describe, expect, it } from "vitest";
import { laggingFollower, minorityLeaderCantCommit } from "../util/scenarios/replication";
import { Cluster } from "../../src/sim/cluster";
import { electedCluster, kvOf, runScenario } from "../util/helpers";

describe("Cluster replication", () => {
  it("replicates puts and deletes to every node", () => {
    const cluster = electedCluster(3);

    cluster.submit({ op: "put", key: "a", value: "1" });
    cluster.submit({ op: "put", key: "b", value: "2" });
    cluster.submit({ op: "delete", key: "a" });
    // The election no-op, then three commands.
    cluster.runUntil(() => [...cluster.nodes.values()].every((n) => n.lastApplied === 4));

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
    runScenario(minorityLeaderCantCommit);
  });

  it("a lagging follower catches up after rejoining", () => {
    runScenario(laggingFollower);
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
