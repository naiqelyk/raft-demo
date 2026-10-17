import { describe, expect, it } from "vitest";
import type { NodeId } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import { InvariantChecker } from "../../src/sim/invariants";
import { Rng } from "../../src/sim/rng";

const SEEDS = 50;
const CHAOS_TICKS = 2000;
const RECOVERY_TICKS = 1000;

/** Per-tick probabilities of each fault or client action during the chaos phase. */
const CHANCE = {
  submit: 0.05,
  crash: 0.005,
  restart: 0.01,
  partition: 0.004,
  heal: 0.008,
};

function pick<T>(rng: Rng, items: T[]): T | undefined {
  return items.length === 0 ? undefined : items[rng.int(0, items.length - 1)];
}

/** Splits the nodes into up to three random groups. */
function randomPartition(rng: Rng, ids: NodeId[]): NodeId[][] {
  const groups: NodeId[][] = [[], [], []];
  for (const id of ids) groups[rng.int(0, 2)]!.push(id);
  return groups;
}

describe("Chaos", () => {
  it.each(Array.from({ length: SEEDS }, (_, i) => i + 1))(
    "seed %i: invariants hold under random faults, and the cluster recovers once they stop",
    (seed) => {
      // Faults come from their own RNG, so the network and nodes see the same
      // randomness whatever the chaos schedule is; the seed still replays it all.
      const chaos = new Rng(seed + 1_000_000);
      const cluster = new Cluster({
        size: chaos.chance(0.5) ? 3 : 5,
        seed,
        network: { dropRate: 0.05, maxDelay: 5 },
      });
      const ids = [...cluster.nodes.keys()];
      const checker = new InvariantChecker();
      const step = () => {
        cluster.step();
        checker.check(cluster);
      };

      // Safety: random writes, crashes, restarts and partitions, checked every tick.
      for (let tick = 0; tick < CHAOS_TICKS; tick++) {
        if (chaos.chance(CHANCE.submit)) {
          const key = pick(chaos, ["a", "b", "c"])!;
          cluster.submit(
            chaos.chance(0.2) ? { op: "delete", key } : { op: "put", key, value: `${tick}` },
          );
        }
        if (chaos.chance(CHANCE.crash)) {
          const id = pick(chaos, ids.filter((id) => cluster.isUp(id)));
          if (id) cluster.crash(id);
        }
        if (chaos.chance(CHANCE.restart)) {
          const id = pick(chaos, ids.filter((id) => !cluster.isUp(id)));
          if (id) cluster.restart(id);
        }
        if (chaos.chance(CHANCE.partition)) cluster.network.partition(randomPartition(chaos, ids));
        if (chaos.chance(CHANCE.heal)) cluster.network.heal();
        step();
      }

      // Liveness: once faults stop, a final write reaches every node. Like a real
      // client, we resubmit whenever a new leader or term appears, because a
      // write from an earlier term only commits once the new leader commits one
      // of its own.
      cluster.network.heal();
      cluster.network.options.dropRate = 0;
      for (const id of ids) if (!cluster.isUp(id)) cluster.restart(id);

      const value = `${seed}`;
      const applied = () => ids.every((id) => cluster.node(id).kv.get("final") === value);
      let submittedTo: string | undefined;
      for (let i = 0; i < RECOVERY_TICKS && !applied(); i++) {
        const leader = cluster.leader();
        const attempt = leader && `${leader.id}@${leader.currentTerm}`;
        if (attempt && attempt !== submittedTo) {
          cluster.submit({ op: "put", key: "final", value });
          submittedTo = attempt;
        }
        step();
      }
      expect(applied(), "every node applied the final write").toBe(true);

      const stores = ids.map((id) => Object.fromEntries(cluster.node(id).kv));
      for (const store of stores) expect(store).toEqual(stores[0]);
    },
  );
});
