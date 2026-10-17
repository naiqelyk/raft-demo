import type { NodeId } from "../../../src/raft/types";
import { Rng } from "../../../src/sim/rng";
import { kvObject, type Scenario } from "../sim";

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

/**
 * Random writes, crashes, restarts and partitions, with the invariant checker
 * run after every tick. Once the faults stop, a final write must reach every
 * node (liveness).
 */
export function chaosScenario(seed: number): Scenario {
  return {
    id: `chaos-${seed}`,
    title: `Chaos (seed ${seed})`,
    description:
      "Random writes, crashes, restarts and partitions for 2000 ticks, with Raft's safety properties checked after every tick. Then the faults stop and a final write must reach every node.",
    size: seed % 2 === 0 ? 3 : 5,
    seed,
    network: { dropRate: 0.05, maxDelay: 5 },
    checkInvariants: true,
    run(sim) {
      // Faults use their own RNG, so changing the chaos schedule doesn't change
      // the network's or nodes' randomness. The seed still replays everything.
      const chaos = new Rng(seed + 1_000_000);
      const { cluster } = sim;
      const ids = cluster.ids;

      for (let tick = 0; tick < CHAOS_TICKS; tick++) {
        if (chaos.chance(CHANCE.submit)) {
          const key = pick(chaos, ["a", "b", "c"])!;
          sim.submit(chaos.chance(0.2) ? { op: "delete", key } : { op: "put", key, value: `${tick}` });
        }
        if (chaos.chance(CHANCE.crash)) {
          const id = pick(chaos, ids.filter((id) => cluster.isUp(id)));
          if (id) sim.crash(id);
        }
        if (chaos.chance(CHANCE.restart)) {
          const id = pick(chaos, ids.filter((id) => !cluster.isUp(id)));
          if (id) sim.restart(id);
        }
        if (chaos.chance(CHANCE.partition)) sim.partition(randomPartition(chaos, ids));
        if (chaos.chance(CHANCE.heal)) sim.heal();
        sim.run(1);
      }

      // Liveness: once faults stop, a final write reaches every node. Like a real
      // client, we resubmit whenever a new leader or term appears, because a
      // write sent to a leader that is then replaced may be lost.
      sim.note("Faults stop: heal, restart every node, no more message loss");
      sim.heal();
      cluster.network.options.dropRate = 0;
      for (const id of ids) if (!cluster.isUp(id)) sim.restart(id);

      const value = `${seed}`;
      const applied = () => ids.every((id) => sim.node(id).kv.get("final") === value);
      let submittedTo: string | undefined;
      for (let i = 0; i < RECOVERY_TICKS && !applied(); i++) {
        const leader = cluster.leader();
        const attempt = leader && `${leader.id}@${leader.currentTerm}`;
        if (attempt && attempt !== submittedTo) {
          sim.submit({ op: "put", key: "final", value });
          submittedTo = attempt;
        }
        sim.run(1);
      }
      sim.check("every node applied the final write", applied(), true);

      const stores = ids.map((id) => kvObject(sim.node(id)));
      sim.check("every node has the same store", stores, ids.map(() => stores[0]));
    },
  };
}

function pick<T>(rng: Rng, items: T[]): T | undefined {
  return items.length === 0 ? undefined : items[rng.int(0, items.length - 1)];
}

/** Splits the nodes into up to three random groups. */
function randomPartition(rng: Rng, ids: NodeId[]): NodeId[][] {
  const groups: NodeId[][] = [[], [], []];
  for (const id of ids) groups[rng.int(0, 2)]!.push(id);
  return groups;
}
