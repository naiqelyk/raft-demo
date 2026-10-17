import { describe, it } from "vitest";
import { chaosScenario } from "../util";
import { runScenario } from "../util/helpers";

const SEEDS = 50;

describe("Chaos", () => {
  it.each(Array.from({ length: SEEDS }, (_, i) => i + 1))(
    "seed %i: invariants hold under random faults, and the cluster recovers once they stop",
    (seed) => {
      runScenario(chaosScenario(seed));
    },
  );
});
