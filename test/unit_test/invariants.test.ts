import { describe, expect, it } from "vitest";
import type { LogEntry } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import { InvariantChecker } from "../../src/sim/invariants";

function entry(term: number, key: string): LogEntry {
  return { term, command: { op: "put", key, value: "v" } };
}

// Each test corrupts a quiet cluster's state by hand and expects the checker to notice.
describe("InvariantChecker", () => {
  it("passes a healthy run", () => {
    const cluster = new Cluster({ size: 3, seed: 1 });
    const checker = new InvariantChecker();
    for (let i = 0; i < 300; i++) {
      if (i % 20 === 0) cluster.submit({ op: "put", key: `k${i}`, value: "v" });
      cluster.step();
      checker.check(cluster);
    }
    expect(cluster.leader()?.commitIndex).toBeGreaterThan(0);
  });

  it("catches two leaders in the same term", () => {
    const cluster = new Cluster({ size: 3 });
    for (const id of ["n1", "n2"]) {
      cluster.node(id).currentTerm = 1;
      cluster.node(id).role = "leader";
    }

    expect(() => new InvariantChecker().check(cluster)).toThrow(/Election safety/);
  });

  it("catches two leaders of the same term at different times", () => {
    const cluster = new Cluster({ size: 3 });
    const checker = new InvariantChecker();
    cluster.node("n1").currentTerm = 1;
    cluster.node("n1").role = "leader";
    checker.check(cluster);

    cluster.node("n1").role = "follower";
    cluster.node("n2").currentTerm = 1;
    cluster.node("n2").role = "leader";

    expect(() => checker.check(cluster)).toThrow(/Election safety/);
  });

  it("catches logs that agree at an index but differ before it", () => {
    const cluster = new Cluster({ size: 3 });
    cluster.node("n1").log = [entry(1, "a"), entry(2, "b")];
    cluster.node("n2").log = [entry(1, "x"), entry(2, "b")];

    expect(() => new InvariantChecker().check(cluster)).toThrow(/Log matching/);
  });

  it("catches two nodes committing different entries at the same index", () => {
    const cluster = new Cluster({ size: 3 });
    cluster.node("n1").log = [entry(1, "a")];
    cluster.node("n1").commitIndex = 1;
    cluster.node("n2").log = [entry(2, "b")];
    cluster.node("n2").commitIndex = 1;

    expect(() => new InvariantChecker().check(cluster)).toThrow(/State machine safety/);
  });

  it("catches a committed entry later disappearing from a node's log", () => {
    const cluster = new Cluster({ size: 3 });
    const checker = new InvariantChecker();
    cluster.node("n1").log = [entry(1, "a")];
    cluster.node("n1").commitIndex = 1;
    checker.check(cluster);

    cluster.node("n1").log = [entry(2, "b")];

    expect(() => checker.check(cluster)).toThrow(/State machine safety/);
  });

  it("catches a later-term leader that is missing a committed entry", () => {
    const cluster = new Cluster({ size: 3 });
    const checker = new InvariantChecker();
    cluster.node("n1").currentTerm = 1;
    cluster.node("n1").log = [entry(1, "a")];
    cluster.node("n1").commitIndex = 1;
    checker.check(cluster);

    cluster.node("n2").currentTerm = 2;
    cluster.node("n2").role = "leader";

    expect(() => checker.check(cluster)).toThrow(/Leader completeness/);
  });

  it("includes the tick in the error", () => {
    const cluster = new Cluster({ size: 3 });
    cluster.run(7);
    cluster.node("n1").log = [entry(1, "a"), entry(2, "b")];
    cluster.node("n2").log = [entry(1, "x"), entry(2, "b")];

    expect(() => new InvariantChecker().check(cluster)).toThrow(/at tick 7/);
  });
});
