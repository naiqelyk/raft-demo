import { describe, expect, it } from "vitest";
import { record } from "../util/record";
import { chaosScenario, SCENARIOS } from "../util";
import type { Scenario } from "../util/sim";
import { firstElection, minorityNoLeader } from "../util/scenarios/election";
import { leaderCrash } from "../util/scenarios/failures";

// record() is what the UI plays back, so check it captures what the UI needs.
describe("record", () => {
  it.each(SCENARIOS.map((s) => [s.id, s] as const))("%s records cleanly", (_, scenario) => {
    const { frames, events, error } = record(scenario);

    expect(error).toBeUndefined();
    expect(events.filter((e) => e.kind === "check" && !e.passed)).toEqual([]);
    expect(events.some((e) => e.kind === "check")).toBe(true);
    // One frame per tick, starting from tick 0.
    frames.forEach((frame, i) => expect(frame.tick).toBe(i));
  });

  it("records a chaos run, including dropped messages, with no invariant violations", () => {
    const { frames, events, error } = record(chaosScenario(1));

    expect(error).toBeUndefined();
    expect(events.filter((e) => e.kind === "check" && !e.passed)).toEqual([]);
    expect(frames.length).toBeGreaterThan(2000);
    expect(frames.some((f) => f.lost.some((l) => l.reason === "dropped"))).toBe(true);
  });

  it("captures node state, messages in flight and leader changes", () => {
    const { frames, events } = record(firstElection);
    const last = frames.at(-1)!;

    expect(last.nodes.map((n) => n.id)).toEqual(["n1", "n2", "n3"]);
    expect(last.nodes.filter((n) => n.role === "leader")).toHaveLength(1);
    expect(frames.some((f) => f.inFlight.length > 0)).toBe(true);
    expect(events.some((e) => e.kind === "state" && e.text.includes("becomes leader"))).toBe(true);
  });

  it("captures partitions and the messages they cut off", () => {
    const { frames } = record(minorityNoLeader);

    expect(frames.at(-1)!.partition).toEqual([["n1", "n2"], ["n3", "n4", "n5"]]);
    expect(frames.some((f) => f.lost.some((l) => l.reason === "partitioned"))).toBe(true);
  });

  it("shows an action's effect in the frame of the tick it happened", () => {
    const { frames, events } = record(leaderCrash);
    const crash = events.find((e) => e.kind === "action" && e.text.startsWith("crash"))!;
    const id = crash.text.replace("crash ", "");

    expect(frames[crash.tick]!.nodes.find((n) => n.id === id)!.up).toBe(false);
  });

  it("records messages that reach a crashed node as lost", () => {
    const { frames } = record(leaderCrash);

    expect(frames.some((f) => f.lost.some((l) => l.reason === "crashed"))).toBe(true);
  });

  it("keeps the frames and reports the error when a script fails", () => {
    const stuck: Scenario = {
      ...firstElection,
      run(sim) {
        sim.partition([["n1"], ["n2"], ["n3"]]);
        sim.runUntil("a leader is elected", () => sim.cluster.leader() !== undefined, 50);
      },
    };

    const { frames, events, error } = record(stuck);

    expect(error).toMatch(/not met within 50 ticks/);
    expect(frames).toHaveLength(51);
    expect(events.at(-1)).toMatchObject({ kind: "check", passed: false });
  });
});
