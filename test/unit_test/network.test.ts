import { describe, expect, it } from "vitest";
import { Network } from "../../src/sim/network";
import { Rng } from "../../src/sim/rng";
import type { Message, NodeId } from "../../src/raft/types";

function heartbeat(from: NodeId, to: NodeId, term = 1): Message {
  return {
    type: "AppendEntries",
    from,
    to,
    term,
    prevLogIndex: 0,
    prevLogTerm: 0,
    entries: [],
    leaderCommit: 0,
  };
}

/** Ticks the network from `start` to `end` inclusive, recording delivery times. */
function drain(net: Network, start: number, end: number) {
  const deliveries: { msg: Message; at: number }[] = [];
  for (let now = start; now <= end; now++) {
    for (const msg of net.deliverDue(now)) deliveries.push({ msg, at: now });
  }
  return deliveries;
}

describe("Network", () => {
  it("delivers every message within [minDelay, maxDelay]", () => {
    const net = new Network(new Rng(1), { minDelay: 2, maxDelay: 5 });
    for (let i = 0; i < 50; i++) net.send(heartbeat("n1", "n2", i), 0);

    const deliveries = drain(net, 0, 10);

    expect(deliveries).toHaveLength(50);
    for (const { at } of deliveries) {
      expect(at).toBeGreaterThanOrEqual(2);
      expect(at).toBeLessThanOrEqual(5);
    }
    expect(net.pending).toHaveLength(0);
  });

  it("reorders messages when delays vary", () => {
    const net = new Network(new Rng(1), { minDelay: 1, maxDelay: 5 });
    for (let term = 0; term < 20; term++) net.send(heartbeat("n1", "n2", term), 0);

    const terms = drain(net, 0, 10).map(({ msg }) => msg.term);

    expect(terms).not.toEqual([...terms].sort((a, b) => a - b));
  });

  it("drops everything when dropRate is 1", () => {
    const net = new Network(new Rng(1), { dropRate: 1 });
    for (let i = 0; i < 20; i++) net.send(heartbeat("n1", "n2"), 0);

    expect(drain(net, 0, 10)).toHaveLength(0);
  });

  it("blocks messages across a partition and restores them after heal", () => {
    const net = new Network(new Rng(1));
    net.partition([["n1", "n2"], ["n3"]]);

    net.send(heartbeat("n1", "n2"), 0);
    net.send(heartbeat("n1", "n3"), 0);
    const during = drain(net, 0, 10).map(({ msg }) => msg.to);
    expect(during).toEqual(["n2"]);

    net.heal();
    net.send(heartbeat("n1", "n3"), 11);
    const after = drain(net, 11, 20).map(({ msg }) => msg.to);
    expect(after).toEqual(["n3"]);
  });

  it("isolates nodes not listed in any partition group", () => {
    const net = new Network(new Rng(1));
    net.partition([["n1", "n2"]]);

    expect(net.canReach("n1", "n2")).toBe(true);
    expect(net.canReach("n1", "n3")).toBe(false);
    expect(net.canReach("n3", "n1")).toBe(false);
  });

  it("loses messages already in flight when a partition starts", () => {
    const net = new Network(new Rng(1), { minDelay: 3, maxDelay: 3 });
    net.send(heartbeat("n1", "n2"), 0);
    net.partition([["n1"], ["n2"]]);

    expect(drain(net, 0, 10)).toHaveLength(0);
  });

  it("is deterministic for the same seed", () => {
    const run = (seed: number) => {
      const net = new Network(new Rng(seed), { minDelay: 1, maxDelay: 4, dropRate: 0.3 });
      for (let i = 0; i < 30; i++) net.send(heartbeat("n1", "n2", i), 0);
      return drain(net, 0, 10).map(({ msg, at }) => `${msg.term}@${at}`);
    };

    expect(run(99)).toEqual(run(99));
    expect(run(99)).not.toEqual(run(100));
  });
});
