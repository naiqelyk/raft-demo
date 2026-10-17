import type { Scenario } from "../sim";

export const firstElection: Scenario = {
  id: "first-election",
  title: "First election",
  description:
    "Three nodes start as followers. The first to time out asks for votes, wins a majority, and every node ends up following it.",
  size: 3,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    sim.run(10); // let heartbeats reach everyone

    const leader = cluster.leader()!;
    sim.check("exactly one leader", cluster.leaders().length, 1);
    sim.check(
      "every node is in the leader's term",
      cluster.ids.map((id) => sim.node(id).currentTerm),
      cluster.ids.map(() => leader.currentTerm),
    );
    sim.check(
      "every node follows the leader",
      cluster.ids.map((id) => sim.node(id).leaderId),
      cluster.ids.map(() => leader.id),
    );
  },
};

export const leaderPartitioned: Scenario = {
  id: "leader-partitioned",
  title: "Leader partitioned away",
  description:
    "The leader is cut off from the other four. They elect a new leader in a higher term. When the network heals, the old leader sees that term and steps down.",
  size: 5,
  seed: 3,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    const oldLeader = cluster.leader()!;
    sim.partition([[oldLeader.id], cluster.ids.filter((id) => id !== oldLeader.id)]);

    sim.runUntil("the majority elects a new leader", () => cluster.leader()?.id !== oldLeader.id);
    const newLeader = cluster.leader()!;
    sim.assert("the new leader has a higher term", newLeader.currentTerm > oldLeader.currentTerm);
    sim.check("the isolated old leader still thinks it leads", oldLeader.role, "leader");

    sim.heal();
    sim.runUntil("the old leader sees the higher term and steps down", () => oldLeader.role === "follower");
    sim.check("only the new leader remains", cluster.leaders().map((n) => n.id), [newLeader.id]);
    sim.check("the old leader follows the new one", oldLeader.leaderId, newLeader.id);
  },
};

export const minorityNoLeader: Scenario = {
  id: "minority-no-leader",
  title: "Minority can't elect a leader",
  description:
    "Five nodes split 2 | 3. The two-node side keeps starting elections but can never win a majority; the three-node side elects a leader.",
  size: 5,
  seed: 4,
  run(sim) {
    const minority = ["n1", "n2"];
    sim.partition([minority, ["n3", "n4", "n5"]]);

    let minorityLed = false;
    for (let i = 0; i < 500; i++) {
      sim.run(1);
      if (minority.some((id) => sim.node(id).role === "leader")) minorityLed = true;
    }
    sim.assert("n1 and n2 never become leader", !minorityLed);
    sim.assert(
      "the majority side elected a leader",
      ["n3", "n4", "n5"].includes(sim.cluster.leader()?.id ?? ""),
    );
  },
};
