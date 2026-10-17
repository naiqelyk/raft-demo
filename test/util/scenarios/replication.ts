import { kvObject, type Scenario } from "../sim";

export const minorityLeaderCantCommit: Scenario = {
  id: "minority-leader-cant-commit",
  title: "Leader in a minority can't commit",
  description:
    "The leader and one follower are cut off from the other three. The old leader still accepts a write but can never commit it. The majority elects a new leader and commits a different value, which replaces the old write after healing.",
  size: 5,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    const oldLeader = cluster.leader()!;
    const minority = [oldLeader.id, cluster.ids.find((id) => id !== oldLeader.id)!];
    sim.partition([minority, cluster.ids.filter((id) => !minority.includes(id))]);

    sim.note("The old leader still thinks it leads, so it accepts a write...");
    sim.check("the old leader accepts k=lost", sim.submit({ op: "put", key: "k", value: "lost" }).ok, true);
    sim.runUntil("the majority elects a new leader", () => cluster.leader() !== oldLeader);
    sim.run(100);
    sim.check("...but can't commit it: its log is just the no-op and the write", oldLeader.log.length, 2);
    sim.check("the old leader has committed nothing", oldLeader.commitIndex, 0);

    sim.submit({ op: "put", key: "k", value: "kept" });
    const newLeader = cluster.leader()!;
    sim.runUntil("the new leader commits k=kept", () => newLeader.kv.get("k") === "kept");

    sim.heal();
    sim.runUntil("every node has k=kept", () => cluster.ids.every((id) => sim.node(id).kv.get("k") === "kept"));
    sim.check("the old leader's log now matches the new leader's", oldLeader.log, newLeader.log);
  },
};

export const laggingFollower: Scenario = {
  id: "lagging-follower",
  title: "Lagging follower catches up",
  description:
    "A follower is cut off while the leader commits five writes. When it rejoins, the leader backs up until their logs match and sends the missing entries.",
  size: 3,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    const leader = cluster.leader()!;
    const laggard = cluster.ids.find((id) => id !== leader.id)!;
    sim.partition([[laggard], cluster.ids.filter((id) => id !== laggard)]);

    for (let i = 1; i <= 5; i++) sim.submit({ op: "put", key: `k${i}`, value: `${i}` });
    sim.runUntil("the leader commits all five", () => leader.kv.get("k5") === "5");
    sim.check(`${laggard} has missed everything`, sim.node(laggard).log.length, 0);

    sim.heal();
    sim.runUntil(`${laggard} catches up`, () => sim.node(laggard).lastApplied === leader.commitIndex);
    sim.check(`${laggard}'s store matches the leader's`, kvObject(sim.node(laggard)), kvObject(leader));
  },
};
