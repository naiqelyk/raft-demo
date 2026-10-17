import { kvObject, type Scenario } from "../sim";

export const happyPath: Scenario = {
  id: "happy-path",
  title: "Happy path",
  description:
    "A healthy cluster on an imperfect network: about 1 in 10 messages is dropped and delays vary, so messages also arrive out of order. The leader keeps resending what each follower is missing, and every write reaches every node's store.",
  size: 5,
  seed: 1,
  network: { dropRate: 0.1, maxDelay: 5 },
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    const leader = cluster.leader()!;
    const term = leader.currentTerm;

    sim.commit({ op: "put", key: "a", value: "1" });
    sim.commit({ op: "put", key: "b", value: "2" });
    sim.commit({ op: "put", key: "c", value: "3" });
    sim.commit({ op: "put", key: "b", value: "20" });
    sim.commit({ op: "delete", key: "a" });

    sim.assert("some messages were dropped along the way", sim.lost.dropped > 0);
    sim.check("the same leader stayed in charge", [cluster.leader()?.id, cluster.leader()?.currentTerm], [leader.id, term]);
    sim.check(
      "every node has the same store",
      cluster.ids.map((id) => kvObject(sim.node(id))),
      cluster.ids.map(() => ({ b: "20", c: "3" })),
    );
    sim.check(
      "every node has the same log",
      cluster.ids.map((id) => sim.node(id).log),
      cluster.ids.map(() => leader.log),
    );
  },
};

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
