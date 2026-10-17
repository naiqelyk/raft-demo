import type { NodeId } from "../../../src/raft/types";
import { kvObject, type Scenario } from "../sim";

// Scenarios with crashes. A restart replaces the node object, so these hold
// node ids and look nodes up again with `sim.node(id)`.

export const leaderCrash: Scenario = {
  id: "leader-crash",
  title: "Leader crashes and rejoins",
  description:
    "The leader crashes after a commit. The others elect a new leader and keep accepting writes. The old leader restarts as a follower and catches up.",
  size: 3,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    sim.commit({ op: "put", key: "a", value: "1" });
    const oldId = sim.leaderId();

    sim.crash(oldId);
    sim.runUntil("the others elect a new leader", () => cluster.leader() !== undefined);
    sim.assert("the new leader is a different node", sim.leaderId() !== oldId);
    sim.commit({ op: "put", key: "b", value: "2" });

    sim.restart(oldId);
    sim.runUntil(`${oldId} catches up`, () => sim.node(oldId).kv.get("b") === "2");
    sim.check(`${oldId} rejoined as a follower`, sim.node(oldId).role, "follower");
    sim.check(`${oldId} has both writes`, kvObject(sim.node(oldId)), { a: "1", b: "2" });
  },
};

export const majorityCrash: Scenario = {
  id: "majority-crash",
  title: "Majority crashed",
  description:
    "Both followers crash. The leader still accepts a write but can't commit it without a majority. When one follower returns, the write commits and nothing committed earlier is lost.",
  size: 3,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    sim.commit({ op: "put", key: "a", value: "1" });
    const leader = cluster.leader()!;
    const committed = leader.commitIndex;
    const followers = cluster.ids.filter((id) => id !== leader.id);
    for (const id of followers) sim.crash(id);

    sim.check("the leader accepts b=2", sim.submit({ op: "put", key: "b", value: "2" }).ok, true);
    sim.run(200);
    sim.check("...but can't commit it alone", leader.commitIndex, committed);

    sim.restart(followers[0]!);
    sim.runUntil("b=2 commits once a majority is back", () =>
      cluster.upNodes().every((n) => n.kv.get("b") === "2"),
    );
    sim.check("nothing committed was lost", kvObject(leader), { a: "1", b: "2" });
  },
};

export const unsentEntriesOverwritten: Scenario = {
  id: "unsent-entries-overwritten",
  title: "Crashed leader's writes are overwritten",
  description:
    "The leader accepts two writes and crashes before sending them. A new leader commits something else at those positions. When the old leader restarts, its uncommitted entries are replaced.",
  size: 3,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    sim.commit({ op: "put", key: "a", value: "1" });
    const oldId = sim.leaderId();

    // clientRequest returns the messages to send; dropping them means the
    // writes never leave the leader.
    sim.node(oldId).clientRequest({ op: "put", key: "x", value: "lost" });
    sim.node(oldId).clientRequest({ op: "put", key: "y", value: "lost" });
    sim.note(`${oldId} accepts x=lost and y=lost, but crashes before sending them`);
    sim.check(`${oldId}'s log: no-op, a, and the two lost writes`, sim.node(oldId).log.length, 4);
    sim.crash(oldId);

    sim.runUntil("the others elect a new leader", () => cluster.leader() !== undefined);
    sim.commit({ op: "put", key: "x", value: "kept" });

    sim.restart(oldId);
    sim.runUntil(`${oldId} learns x=kept`, () => sim.node(oldId).kv.get("x") === "kept");
    sim.check(`${oldId}'s log matches the leader's`, sim.node(oldId).log, cluster.leader()!.log);
    sim.check(`${oldId} never applied the lost writes`, kvObject(sim.node(oldId)), { a: "1", x: "kept" });
  },
};

export const oldTermEntry: Scenario = {
  id: "old-term-entry",
  title: "Old-term entry commits with the next no-op",
  description:
    "Through a series of crashes and partitions, an entry from an old term ends up on a majority. A leader never commits such an entry by counting copies. Here it commits together with the next leader's no-op, and a conflicting entry elsewhere is discarded.",
  size: 5,
  seed: 1,
  run(sim) {
    const { cluster } = sim;
    sim.runUntil("a leader is elected", () => cluster.leader() !== undefined);
    sim.commit({ op: "put", key: "x", value: "1" }); // log: no-op, x

    // (a) Leader A replicates y=a (index 3) to B only, then crashes.
    const a = sim.leaderId();
    const b = cluster.ids.find((id) => id !== a)!;
    const rest = cluster.ids.filter((id) => id !== a && id !== b);
    sim.partition([[a, b], rest]);
    sim.submit({ op: "put", key: "y", value: "a" });
    sim.runUntil(`${b} has y=a; it can't commit (2 of 5)`, () => sim.node(b).log.length === 3);
    sim.crash(a);
    const oldTerm = sim.node(b).termAt(3);

    // (b) One of the other three, E, wins with just their votes and appends its
    // no-op at index 3 in a newer term. Cut E off before that reaches anyone,
    // then crash it. Partitions apply at delivery, so its in-flight messages are lost.
    sim.runUntil("one of the other three wins", () => cluster.leaders().some((n) => rest.includes(n.id)));
    const e = sim.leaderId();
    const [r1, r2] = rest.filter((id) => id !== e) as [NodeId, NodeId];
    sim.partition([[e], [b, r1, r2]]);
    sim.crash(e);
    sim.run(5);
    sim.check(`${e}'s no-op never reached ${r1}`, sim.node(r1).log.length, 2);

    // (c) A or B wins a later term (r1 and r2 have shorter logs, so they can't),
    // and sends y=a together with its own no-op. Both commit.
    sim.restart(a);
    sim.heal();
    sim.runUntil("y=a commits with the new leader's no-op", () =>
      cluster.upNodes().every((n) => n.kv.get("y") === "a"),
    );
    sim.check(`${r1} holds the old-term entry`, sim.node(r1).termAt(3), oldTerm);

    // (d) Even with A and B down, E can't win any more: r1 and r2 have a newer
    // last term. One of them leads instead and overwrites E's conflicting no-op.
    sim.crash(a);
    sim.crash(b);
    sim.restart(e);
    sim.runUntil(`${e} learns y=a`, () => sim.node(e).kv.get("y") === "a");
    sim.assert(`${e} did not become leader`, cluster.leader()?.id !== e);
    sim.check(`${e}'s conflicting entry was replaced`, sim.node(e).termAt(3), oldTerm);
  },
};
