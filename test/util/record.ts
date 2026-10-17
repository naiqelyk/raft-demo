import type { Role } from "../../src/raft/node";
import type { LogEntry, Message, NodeId } from "../../src/raft/types";
import type { InFlight, LossReason } from "../../src/sim/network";
import { kvObject, Sim, type Scenario, type SimEvent } from "./sim";

/** One node as it was at the end of a tick. */
export interface NodeFrame {
  id: NodeId;
  up: boolean;
  role: Role;
  term: number;
  leaderId: NodeId | null;
  log: readonly LogEntry[];
  commitIndex: number;
  lastApplied: number;
  kv: Record<string, string>;
  /** 0 to 1: how far the election timer has run. */
  electionProgress: number;
}

/** The whole cluster at the end of a tick. Frame i is tick i; frame 0 is the start. */
export interface Frame {
  tick: number;
  nodes: NodeFrame[];
  inFlight: readonly InFlight[];
  /** Messages lost since the previous frame. */
  lost: { msg: Message; reason: LossReason }[];
  partition: NodeId[][] | null;
}

export interface Recording {
  scenario: Scenario;
  frames: Frame[];
  events: SimEvent[];
  /** Set if the script threw, e.g. a `runUntil` that timed out. */
  error?: string;
}

/**
 * Runs a scenario and records a frame after every tick, for the UI to play
 * back. The invariant checker always runs, so a violation shows up as a failed
 * check on the timeline.
 */
export function record(scenario: Scenario): Recording {
  const frames: Frame[] = [];
  let lost: Frame["lost"] = [];

  const sim: Sim = new Sim(scenario, {
    checkInvariants: true,
    afterStep: () => {
      const frame = snapshot(sim, lost);
      lost = [];
      noteStateChanges(sim, frames.at(-1), frame);
      frames.push(frame);
    },
    // Actions happen between ticks. Re-take the current frame so that frame t
    // shows the state after tick t's actions, e.g. a node crashed at t is down.
    afterAction: () => {
      const previous = frames.pop()!;
      frames.push(snapshot(sim, [...previous.lost, ...lost]));
      lost = [];
    },
  });
  sim.cluster.network.onLost = (msg, reason) => lost.push({ msg, reason });
  frames.push(snapshot(sim, []));

  let error: string | undefined;
  try {
    scenario.run(sim);
  } catch (e) {
    error = (e as Error).message;
    sim.event("check", `scenario stopped: ${error}`, false);
  }
  return error === undefined
    ? { scenario, frames, events: sim.events }
    : { scenario, frames, events: sim.events, error };
}

function snapshot(sim: Sim, lost: Frame["lost"]): Frame {
  const { cluster } = sim;
  return {
    tick: cluster.now,
    nodes: cluster.ids.map((id) => {
      const node = cluster.node(id);
      return {
        id,
        up: cluster.isUp(id),
        role: node.role,
        term: node.currentTerm,
        leaderId: node.leaderId,
        log: [...node.log], // entries are never mutated, so a shallow copy is enough
        commitIndex: node.commitIndex,
        lastApplied: node.lastApplied,
        kv: kvObject(node),
        electionProgress: node.electionProgress(),
      };
    }),
    inFlight: [...cluster.network.pending],
    lost,
    partition: sim.partitionGroups,
  };
}

/** Captions role changes, which the script doesn't cause directly. */
function noteStateChanges(sim: Sim, before: Frame | undefined, after: Frame): void {
  if (!before) return;
  after.nodes.forEach((node, i) => {
    const prev = before.nodes[i]!;
    // A restart is already captioned; only look at nodes running in both frames.
    if (!node.up || !prev.up || (node.role === prev.role && node.term === prev.term)) return;
    if (node.role === "leader" && prev.role !== "leader") {
      sim.event("state", `${node.id} becomes leader (term ${node.term})`);
    } else if (node.role === "candidate" && node.term > prev.term) {
      sim.event("state", `${node.id} starts an election (term ${node.term})`);
    } else if (prev.role === "leader" && node.role !== "leader") {
      sim.event("state", `${node.id} steps down (term ${node.term})`);
    }
  });
}
