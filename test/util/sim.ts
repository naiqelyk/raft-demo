import type { ClientResult, RaftNode } from "../../src/raft/node";
import type { Command, NodeId } from "../../src/raft/types";
import { Cluster } from "../../src/sim/cluster";
import { InvariantChecker } from "../../src/sim/invariants";
import type { NetworkOptions } from "../../src/sim/network";

/**
 * A scripted run of the cluster, shared by the integration tests and the UI.
 * Scripts act through `Sim` rather than the cluster directly, so every action
 * and check is also recorded as a caption on the timeline.
 */
export interface Scenario {
  id: string;
  title: string;
  description: string;
  size: number;
  seed: number;
  network?: Partial<NetworkOptions>;
  /** Run the invariant checker after every tick (the tests only do this for chaos). */
  checkInvariants?: boolean;
  run(sim: Sim): void;
}

export type SimEvent = {
  tick: number;
  /** action: something the script did. note: narration. check: an assertion.
   *  state: a change the recorder noticed, e.g. a new leader. */
  kind: "action" | "note" | "check" | "state";
  text: string;
  /** For checks: whether it passed. */
  passed?: boolean;
};

/** Receives every check; the tests turn these into `expect` assertions. */
export type CheckHandler = (label: string, actual: unknown, expected: unknown) => void;

export interface SimOptions {
  onCheck?: CheckHandler;
  checkInvariants?: boolean;
  /** Called after every tick, after the invariant check. */
  afterStep?: () => void;
}

export class Sim {
  readonly cluster: Cluster;
  readonly events: SimEvent[] = [];
  /** The current partition as passed to `partition()`, or null when healed. */
  partitionGroups: NodeId[][] | null = null;

  constructor(
    scenario: Scenario,
    private readonly options: SimOptions = {},
  ) {
    this.cluster = new Cluster({ size: scenario.size, seed: scenario.seed, network: scenario.network });
    let checker = options.checkInvariants ? new InvariantChecker() : undefined;
    this.cluster.onStep = () => {
      try {
        checker?.check(this.cluster);
      } catch (error) {
        checker = undefined; // report only the first violation
        this.check("invariants hold", (error as Error).message, "no violation");
      }
      options.afterStep?.();
    };
  }

  get now(): number {
    return this.cluster.now;
  }

  node(id: NodeId): RaftNode {
    return this.cluster.node(id);
  }

  /** The current leader's id. Throws if there is none, like a failed test. */
  leaderId(): NodeId {
    const leader = this.cluster.leader();
    if (!leader) throw new Error(`no leader at tick ${this.now}`);
    return leader.id;
  }

  // --- Actions (captioned) ---------------------------------------------------

  partition(groups: NodeId[][]): void {
    this.cluster.network.partition(groups);
    this.partitionGroups = groups;
    this.event("action", `partition ${groups.map((g) => `{${g.join(", ")}}`).join(" | ")}`);
  }

  heal(): void {
    this.cluster.network.heal();
    this.partitionGroups = null;
    this.event("action", "heal the network");
  }

  crash(id: NodeId): void {
    this.cluster.crash(id);
    this.event("action", `crash ${id}`);
  }

  restart(id: NodeId): void {
    this.cluster.restart(id);
    this.event("action", `restart ${id}`);
  }

  submit(command: Command): ClientResult {
    const leader = this.cluster.leader();
    const result = this.cluster.submit(command);
    const text = describeCommand(command);
    this.event(
      "action",
      result.ok ? `client: ${text} → ${leader?.id} (index ${result.index})` : `client: ${text} rejected, no leader`,
    );
    return result;
  }

  /** Submits a command and waits until every running node has applied it. */
  commit(command: Command): void {
    const result = this.submit(command);
    if (!result.ok) throw new Error(`no leader to submit to at tick ${this.now}`);
    this.runUntil("committed and applied everywhere", () =>
      this.cluster.upNodes().every((n) => n.lastApplied >= result.index),
    );
  }

  // --- Time -------------------------------------------------------------------

  run(ticks: number): void {
    this.cluster.run(ticks);
  }

  /** Steps until `condition` holds, then captions `label`. Throws after `maxTicks`. */
  runUntil(label: string, condition: () => boolean, maxTicks = 1000): void {
    this.cluster.runUntil(condition, maxTicks);
    this.event("note", label);
  }

  // --- Narration and checks ----------------------------------------------------

  note(text: string): void {
    this.event("note", text);
  }

  /** Checks that `actual` deep-equals `expected`. */
  check(label: string, actual: unknown, expected: unknown): void {
    this.event("check", label, deepEqual(actual, expected));
    this.options.onCheck?.(label, actual, expected);
  }

  /** Checks that `condition` is true. */
  assert(label: string, condition: boolean): void {
    this.check(label, condition, true);
  }

  /** Adds an event at the current tick. Scripts use the methods above instead. */
  event(kind: SimEvent["kind"], text: string, passed?: boolean): void {
    this.events.push(passed === undefined ? { tick: this.now, kind, text } : { tick: this.now, kind, text, passed });
  }
}

/** A node's key-value store as a plain object, for checks and display. */
export function kvObject(node: RaftNode): Record<string, string> {
  return Object.fromEntries(node.kv);
}

export function describeCommand(command: Command): string {
  switch (command.op) {
    case "put":
      return `put ${command.key}=${command.value}`;
    case "delete":
      return `delete ${command.key}`;
    case "noop":
      return "no-op";
  }
}

function deepEqual(a: unknown, b: unknown): boolean {
  if (a === b) return true;
  if (typeof a !== "object" || typeof b !== "object" || a === null || b === null) return false;
  if (Array.isArray(a) !== Array.isArray(b)) return false;
  const keysA = Object.keys(a);
  const keysB = Object.keys(b);
  if (keysA.length !== keysB.length) return false;
  return keysA.every((key) =>
    deepEqual((a as Record<string, unknown>)[key], (b as Record<string, unknown>)[key]),
  );
}
