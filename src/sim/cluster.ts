import {
  DEFAULT_RAFT_CONFIG,
  RaftNode,
  type ClientResult,
  type PersistentState,
  type RaftConfig,
} from "../raft/node";
import type { Command, Message, NodeId } from "../raft/types";
import { Network, type NetworkOptions } from "./network";
import { Rng } from "./rng";

export interface ClusterOptions {
  size?: number;
  seed?: number;
  network?: Partial<NetworkOptions>;
  raft?: Partial<RaftConfig>;
}

/**
 * Owns the clock and wires nodes to the network. Each `step()` delivers the
 * messages that are due, then ticks every node, sending whatever they return.
 */
export class Cluster {
  now = 0;
  readonly network: Network;
  readonly nodes: Map<NodeId, RaftNode>;
  /** Node ids n1..nN. Hold on to these rather than node objects: a restart replaces the object. */
  readonly ids: NodeId[];
  /** Called at the end of every `step()`, e.g. to record or check the cluster. */
  onStep?: () => void;
  private readonly rng: Rng;
  private readonly config: RaftConfig;
  private readonly down = new Set<NodeId>();

  constructor({ size = 3, seed = 1, network = {}, raft = {} }: ClusterOptions = {}) {
    this.rng = new Rng(seed);
    this.config = { ...DEFAULT_RAFT_CONFIG, ...raft };
    this.ids = Array.from({ length: size }, (_, i) => `n${i + 1}`);

    this.network = new Network(this.rng, network);
    this.nodes = new Map(this.ids.map((id) => [id, this.newNode(id)]));
  }

  step(): void {
    this.now++;
    for (const msg of this.network.deliverDue(this.now)) {
      // Messages that reach a crashed node are lost.
      const node = this.nodes.get(msg.to);
      if (node && this.isUp(node.id)) this.send(node.receive(msg));
    }
    for (const node of this.upNodes()) this.send(node.tick());
    this.onStep?.();
  }

  /**
   * Stops a node: it no longer ticks or receives. Messages it already sent are
   * still delivered.
   */
  crash(id: NodeId): void {
    this.node(id); // throws on an unknown id
    this.down.add(id);
  }

  /** Brings a node back with only its persistent state; all volatile state starts fresh. */
  restart(id: NodeId): void {
    this.nodes.set(id, this.newNode(id, this.node(id).persistentState()));
    this.down.delete(id);
  }

  isUp(id: NodeId): boolean {
    return !this.down.has(id);
  }

  run(ticks: number): void {
    for (let i = 0; i < ticks; i++) this.step();
  }

  /** Steps until `condition` holds, failing loudly if it takes longer than `maxTicks`. */
  runUntil(condition: () => boolean, maxTicks = 1000): void {
    for (let i = 0; i < maxTicks; i++) {
      if (condition()) return;
      this.step();
    }
    if (!condition()) throw new Error(`Condition not met within ${maxTicks} ticks`);
  }

  node(id: NodeId): RaftNode {
    const node = this.nodes.get(id);
    if (!node) throw new Error(`Unknown node ${id}`);
    return node;
  }

  /** Nodes that are not crashed. */
  upNodes(): RaftNode[] {
    return [...this.nodes.values()].filter((node) => this.isUp(node.id));
  }

  /** All running nodes that believe they are leader (may be >1 during a partition). */
  leaders(): RaftNode[] {
    return this.upNodes().filter((node) => node.role === "leader");
  }

  /** The leader with the highest term, if any. */
  leader(): RaftNode | undefined {
    return this.leaders().sort((a, b) => b.currentTerm - a.currentTerm)[0];
  }

  /** Submits a command to the current leader, if there is one. */
  submit(command: Command): ClientResult {
    const leader = this.leader();
    if (!leader) return { ok: false, leaderHint: null };
    const result = leader.clientRequest(command);
    if (result.ok) this.send(result.messages);
    return result;
  }

  private newNode(id: NodeId, persisted?: PersistentState): RaftNode {
    const peers = this.ids.filter((peer) => peer !== id);
    return new RaftNode(id, peers, this.rng, this.config, persisted);
  }

  private send(messages: Message[]): void {
    for (const msg of messages) this.network.send(msg, this.now);
  }
}
