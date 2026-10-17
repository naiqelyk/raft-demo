import { DEFAULT_RAFT_CONFIG, RaftNode, type ClientResult, type RaftConfig } from "../raft/node";
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

  constructor({ size = 3, seed = 1, network = {}, raft = {} }: ClusterOptions = {}) {
    const rng = new Rng(seed);
    const config = { ...DEFAULT_RAFT_CONFIG, ...raft };
    const ids = Array.from({ length: size }, (_, i) => `n${i + 1}`);

    this.network = new Network(rng, network);
    this.nodes = new Map(
      ids.map((id) => [id, new RaftNode(id, ids.filter((peer) => peer !== id), rng, config)]),
    );
  }

  step(): void {
    this.now++;
    for (const msg of this.network.deliverDue(this.now)) {
      const node = this.nodes.get(msg.to);
      if (node) this.send(node.receive(msg));
    }
    for (const node of this.nodes.values()) this.send(node.tick());
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

  /** All nodes that currently believe they are leader (may be >1 during a partition). */
  leaders(): RaftNode[] {
    return [...this.nodes.values()].filter((node) => node.role === "leader");
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

  private send(messages: Message[]): void {
    for (const msg of messages) this.network.send(msg, this.now);
  }
}
