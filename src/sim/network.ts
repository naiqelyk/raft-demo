import type { Message, NodeId } from "../raft/types";
import type { Rng } from "./rng";

export interface NetworkOptions {
  /** Minimum ticks between send and delivery. */
  minDelay: number;
  /** Maximum ticks between send and delivery. Varying delays cause reordering. */
  maxDelay: number;
  /** Probability that any given message is lost. */
  dropRate: number;
}

export const DEFAULT_NETWORK_OPTIONS: NetworkOptions = {
  minDelay: 1,
  maxDelay: 3,
  dropRate: 0,
};

export type LossReason = "dropped" | "partitioned";

export interface InFlight {
  msg: Message;
  sentAt: number;
  deliverAt: number;
}

/**
 * An unreliable in-memory network. Messages are delayed by a random number of
 * ticks, may be dropped, and are only delivered between nodes on the same side
 * of a partition. The network has no clock of its own; callers pass `now`.
 */
export class Network {
  options: NetworkOptions;
  /** Called for every message that is lost, e.g. so the UI can show it. */
  onLost?: (msg: Message, reason: LossReason) => void;
  private inFlight: InFlight[] = [];
  /** Partition group of each node, or null when the network is fully connected. */
  private groups: Map<NodeId, number> | null = null;

  constructor(
    private readonly rng: Rng,
    options: Partial<NetworkOptions> = {},
  ) {
    this.options = { ...DEFAULT_NETWORK_OPTIONS, ...options };
  }

  send(msg: Message, now: number): void {
    if (this.rng.chance(this.options.dropRate)) {
      this.onLost?.(msg, "dropped");
      return;
    }
    const delay = this.rng.int(this.options.minDelay, this.options.maxDelay);
    this.inFlight.push({ msg, sentAt: now, deliverAt: now + delay });
  }

  /**
   * Removes and returns every message due at or before `now`. Reachability is
   * checked at delivery time, so messages in flight when a partition starts
   * are lost.
   */
  deliverDue(now: number): Message[] {
    const due: InFlight[] = [];
    const remaining: InFlight[] = [];
    for (const item of this.inFlight) {
      (item.deliverAt <= now ? due : remaining).push(item);
    }
    this.inFlight = remaining;
    const delivered = due.filter((item) => {
      const reachable = this.canReach(item.msg.from, item.msg.to);
      if (!reachable) this.onLost?.(item.msg, "partitioned");
      return reachable;
    });
    return delivered.sort((a, b) => a.deliverAt - b.deliverAt).map((item) => item.msg);
  }

  /** Splits the cluster into groups. Nodes not listed are isolated. */
  partition(groups: NodeId[][]): void {
    this.groups = new Map();
    groups.forEach((group, i) => {
      for (const id of group) this.groups!.set(id, i);
    });
  }

  heal(): void {
    this.groups = null;
  }

  canReach(from: NodeId, to: NodeId): boolean {
    if (this.groups === null) return true;
    const group = this.groups.get(from);
    return group !== undefined && group === this.groups.get(to);
  }

  /** Messages currently in flight, for inspection by tests and the UI. */
  get pending(): readonly InFlight[] {
    return this.inFlight;
  }
}
