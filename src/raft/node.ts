import type { Rng } from "../sim/rng";
import type {
  AppendEntries,
  AppendEntriesResponse,
  LogEntry,
  Message,
  NodeId,
  RequestVote,
  RequestVoteResponse,
} from "./types";

export type Role = "follower" | "candidate" | "leader";

export interface RaftConfig {
  /** Election timeouts are drawn uniformly from [min, max] ticks. */
  electionTimeoutMin: number;
  electionTimeoutMax: number;
  /** Ticks between leader heartbeats. Must be well below electionTimeoutMin. */
  heartbeatInterval: number;
}

export const DEFAULT_RAFT_CONFIG: RaftConfig = {
  electionTimeoutMin: 15,
  electionTimeoutMax: 30,
  heartbeatInterval: 5,
};

/**
 * A single Raft node. It never performs I/O or reads a clock: the caller
 * drives it with `tick()` and `receive()`, and sends whatever messages those
 * return.
 */
export class RaftNode {
  // Persistent state (survives crashes).
  currentTerm = 0;
  votedFor: NodeId | null = null;
  /** Log entries; Raft indices are 1-based, so entry i lives at log[i - 1]. */
  log: LogEntry[] = [];

  // Volatile state.
  role: Role = "follower";
  leaderId: NodeId | null = null;
  private votesReceived = new Set<NodeId>();
  private electionElapsed = 0;
  private electionTimeout: number;
  private heartbeatElapsed = 0;

  constructor(
    readonly id: NodeId,
    readonly peers: NodeId[],
    private readonly rng: Rng,
    private readonly config: RaftConfig = DEFAULT_RAFT_CONFIG,
  ) {
    this.electionTimeout = this.randomElectionTimeout();
  }

  /** Advances this node's timers by one tick. */
  tick(): Message[] {
    if (this.role === "leader") {
      this.heartbeatElapsed++;
      return this.heartbeatElapsed >= this.config.heartbeatInterval
        ? this.broadcastAppendEntries()
        : [];
    }
    this.electionElapsed++;
    return this.electionElapsed >= this.electionTimeout ? this.startElection() : [];
  }

  /** Handles a message delivered by the network. */
  receive(msg: Message): Message[] {
    // Any message from a newer term means we are out of date.
    if (msg.term > this.currentTerm) this.becomeFollower(msg.term);

    switch (msg.type) {
      case "RequestVote":
        return this.handleRequestVote(msg);
      case "RequestVoteResponse":
        return this.handleRequestVoteResponse(msg);
      case "AppendEntries":
        return this.handleAppendEntries(msg);
      case "AppendEntriesResponse":
        return this.handleAppendEntriesResponse(msg);
    }
  }

  lastLogIndex(): number {
    return this.log.length;
  }

  lastLogTerm(): number {
    return this.log.at(-1)?.term ?? 0;
  }

  // --- Elections -----------------------------------------------------------

  private startElection(): Message[] {
    this.role = "candidate";
    this.currentTerm++;
    this.votedFor = this.id;
    this.leaderId = null;
    this.votesReceived = new Set([this.id]);
    this.resetElectionTimer();

    if (this.hasMajority(this.votesReceived.size)) return this.becomeLeader();

    return this.peers.map(
      (peer): RequestVote => ({
        type: "RequestVote",
        from: this.id,
        to: peer,
        term: this.currentTerm,
        lastLogIndex: this.lastLogIndex(),
        lastLogTerm: this.lastLogTerm(),
      }),
    );
  }

  private handleRequestVote(msg: RequestVote): Message[] {
    // Only vote for candidates whose log is at least as up-to-date as ours,
    // so that a new leader always holds every committed entry (§5.4.1).
    const logIsUpToDate =
      msg.lastLogTerm > this.lastLogTerm() ||
      (msg.lastLogTerm === this.lastLogTerm() && msg.lastLogIndex >= this.lastLogIndex());

    const grant =
      msg.term === this.currentTerm &&
      (this.votedFor === null || this.votedFor === msg.from) &&
      logIsUpToDate;

    if (grant) {
      this.votedFor = msg.from;
      this.resetElectionTimer();
    }

    const reply: RequestVoteResponse = {
      type: "RequestVoteResponse",
      from: this.id,
      to: msg.from,
      term: this.currentTerm,
      voteGranted: grant,
    };
    return [reply];
  }

  private handleRequestVoteResponse(msg: RequestVoteResponse): Message[] {
    // Ignore late votes from earlier elections, or after we've already won.
    if (this.role !== "candidate" || msg.term !== this.currentTerm || !msg.voteGranted) {
      return [];
    }
    this.votesReceived.add(msg.from);
    return this.hasMajority(this.votesReceived.size) ? this.becomeLeader() : [];
  }

  private becomeLeader(): Message[] {
    this.role = "leader";
    this.leaderId = this.id;
    // Announce leadership immediately so other candidates stand down.
    return this.broadcastAppendEntries();
  }

  private becomeFollower(term: number): void {
    this.role = "follower";
    this.currentTerm = term;
    this.votedFor = null;
    this.leaderId = null;
  }

  // --- Heartbeats / replication ---------------------------------------------
  // For now the leader only sends empty heartbeats. Log replication comes in
  // the next step and will extend these three methods.

  private broadcastAppendEntries(): Message[] {
    this.heartbeatElapsed = 0;
    return this.peers.map(
      (peer): AppendEntries => ({
        type: "AppendEntries",
        from: this.id,
        to: peer,
        term: this.currentTerm,
        prevLogIndex: this.lastLogIndex(),
        prevLogTerm: this.lastLogTerm(),
        entries: [],
        leaderCommit: 0,
      }),
    );
  }

  private handleAppendEntries(msg: AppendEntries): Message[] {
    const reply = (success: boolean): AppendEntriesResponse => ({
      type: "AppendEntriesResponse",
      from: this.id,
      to: msg.from,
      term: this.currentTerm,
      success,
      matchIndex: success ? msg.prevLogIndex + msg.entries.length : 0,
    });

    // Reject leaders from stale terms; they'll step down when they see our term.
    if (msg.term < this.currentTerm) return [reply(false)];

    // A valid leader exists for our term: a candidate concedes, and everyone
    // resets their election timer.
    this.role = "follower";
    this.leaderId = msg.from;
    this.resetElectionTimer();
    return [reply(true)];
  }

  private handleAppendEntriesResponse(_msg: AppendEntriesResponse): Message[] {
    // Stepping down on a higher term is handled in receive(); nothing else to do yet.
    return [];
  }

  // --- Helpers ---------------------------------------------------------------

  private hasMajority(count: number): boolean {
    const clusterSize = this.peers.length + 1;
    return count > clusterSize / 2;
  }

  private resetElectionTimer(): void {
    this.electionElapsed = 0;
    this.electionTimeout = this.randomElectionTimeout();
  }

  private randomElectionTimeout(): number {
    return this.rng.int(this.config.electionTimeoutMin, this.config.electionTimeoutMax);
  }
}
