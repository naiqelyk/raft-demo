import type { Rng } from "../sim/rng";
import type {
  AppendEntries,
  AppendEntriesResponse,
  Command,
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

/** Result of `clientRequest`: the new entry's index, or who we think the leader is. */
export type ClientResult =
  | { ok: true; index: number; messages: Message[] }
  | { ok: false; leaderHint: NodeId | null };

/** The state Raft keeps on disk; it survives a crash, everything else is lost. */
export interface PersistentState {
  currentTerm: number;
  votedFor: NodeId | null;
  log: LogEntry[];
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
  /** Highest log index known to be committed. */
  commitIndex = 0;
  /** Highest log index applied to `kv`. */
  lastApplied = 0;
  /** The replicated key-value state machine. */
  kv = new Map<string, string>();
  private votesReceived = new Set<NodeId>();
  private electionElapsed = 0;
  private electionTimeout: number;
  private heartbeatElapsed = 0;

  // Volatile leader state, reset on every election win.
  /** Index of the next entry to send to each peer. */
  private nextIndex = new Map<NodeId, number>();
  /** Highest index known to be replicated on each peer. */
  private matchIndex = new Map<NodeId, number>();

  constructor(
    readonly id: NodeId,
    readonly peers: NodeId[],
    private readonly rng: Rng,
    private readonly config: RaftConfig = DEFAULT_RAFT_CONFIG,
    persisted?: PersistentState,
  ) {
    if (persisted) {
      this.currentTerm = persisted.currentTerm;
      this.votedFor = persisted.votedFor;
      this.log = [...persisted.log];
    }
    this.electionTimeout = this.randomElectionTimeout();
  }

  /** A copy of the state that would be on disk, for restarting this node after a crash. */
  persistentState(): PersistentState {
    return { currentTerm: this.currentTerm, votedFor: this.votedFor, log: [...this.log] };
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

  /**
   * Appends a client command if we are the leader. The entry is committed
   * (and applied to `kv`) once a majority stores it.
   */
  clientRequest(command: Command): ClientResult {
    if (this.role !== "leader") return { ok: false, leaderHint: this.leaderId };
    this.log.push({ term: this.currentTerm, command });
    this.advanceCommitIndex(); // a single-node cluster commits immediately
    return { ok: true, index: this.lastLogIndex(), messages: this.broadcastAppendEntries() };
  }

  lastLogIndex(): number {
    return this.log.length;
  }

  lastLogTerm(): number {
    return this.termAt(this.lastLogIndex());
  }

  /** Term of the entry at `index`; 0 for index 0 or past the end of the log. */
  termAt(index: number): number {
    return this.log[index - 1]?.term ?? 0;
  }

  // --- Elections -------------------------------------------------------------

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
    // so that a new leader always holds every committed entry.
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
    for (const peer of this.peers) {
      this.nextIndex.set(peer, this.lastLogIndex() + 1);
      this.matchIndex.set(peer, 0);
    }
    // Append a no-op from our own term. Entries left over from earlier terms
    // only commit along with an entry from the current term (see
    // advanceCommitIndex), so this commits them now instead of waiting for the
    // next client write.
    this.log.push({ term: this.currentTerm, command: { op: "noop" } });
    this.advanceCommitIndex(); // a single-node cluster commits immediately
    // Announce leadership immediately so other candidates stand down.
    return this.broadcastAppendEntries();
  }

  private becomeFollower(term: number): void {
    this.role = "follower";
    this.currentTerm = term;
    this.votedFor = null;
    this.leaderId = null;
  }

  // --- Log replication -------------------------------------------------------

  /** Sends every peer the entries it is missing; with nothing missing, it's a heartbeat. */
  private broadcastAppendEntries(): Message[] {
    this.heartbeatElapsed = 0;
    return this.peers.map((peer) => this.appendEntriesFor(peer));
  }

  private appendEntriesFor(peer: NodeId): AppendEntries {
    const prevLogIndex = (this.nextIndex.get(peer) ?? this.lastLogIndex() + 1) - 1;
    return {
      type: "AppendEntries",
      from: this.id,
      to: peer,
      term: this.currentTerm,
      prevLogIndex,
      prevLogTerm: this.termAt(prevLogIndex),
      entries: this.log.slice(prevLogIndex),
      leaderCommit: this.commitIndex,
    };
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

    // This is the legitimate leader for our term: follow it (a candidate
    // concedes) and reset the election timer.
    this.role = "follower";
    this.leaderId = msg.from;
    this.resetElectionTimer();

    // Consistency check: we must already hold the entry the new ones follow.
    if (msg.prevLogIndex > this.lastLogIndex() || this.termAt(msg.prevLogIndex) !== msg.prevLogTerm) {
      return [reply(false)];
    }

    // Truncate only at the first conflicting entry. Entries we already match are
    // kept, so a stale or reordered AppendEntries can't undo a newer one.
    for (const [i, entry] of msg.entries.entries()) {
      const index = msg.prevLogIndex + 1 + i;
      if (index <= this.lastLogIndex() && this.termAt(index) === entry.term) continue;
      this.log.splice(index - 1);
      this.log.push(entry);
    }

    // Only entries up to the end of this message are known to match the leader.
    this.commitTo(Math.min(msg.leaderCommit, msg.prevLogIndex + msg.entries.length));
    return [reply(true)];
  }

  private handleAppendEntriesResponse(msg: AppendEntriesResponse): Message[] {
    // Stepping down on a higher term is handled in receive(). Ignore responses
    // from an earlier term, or that arrive after we've stepped down.
    if (this.role !== "leader" || msg.term !== this.currentTerm) return [];

    const match = this.matchIndex.get(msg.from) ?? 0;
    if (msg.success) {
      // max(): a reordered, older response must not move matchIndex backwards.
      const newMatch = Math.max(match, msg.matchIndex);
      this.matchIndex.set(msg.from, newMatch);
      this.nextIndex.set(msg.from, newMatch + 1);
      this.advanceCommitIndex();
      return [];
    }

    // The follower's log doesn't match at prevLogIndex: back off one entry and
    // retry, but never below what the follower is already known to match.
    const next = this.nextIndex.get(msg.from) ?? 1;
    this.nextIndex.set(msg.from, Math.max(match + 1, next - 1));
    return [this.appendEntriesFor(msg.from)];
  }

  // --- Commit and apply ------------------------------------------------------

  /** Leader: commit the highest entry from the current term stored on a majority. */
  private advanceCommitIndex(): void {
    for (let n = this.lastLogIndex(); n > this.commitIndex; n--) {
      // The current-term commit rule: only count replicas of entries from our
      // own term. An earlier-term entry can be on a majority and still be
      // overwritten later by a leader whose log ends in a newer term. Once a
      // current-term entry is on a majority, no node without it can win an
      // election, so it and everything before it are safe. Terms only grow
      // along the log, so nothing below n qualifies either.
      if (this.termAt(n) !== this.currentTerm) return;

      const replicas = 1 + this.peers.filter((peer) => (this.matchIndex.get(peer) ?? 0) >= n).length;
      if (this.hasMajority(replicas)) return this.commitTo(n);
    }
  }

  /** Raises commitIndex to `index` (never lowers it) and applies newly committed entries. */
  private commitTo(index: number): void {
    if (index <= this.commitIndex) return;
    this.commitIndex = index;
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied++;
      const entry = this.log[this.lastApplied - 1];
      if (!entry) throw new Error(`${this.id}: committed index ${this.lastApplied} is missing`);
      this.apply(entry.command);
    }
  }

  private apply(command: Command): void {
    if (command.op === "put") this.kv.set(command.key, command.value);
    else if (command.op === "delete") this.kv.delete(command.key);
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
