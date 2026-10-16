// Core Raft data types, following Figure 2 of the Raft paper.

export type NodeId = string;

/** A command for the replicated key-value state machine. */
export type Command =
  | { op: "put"; key: string; value: string }
  | { op: "delete"; key: string };

export interface LogEntry {
  term: number;
  command: Command;
}

interface BaseMessage {
  from: NodeId;
  to: NodeId;
  term: number;
}

/** Sent by candidates to gather votes. */
export interface RequestVote extends BaseMessage {
  type: "RequestVote";
  lastLogIndex: number;
  lastLogTerm: number;
}

export interface RequestVoteResponse extends BaseMessage {
  type: "RequestVoteResponse";
  voteGranted: boolean;
}

/** Sent by the leader to replicate entries; empty `entries` is a heartbeat. */
export interface AppendEntries extends BaseMessage {
  type: "AppendEntries";
  prevLogIndex: number;
  prevLogTerm: number;
  entries: LogEntry[];
  leaderCommit: number;
}

export interface AppendEntriesResponse extends BaseMessage {
  type: "AppendEntriesResponse";
  success: boolean;
  /**
   * On success, the index of the last entry the follower now matches. Not in
   * Figure 2, but it lets the leader handle reordered or duplicated responses
   * without guessing which request this one answers.
   */
  matchIndex: number;
}

export type Message =
  | RequestVote
  | RequestVoteResponse
  | AppendEntries
  | AppendEntriesResponse;
