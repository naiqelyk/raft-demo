import type { Role } from "../raft/node";
import type { Message } from "../raft/types";

// Colours are CSS variables (see styles.css) so they follow light/dark mode.

export const ROLE_COLOR: Record<Role, string> = {
  follower: "var(--follower)",
  candidate: "var(--candidate)",
  leader: "var(--leader)",
};

export const MESSAGE_COLOR: Record<Message["type"], string> = {
  RequestVote: "var(--msg-vote)",
  RequestVoteResponse: "var(--msg-vote-reply)",
  AppendEntries: "var(--msg-append)",
  AppendEntriesResponse: "var(--msg-append-reply)",
};

/** A distinct colour per term, for log entries. */
export function termColor(term: number): string {
  // Steps of 137° keep consecutive terms far apart on the colour wheel.
  return `hsl(${(200 + term * 137) % 360} 60% 60%)`;
}

/** Background tint for each side of a partition. */
export const GROUP_COLORS = ["var(--group-0)", "var(--group-1)", "var(--group-2)", "var(--group-3)", "var(--group-4)"];
