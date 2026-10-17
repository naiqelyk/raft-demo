import type { Message, NodeId } from "../raft/types";
import type { Frame } from "../../test/util/record";
import { GROUP_COLORS, MESSAGE_COLOR, ROLE_COLOR } from "./colors";

const SIZE = 440;
const CENTER = SIZE / 2;
const RADIUS = 160;
const NODE_R = 30;
const RING_R = NODE_R + 6;
const RING_LENGTH = 2 * Math.PI * RING_R;
/** How many ticks a lost-message mark stays visible. */
const LOST_FADE_TICKS = 3;

type Point = { x: number; y: number };

/**
 * The cluster at playhead `time`: nodes on a circle, messages moving between
 * them, lost messages, and partition groups.
 */
export function ClusterView({ frames, time }: { frames: Frame[]; time: number }) {
  const tick = Math.min(Math.floor(time), frames.length - 1);
  const frame = frames[tick]!;
  const pos = nodePositions(frame.nodes.map((n) => n.id));
  const at = (id: NodeId) => pos.get(id)!;

  return (
    <svg className="cluster" viewBox={`0 0 ${SIZE} ${SIZE}`} role="img" aria-label="Cluster">
      {partitionGroups(frame)?.map((group, i) =>
        group.map((id) => (
          <circle
            key={id}
            cx={at(id).x}
            cy={at(id).y}
            r={NODE_R + 26}
            style={{ fill: GROUP_COLORS[i % GROUP_COLORS.length] }}
          />
        )),
      )}

      {frame.inFlight.map(({ msg, sentAt, deliverAt }, i) => {
        const progress = clamp((time - sentAt) / (deliverAt - sentAt));
        const p = alongPath(at(msg.from), at(msg.to), progress);
        const filled = isFilled(msg);
        return (
          <circle
            key={i}
            cx={p.x}
            cy={p.y}
            r={filled ? 6 : 5}
            className="message"
            style={{
              fill: filled ? MESSAGE_COLOR[msg.type] : "var(--bg)",
              stroke: MESSAGE_COLOR[msg.type],
            }}
          />
        );
      })}

      {recentLosses(frames, tick).map(({ msg, reason, age }, i) => {
        // Drops happen when sent; partitions and crashed nodes cut messages off on arrival.
        const p = alongPath(at(msg.from), at(msg.to), reason === "dropped" ? 0.15 : 0.85);
        return (
          <text key={i} x={p.x} y={p.y} className="lost" style={{ opacity: 1 - age / LOST_FADE_TICKS }}>
            ×
          </text>
        );
      })}

      {frame.nodes.map((node) => {
        const { x, y } = at(node.id);
        return (
          <g key={node.id} transform={`translate(${x} ${y})`} className={node.up ? "node" : "node down"}>
            {node.up && node.role !== "leader" && (
              <circle
                r={RING_R}
                className="ring"
                strokeDasharray={`${node.electionProgress * RING_LENGTH} ${RING_LENGTH}`}
                transform="rotate(-90)"
              />
            )}
            <circle r={NODE_R} style={{ fill: node.up ? ROLE_COLOR[node.role] : "var(--crashed)" }} />
            <text y={-4} className="node-id">
              {node.id}
            </text>
            <text y={13} className="node-term">
              {node.up ? `T${node.term}` : "crashed"}
            </text>
          </g>
        );
      })}

      <text x={CENTER} y={CENTER} className="center-tick">
        tick {tick}
      </text>
    </svg>
  );
}

function nodePositions(ids: NodeId[]): Map<NodeId, Point> {
  return new Map(
    ids.map((id, i) => {
      const angle = -Math.PI / 2 + (2 * Math.PI * i) / ids.length;
      return [id, { x: CENTER + RADIUS * Math.cos(angle), y: CENTER + RADIUS * Math.sin(angle) }];
    }),
  );
}

/**
 * A point `progress` of the way from one node's edge to the other's, shifted a
 * little to the side so messages in opposite directions don't overlap.
 */
function alongPath(from: Point, to: Point, progress: number): Point {
  const dx = to.x - from.x;
  const dy = to.y - from.y;
  const length = Math.hypot(dx, dy) || 1;
  const ux = dx / length;
  const uy = dy / length;
  const start = { x: from.x + ux * NODE_R, y: from.y + uy * NODE_R };
  const travel = length - 2 * NODE_R;
  return {
    x: start.x + ux * travel * progress - uy * 6,
    y: start.y + uy * travel * progress + ux * 6,
  };
}

/** Hollow dots are heartbeats and rejections; filled dots carry entries or a yes. */
function isFilled(msg: Message): boolean {
  switch (msg.type) {
    case "RequestVote":
      return true;
    case "RequestVoteResponse":
      return msg.voteGranted;
    case "AppendEntries":
      return msg.entries.length > 0;
    case "AppendEntriesResponse":
      return msg.success;
  }
}

/** The partition's groups, with each unlisted node as its own isolated group. */
function partitionGroups(frame: Frame): NodeId[][] | null {
  if (!frame.partition) return null;
  const listed = new Set(frame.partition.flat());
  const isolated = frame.nodes.filter((n) => !listed.has(n.id)).map((n) => [n.id]);
  return [...frame.partition.filter((g) => g.length > 0), ...isolated];
}

function recentLosses(frames: Frame[], tick: number) {
  const losses = [];
  for (let age = 0; age < LOST_FADE_TICKS && tick - age >= 0; age++) {
    for (const lost of frames[tick - age]!.lost) losses.push({ ...lost, age });
  }
  return losses;
}

function clamp(x: number): number {
  return Math.max(0, Math.min(1, x));
}
