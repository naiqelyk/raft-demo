import type { Command } from "../raft/types";
import type { Frame, NodeFrame } from "../../test/util/record";
import { describeCommand } from "../../test/util/sim";
import { ROLE_COLOR, termColor } from "./colors";

/** Only the end of long logs is shown. */
const MAX_ENTRIES = 16;

/** Each node's role, log (with commit marks) and key-value store. */
export function NodePanel({ frame }: { frame: Frame }) {
  return (
    <div className="node-panel">
      {frame.nodes.map((node) => (
        <NodeRow key={node.id} node={node} />
      ))}
    </div>
  );
}

function NodeRow({ node }: { node: NodeFrame }) {
  const hidden = Math.max(0, node.log.length - MAX_ENTRIES);
  const store = Object.entries(node.kv);

  return (
    <div className={node.up ? "node-row" : "node-row down"}>
      <div className="node-row-head">
        <span className="role-dot" style={{ background: node.up ? ROLE_COLOR[node.role] : "var(--crashed)" }} />
        <strong>{node.id}</strong>
        <span>{node.up ? node.role : "crashed"}</span>
        <span className="muted">term {node.term}</span>
        <span className="muted">commit {node.commitIndex}</span>
      </div>

      <div className="field">
        <span className="field-label">log</span>
        <div className="log">
        {hidden > 0 && <span className="muted">…{hidden} earlier</span>}
        {node.log.slice(hidden).map((entry, i) => {
          const index = hidden + i + 1;
          const committed = index <= node.commitIndex;
          return (
            <span
              key={index}
              className={committed ? "cell entry committed" : "cell entry"}
              style={{ borderColor: termColor(entry.term), background: committed ? termColor(entry.term) : undefined }}
              title={`#${index}, term ${entry.term}: ${describeCommand(entry.command)}${committed ? "" : " (not committed)"}`}
            >
              {shortCommand(entry.command)}
            </span>
          );
        })}
        {node.log.length === 0 && <span className="muted">empty</span>}
        </div>
      </div>

      <div className="field">
        <span className="field-label">store</span>
        <div className="kv">
          {store.length === 0 ? (
            <span className="muted">empty</span>
          ) : (
            store.map(([key, value]) => (
              <span key={key} className="cell store-item" title={`${key}=${value}`}>
                {key}={value}
              </span>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

function shortCommand(command: Command): string {
  switch (command.op) {
    case "put":
      return `${command.key}=${command.value}`;
    case "delete":
      return `−${command.key}`;
    case "noop":
      return "·";
  }
}
