import { useEffect, useRef } from "react";
import type { SimEvent } from "../../test/util/sim";

/**
 * Every captioned event. Ones that haven't happened yet are dimmed, and the
 * list follows the playhead. Clicking an event jumps to it.
 */
export function EventLog({ events, tick, onSeek }: { events: SimEvent[]; tick: number; onSeek: (tick: number) => void }) {
  const listRef = useRef<HTMLOListElement>(null);
  let current = -1; // the last event at or before the playhead
  while (current + 1 < events.length && events[current + 1]!.tick <= tick) current++;

  useEffect(() => {
    const list = listRef.current;
    const item = list?.children[Math.max(0, current)] as HTMLElement | undefined;
    // The list is position: relative, so offsetTop is measured from its top.
    if (list && item) list.scrollTop = item.offsetTop - list.clientHeight / 2;
  }, [current]);

  return (
    <ol className="event-log" ref={listRef}>
      {events.map((e, i) => (
        <li
          key={i}
          className={[
            e.kind,
            e.kind === "check" && !e.passed ? "failed-check" : "",
            i > current ? "future" : "",
            i === current ? "current" : "",
          ].join(" ")}
          onClick={() => onSeek(e.tick)}
        >
          <span className="event-tick">t={e.tick}</span>
          <span className="event-kind">{e.kind}</span>
          <span>
            {e.kind === "check" && (e.passed ? "✓ " : "✗ ")}
            {e.text}
          </span>
        </li>
      ))}
    </ol>
  );
}
