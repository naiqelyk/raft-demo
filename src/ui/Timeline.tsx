import type { SimEvent } from "../../test/util/sim";
import { SPEEDS } from "./usePlayback";

interface Props {
  tick: number;
  lastTick: number;
  playing: boolean;
  speed: number;
  events: SimEvent[];
  onToggle: () => void;
  onStep: (delta: number) => void;
  onSeek: (tick: number) => void;
  onSpeed: (speed: number) => void;
}

/** Playback buttons and a scrubber with a marker for each action and failed check. */
export function Timeline({ tick, lastTick, playing, speed, events, onToggle, onStep, onSeek, onSpeed }: Props) {
  const markers = events.filter((e) => e.kind === "action" || (e.kind === "check" && !e.passed));

  return (
    <div className="timeline">
      <div className="controls">
        <button onClick={() => onSeek(0)} title="Back to the start">
          ⏮
        </button>
        <button onClick={() => onStep(-1)} title="Back one tick">
          ◀
        </button>
        <button onClick={onToggle} className="play" title={playing ? "Pause" : "Play"}>
          {playing ? "⏸" : "▶"}
        </button>
        <button onClick={() => onStep(1)} title="Forward one tick">
          ▶|
        </button>
        <button onClick={() => onSeek(lastTick)} title="Jump to the end">
          ⏭
        </button>
        <label>
          speed{" "}
          <select value={speed} onChange={(e) => onSpeed(Number(e.target.value))}>
            {SPEEDS.map((s) => (
              <option key={s} value={s}>
                {s}×
              </option>
            ))}
          </select>
        </label>
        <span className="muted">
          tick {tick} / {lastTick}
        </span>
      </div>

      <div className="scrubber">
        <div className="markers">
          {markers.map((e, i) => (
            <button
              key={i}
              className={e.kind === "check" ? "marker failed" : "marker"}
              style={{ left: `${(e.tick / Math.max(1, lastTick)) * 100}%` }}
              title={`tick ${e.tick}: ${e.text}`}
              onClick={() => onSeek(e.tick)}
            />
          ))}
        </div>
        <input
          type="range"
          min={0}
          max={lastTick}
          value={tick}
          onChange={(e) => onSeek(Number(e.target.value))}
          aria-label="Timeline"
        />
      </div>
    </div>
  );
}
