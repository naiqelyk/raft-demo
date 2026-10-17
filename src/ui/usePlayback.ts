import { useEffect, useState } from "react";

export const SPEEDS = [0.2, 0.5, 1, 2, 5];
/** Ticks per second at 1× speed. */
const BASE_RATE = 1;

/**
 * A playhead over ticks 0..lastTick. `time` is fractional while playing, so
 * messages can move smoothly between ticks; `tick` is the frame to show.
 */
export function usePlayback(lastTick: number) {
  const [time, setTime] = useState(0);
  const [playing, setPlaying] = useState(true);
  const [speed, setSpeed] = useState(1);

  useEffect(() => {
    if (!playing) return;
    let frame = 0;
    // Measure between requestAnimationFrame timestamps only: they can be
    // slightly earlier than performance.now(), which would make time go negative.
    let last: number | undefined;
    const loop = (now: number) => {
      const elapsed = last === undefined ? 0 : (now - last) / 1000;
      last = now;
      setTime((t) => Math.min(lastTick, t + elapsed * BASE_RATE * speed));
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(frame);
  }, [playing, speed, lastTick]);

  useEffect(() => {
    if (time >= lastTick) setPlaying(false);
  }, [time, lastTick]);

  const seek = (t: number) => setTime(Math.max(0, Math.min(lastTick, t)));

  return {
    time,
    tick: Math.floor(time),
    playing,
    speed,
    setSpeed,
    seek,
    /** Pauses and moves one whole tick forward or back. */
    step(delta: number) {
      setPlaying(false);
      seek(Math.floor(time) + delta);
    },
    toggle() {
      if (!playing && time >= lastTick) setTime(0); // replay from the start
      setPlaying(!playing);
    },
  };
}
