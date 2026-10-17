import { useMemo, useState } from "react";
import { chaosScenario, SCENARIOS } from "../../test/util";
import { record, type Recording } from "../../test/util/record";
import { ClusterView } from "./ClusterView";
import { MESSAGE_COLOR, ROLE_COLOR } from "./colors";
import { EventLog } from "./EventLog";
import { NodePanel } from "./NodePanel";
import { CHAOS_ID, ScenarioPicker } from "./ScenarioPicker";
import { Timeline } from "./Timeline";
import { usePlayback } from "./usePlayback";

export function App() {
  const [selected, setSelected] = useState(SCENARIOS[0]!.id);
  const [seed, setSeed] = useState(1);

  // Recording runs the whole scenario up front, so playback can jump anywhere.
  const recording = useMemo(
    () => record(selected === CHAOS_ID ? chaosScenario(seed) : SCENARIOS.find((s) => s.id === selected)!),
    [selected, seed],
  );

  return (
    <div className="app">
      <ScenarioPicker selected={selected} seed={seed} onSelect={setSelected} onSeed={setSeed} />
      {/* A new key resets playback when the recording changes. */}
      <Player key={recording.scenario.id} recording={recording} />
    </div>
  );
}

function Player({ recording }: { recording: Recording }) {
  const { scenario, frames, events } = recording;
  const lastTick = frames.length - 1;
  const playback = usePlayback(lastTick);
  const frame = frames[playback.tick]!;
  const failed = events.filter((e) => e.kind === "check" && !e.passed && e.tick <= playback.tick);

  return (
    <main className="player">
      <header>
        <h1>{scenario.title}</h1>
        <p className="muted">{scenario.description}</p>
        <span className={failed.length > 0 ? "badge failed" : "badge"}>
          {failed.length > 0 ? `✗ ${failed[0]!.text}` : "✓ all checks and invariants hold so far"}
        </span>
      </header>

      <div className="stage">
        <div>
          <ClusterView frames={frames} time={playback.time} />
          <Legend />
        </div>
        <NodePanel frame={frame} />
      </div>

      <Timeline
        tick={playback.tick}
        lastTick={lastTick}
        playing={playback.playing}
        speed={playback.speed}
        events={events}
        onToggle={playback.toggle}
        onStep={playback.step}
        onSeek={playback.seek}
        onSpeed={playback.setSpeed}
      />
      <EventLog events={events} tick={playback.tick} onSeek={playback.seek} />
    </main>
  );
}

function Legend() {
  const swatch = (color: string) => <span className="swatch" style={{ background: color }} />;
  return (
    <div className="legend">
      <span>{swatch(ROLE_COLOR.follower)}follower</span>
      <span>{swatch(ROLE_COLOR.candidate)}candidate</span>
      <span>{swatch(ROLE_COLOR.leader)}leader</span>
      <span>{swatch("var(--crashed)")}crashed</span>
      <span>{swatch(MESSAGE_COLOR.RequestVote)}vote request / reply</span>
      <span>{swatch(MESSAGE_COLOR.AppendEntries)}append / reply</span>
      <span className="muted">hollow = heartbeat or rejection · × = lost · ring = election timer</span>
    </div>
  );
}
