import { SCENARIOS } from "../../test/util";

export const CHAOS_ID = "chaos";

interface Props {
  selected: string;
  seed: number;
  onSelect: (id: string) => void;
  onSeed: (seed: number) => void;
}

/** The scripted scenarios, plus chaos with a seed picker. */
export function ScenarioPicker({ selected, seed, onSelect, onSeed }: Props) {
  return (
    <nav className="picker">
      <h2>Scenarios</h2>
      {SCENARIOS.map((s) => (
        <button key={s.id} className={s.id === selected ? "active" : ""} onClick={() => onSelect(s.id)}>
          {s.title}
        </button>
      ))}
      <div className="chaos-pick">
        <button className={selected === CHAOS_ID ? "active" : ""} onClick={() => onSelect(CHAOS_ID)}>
          Chaos
        </button>
        <label>
          seed{" "}
          <input
            type="number"
            min={1}
            value={seed}
            onChange={(e) => {
              onSeed(Math.max(1, Math.floor(Number(e.target.value)) || 1));
              onSelect(CHAOS_ID);
            }}
          />
        </label>
      </div>
    </nav>
  );
}
