import { firstElection, leaderPartitioned, minorityNoLeader } from "./scenarios/election";
import { leaderCrash, majorityCrash, oldTermEntry, unsentEntriesOverwritten } from "./scenarios/failures";
import { happyPath, laggingFollower, minorityLeaderCantCommit } from "./scenarios/replication";
import type { Scenario } from "./sim";

export { chaosScenario } from "./scenarios/chaos";

/** The scripted scenarios, in the order the UI lists them. Chaos is separate (it takes a seed). */
export const SCENARIOS: Scenario[] = [
  firstElection,
  happyPath,
  leaderPartitioned,
  minorityNoLeader,
  minorityLeaderCantCommit,
  laggingFollower,
  leaderCrash,
  majorityCrash,
  unsentEntriesOverwritten,
  oldTermEntry,
];
