import type {TrafficStage} from '../change-log-traffic.ts';
import type {SoakConfig} from './config.ts';

/**
 * The workload phases of plan section 5.
 *
 * A flat rate does not stress purge scheduling, the barrier, or the vfs
 * poller's pause/resume; sustained rate, bursts and idle gaps do. Every
 * duration is multiplied by `config.scale`, so the same shape runs as a
 * two-minute smoke test or as a thirty-minute soak.
 */

export type PhaseSpec = {
  readonly name: string;
  readonly stresses: string;
  readonly stages: readonly TrafficStage[];
};

function scaled(config: SoakConfig, seconds: number): number {
  return Math.max(2, Math.round(seconds * config.scale));
}

export function workloadPhases(config: SoakConfig): PhaseSpec[] {
  return [
    {
      name: 'sustained',
      stresses: 'steady append and purge cadence',
      stages: [
        {rate: 25, durationSeconds: scaled(config, 300), label: 'sustained'},
      ],
    },
    {
      name: 'burst',
      stresses: 'the barrier under backlog pressure; batch limits',
      stages: [
        {rate: 500, durationSeconds: scaled(config, 20), label: 'burst-1'},
        {rate: 0, durationSeconds: scaled(config, 20), label: 'burst-gap-1'},
        {rate: 500, durationSeconds: scaled(config, 20), label: 'burst-2'},
        {rate: 0, durationSeconds: scaled(config, 20), label: 'burst-gap-2'},
        {rate: 500, durationSeconds: scaled(config, 20), label: 'burst-3'},
      ],
    },
    {
      name: 'quiet',
      stresses:
        'the purger draining to its floor, and the vfs poller pausing when local == remote',
      stages: [
        {
          // Longer than retentionMs, so the log ages out of its warm-up
          // window and the purger has something to do.
          rate: 0,
          durationSeconds: Math.max(
            scaled(config, 90),
            Math.ceil(config.changeLog.retentionMs / 1000) + 15,
          ),
          label: 'quiet',
        },
      ],
    },
    {
      name: 'fat',
      stresses: 'the `oversized` compare outcome and multipart backup',
      stages: [
        {
          rate: 25,
          durationSeconds: scaled(config, 120),
          payloadBytes: 8000,
          label: 'fat',
        },
      ],
    },
  ];
}

/** Phase 6: phases 2-4 again, with chaos interleaved by the orchestrator. */
export function mixedPhase(config: SoakConfig): PhaseSpec {
  return {
    name: 'mixed',
    stresses: 'everything at once',
    stages: [
      {rate: 25, durationSeconds: scaled(config, 60), label: 'mixed-sustained'},
      {rate: 300, durationSeconds: scaled(config, 15), label: 'mixed-burst'},
      {rate: 0, durationSeconds: scaled(config, 30), label: 'mixed-quiet'},
    ],
  };
}
