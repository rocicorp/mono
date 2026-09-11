import fc from 'fast-check';
import type {BackfillFault} from './sim-pg.ts';
import {backfillWorkloadTxArb, type WorkloadOp} from './workload.ts';

/** Steps come in groups, which a run's swarm mask switches off together. */
export const STEP_GROUPS = [
  'workload',
  'delivery',
  'upstream-faults',
  'rm',
  'backup',
  'view-syncer',
  'time',
  'yields',
] as const;

export type StepGroup = (typeof STEP_GROUPS)[number];

/** Where `rmCrashAt` crashes the replication-manager; see `probes.ts`. */
export const CRASH_POINTS = [
  'before-log-commit',
  'after-log-commit',
  'after-forward',
  'mid-flush',
] as const;

export type CrashPoint = (typeof CRASH_POINTS)[number];

export type StepAction =
  | {
      readonly kind: 'commit';
      readonly ops: readonly WorkloadOp[];
      readonly gap: number;
    }
  | {readonly kind: 'idle'}
  | {readonly kind: 'deliver'; readonly n: number}
  | {readonly kind: 'sourceDisconnect'; readonly partial: number | undefined}
  | {readonly kind: 'backfillFault'; readonly fault: BackfillFault}
  // A new replication-manager task, restored from the latest backup, takes the
  // slot from the running one. Fenced, the old task dies as it loses the slot;
  // overlapping, it keeps running until it reads that it lost the slot, which
  // shuts it down, or until its orchestrator stops it.
  | {readonly kind: 'slotTakeover'; readonly overlap: boolean}
  | {readonly kind: 'rmCrash'; readonly shm: boolean}
  | {
      readonly kind: 'rmCrashAt';
      readonly point: CrashPoint;
      readonly after: number;
    }
  | {readonly kind: 'rmRestart'}
  | {readonly kind: 'rmReplace'}
  | {readonly kind: 'deleteChangeLog'}
  | {readonly kind: 'backupTake'}
  | {readonly kind: 'backupStall'}
  | {readonly kind: 'backupResume'}
  | {readonly kind: 'vsPull'; readonly vs: number; readonly n: number}
  | {readonly kind: 'vsPause'; readonly vs: number}
  | {readonly kind: 'vsResume'; readonly vs: number}
  | {
      readonly kind: 'vsDisconnect';
      readonly vs: number;
      readonly error: boolean;
    }
  | {readonly kind: 'vsRestart'; readonly vs: number}
  | {readonly kind: 'vsWipe'; readonly vs: number}
  | {readonly kind: 'vsHoldReservation'; readonly vs: number}
  | {readonly kind: 'advance'; readonly ms: number}
  | {readonly kind: 'pumpPurge'; readonly batches: number};

/**
 * One step. `id` seeds the step's own randomness and is generated with it, so
 * it survives shrinking. `dt` is how far virtual time moves after the action;
 * every step moves it at least 1 ms, or a waiting backfill never yields.
 */
export type Step = StepAction & {readonly id: number; readonly dt: number};

const GROUP_OF: Record<StepAction['kind'], StepGroup> = {
  commit: 'workload',
  idle: 'workload',
  deliver: 'delivery',
  sourceDisconnect: 'upstream-faults',
  backfillFault: 'upstream-faults',
  slotTakeover: 'upstream-faults',
  rmCrash: 'rm',
  rmCrashAt: 'rm',
  rmRestart: 'rm',
  rmReplace: 'rm',
  deleteChangeLog: 'rm',
  backupTake: 'backup',
  backupStall: 'backup',
  backupResume: 'backup',
  vsPull: 'view-syncer',
  vsPause: 'view-syncer',
  vsResume: 'view-syncer',
  vsDisconnect: 'view-syncer',
  vsRestart: 'view-syncer',
  vsWipe: 'view-syncer',
  vsHoldReservation: 'view-syncer',
  advance: 'time',
  pumpPurge: 'yields',
};

export function groupOf(step: StepAction): StepGroup {
  return GROUP_OF[step.kind];
}

/** Everything about a run that is not a step. */
export type RunConfig = {
  /** Seeds `Math.random`, and with each step's ID, the step's draws. */
  readonly seed: number;
  readonly disabledGroups: readonly StepGroup[];
  /**
   * `stream.getDefaultHighWaterMark(false)` for the run, which is the
   * change-streamer's flow-control threshold. At the 64 KiB default, small
   * rows never reach the stream loop's only await.
   */
  readonly highWaterMark: number;
  readonly viewSyncers: number;
  readonly retentionMs: number;
  readonly purgeBatchRows: number;
  readonly readBatchRows: number;
  readonly barrierTimeoutMs: number;
  readonly reservationMaxAgeMs: number;
  /** How long a view-syncer's restore takes to download its backup. */
  readonly restoreDurationMs: number;
  /** How long an orchestrator takes to restart a view-syncer that exited. */
  readonly vsRestartDelayMs: number;
  readonly checkpointThresholdPages: number;
  readonly maxWalPages: number | undefined;
  /** Parks the purge scheduler between batches until a `pumpPurge` step. */
  readonly gatedPurge: boolean;
  readonly flowControlConsensusTimeoutProportion: number;
  /** Rows per `backfill` message, which stands in for a COPY chunk. */
  readonly batchRows: number;
  /** `BackfillOptions.commitThresholdBytes`. */
  readonly commitThresholdBytes: number;
  /** Whether backfill runs are ordered by row key, and so resumable. */
  readonly resume: boolean;
  /**
   * How long a replication-manager whose slot was taken over keeps running
   * before its orchestrator stops it.
   */
  readonly overlapMs: number;
};

/** Groups a swarm mask may switch off. A run always has a workload. */
const MASKABLE: StepGroup[] = [
  'upstream-faults',
  'rm',
  'backup',
  'view-syncer',
  'time',
  'yields',
];

export const runConfigArb: fc.Arbitrary<RunConfig> = fc.record({
  seed: fc.integer({min: 0, max: 0x7fffffff}),
  disabledGroups: fc.subarray(MASKABLE),
  highWaterMark: fc.constantFrom(256, 1024, 4096, 65536),
  viewSyncers: fc.integer({min: 1, max: 3}),
  retentionMs: fc.constantFrom(1_000, 10_000, 60_000),
  purgeBatchRows: fc.integer({min: 1, max: 16}),
  readBatchRows: fc.integer({min: 1, max: 8}),
  barrierTimeoutMs: fc.constantFrom(1_000, 10_000),
  reservationMaxAgeMs: fc.constantFrom(60_000, 600_000),
  restoreDurationMs: fc.constantFrom(50, 2_000, 20_000),
  vsRestartDelayMs: fc.constantFrom(100, 5_000),
  checkpointThresholdPages: fc.integer({min: 1, max: 32}),
  maxWalPages: fc.option(fc.integer({min: 32, max: 256}), {nil: undefined}),
  gatedPurge: fc.boolean(),
  flowControlConsensusTimeoutProportion: fc.constantFrom(-1, 2),
  batchRows: fc.integer({min: 1, max: 3}),
  commitThresholdBytes: fc.constantFrom(1, 64, 1 << 20),
  resume: fc.constantFrom(true, true, true, false),
  overlapMs: fc.constantFrom(1_000, 10_000, 60_000),
});

/** A run configuration for pinned step lists: nothing masked, nothing gated. */
export const PINNED_CONFIG: RunConfig = {
  seed: 1,
  disabledGroups: [],
  highWaterMark: 1024,
  viewSyncers: 2,
  retentionMs: 10_000,
  purgeBatchRows: 4,
  readBatchRows: 3,
  barrierTimeoutMs: 10_000,
  reservationMaxAgeMs: 600_000,
  restoreDurationMs: 500,
  vsRestartDelayMs: 1_000,
  checkpointThresholdPages: 8,
  maxWalPages: undefined,
  gatedPurge: false,
  flowControlConsensusTimeoutProportion: 2,
  batchRows: 1,
  commitThresholdBytes: 1,
  resume: true,
  overlapMs: 10_000,
};

const vsArb = fc.nat({max: 2});

const actionArb: fc.Arbitrary<StepAction> = fc.oneof(
  {
    weight: 8,
    arbitrary: fc.record({
      kind: fc.constant('commit' as const),
      ops: backfillWorkloadTxArb,
      gap: fc.integer({min: 1, max: 40}),
    }),
  },
  {weight: 1, arbitrary: fc.constant({kind: 'idle' as const})},
  {
    weight: 8,
    arbitrary: fc.record({
      kind: fc.constant('deliver' as const),
      n: fc.integer({min: 1, max: 5}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('sourceDisconnect' as const),
      partial: fc.option(fc.integer({min: 1, max: 4}), {nil: undefined}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('backfillFault' as const),
      fault: fc.oneof(
        fc.constant<BackfillFault>({at: 'snapshot'}),
        fc.record({
          at: fc.constant('copy' as const),
          afterBatches: fc.integer({min: 0, max: 3}),
        }),
      ),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('slotTakeover' as const),
      overlap: fc.boolean(),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('rmCrash' as const),
      shm: fc.boolean(),
    }),
  },
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant('rmCrashAt' as const),
      point: fc.constantFrom(...CRASH_POINTS),
      after: fc.integer({min: 1, max: 3}),
    }),
  },
  {weight: 2, arbitrary: fc.constant({kind: 'rmRestart' as const})},
  {weight: 1, arbitrary: fc.constant({kind: 'rmReplace' as const})},
  {weight: 1, arbitrary: fc.constant({kind: 'deleteChangeLog' as const})},
  {weight: 4, arbitrary: fc.constant({kind: 'backupTake' as const})},
  {weight: 1, arbitrary: fc.constant({kind: 'backupStall' as const})},
  {weight: 2, arbitrary: fc.constant({kind: 'backupResume' as const})},
  {
    weight: 2,
    arbitrary: fc.record({
      kind: fc.constant('vsPull' as const),
      vs: vsArb,
      n: fc.integer({min: 1, max: 20}),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({kind: fc.constant('vsPause' as const), vs: vsArb}),
  },
  {
    weight: 1,
    arbitrary: fc.record({kind: fc.constant('vsResume' as const), vs: vsArb}),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('vsDisconnect' as const),
      vs: vsArb,
      error: fc.boolean(),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({kind: fc.constant('vsRestart' as const), vs: vsArb}),
  },
  {
    weight: 1,
    arbitrary: fc.record({kind: fc.constant('vsWipe' as const), vs: vsArb}),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('vsHoldReservation' as const),
      vs: vsArb,
    }),
  },
  {
    weight: 4,
    arbitrary: fc.record({
      kind: fc.constant('advance' as const),
      ms: fc.oneof(
        fc.integer({min: 1, max: 2_000}),
        fc.integer({min: 5_000, max: 40_000}),
        fc.integer({min: 60_000, max: 700_000}),
      ),
    }),
  },
  {
    weight: 1,
    arbitrary: fc.record({
      kind: fc.constant('pumpPurge' as const),
      batches: fc.integer({min: 1, max: 4}),
    }),
  },
);

export const stepArb: fc.Arbitrary<Step> = fc
  .tuple(actionArb, fc.nat(), fc.integer({min: 1, max: 250}))
  .map(([action, id, dt]) => ({...action, id, dt}));

export function stepsArb(maxLength: number): fc.Arbitrary<Step[]> {
  // fast-check's default `size` keeps arrays near ten elements whatever their
  // `maxLength`, which is too short to reach most states.
  return fc.array(stepArb, {minLength: 1, maxLength, size: 'max'});
}

/** A pinned step with an ID and `dt` filled in. */
export function pinned(actions: readonly StepAction[], dt = 50): Step[] {
  return actions.map((action, id) => ({...action, id, dt}));
}
