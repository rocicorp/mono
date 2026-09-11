import {writeFileSync} from 'node:fs';
import fc from 'fast-check';
import {describe, expect, test, vi} from 'vitest';
import {Census} from './census.ts';
import {knownBug} from './known-bugs.ts';
import {runSimulation} from './simulation.ts';
import {
  PINNED_CONFIG,
  pinned,
  runConfigArb,
  stepsArb,
  type StepAction,
} from './steps.ts';

vi.mock(import('../../../../zqlite/src/db.ts'), async importOriginal => {
  const {trackDatabases} = await import('./sqlite.ts');
  return trackDatabases(await importOriginal());
});
vi.mock(import('../../observability/metrics.ts'), async importOriginal => {
  const {observeMetrics} = await import('./census.ts');
  return observeMetrics(await importOriginal());
});

const RUNS = Number(process.env['ZERO_SIM_RUNS'] ?? 10);
const MAX_STEPS = Number(process.env['ZERO_SIM_STEPS'] ?? 60);
const SEED = process.env['ZERO_SIM_SEED'];
const PATH = process.env['ZERO_SIM_PATH'];
const TRACE = process.env['ZERO_SIM_TRACE'];
const CENSUS_OUT = process.env['ZERO_SIM_CENSUS_OUT'];

const SWEEP_TIMEOUT_MS = 30 * 60_000;

/**
 * Outcomes that a sweep of at least {@link CENSUS_MIN_RUNS} runs must reach, so
 * that generator weights that drift away from a path are noticed. Rarer ones --
 * a subscriber registered during a flow-control wait, a consensus-timeout
 * release, a replication-manager replaced mid-run -- are counted but not
 * required: a thousand runs reach them only a few times.
 */
const CENSUS_MIN_RUNS = 1000;
const EXPECTED_OUTCOMES = [
  'backfill:completed',
  'backfill:restart/declaration',
  'backfill:restart/key-change',
  'backfill:run/resumed',
  'backfill:run/zero',
  'checkpointer:soft-wait-timeout',
  'crash:after-forward',
  'crash:after-log-commit',
  'crash:before-log-commit',
  'crash:mid-flush',
  'declaration:forwarded',
  'flow-control:wait/all-subscribers',
  'reconcile:keep',
  'reconcile:reseeded/created',
  'reservation:confirm-delayed',
  'reservation:expired',
  'reservation:invalidated/created',
  'reservation:taken-back-mid-restore',
  'restore:invalid',
  'restore:kept',
  'restore:mid-run',
  'restore:wedged-through-heal',
  'rm:crash/mid-burst',
  'rm:exited/slot-taken',
  'rm:takeover/fenced',
  'rm:takeover/overlap',
  'route:none/watermark-uncovered',
  'route:sqlite/selected',
  'route:sqlite/selected-cold',
  'subscriber:error/WatermarkTooOld',
];

const insert = (id: number): StepAction => ({
  kind: 'commit',
  ops: [
    {op: 'createTable', types: ['text', 'int8']},
    {op: 'insert', table: 0, id, value: id},
  ],
  gap: 3,
});

describe('sim/replication-manager, with the SQLite change log only', () => {
  test('a quiet run replicates to its view-syncers and drains its log', async () => {
    const {census} = await runSimulation(
      PINNED_CONFIG,
      pinned([
        insert(1),
        {kind: 'deliver', n: 1},
        {
          kind: 'commit',
          ops: [{op: 'update', table: 0, id: 1, value: 9}],
          gap: 40,
        },
        {kind: 'deliver', n: 5},
        {kind: 'backupTake'},
        {kind: 'advance', ms: 45_000},
      ]),
      {guard: true},
    );
    expect(
      census.count('route:sqlite/selected') +
        census.count('route:sqlite/selected-cold'),
      JSON.stringify(census.entries()),
    ).toBeGreaterThan(0);
  });

  test('a crash and restart keeps the log, and resumes from its head (C5)', async () => {
    const {census} = await runSimulation(
      PINNED_CONFIG,
      pinned([
        insert(1),
        {kind: 'deliver', n: 1},
        {kind: 'backupTake'},
        {kind: 'rmCrash', shm: true},
        {kind: 'advance', ms: 2_000},
        {kind: 'rmRestart'},
        insert(2),
        {kind: 'deliver', n: 5},
      ]),
    );
    expect(census.count('reconcile:keep')).toBeGreaterThan(0);
  });

  test('a deleted change log is reseeded at the next start (C6)', async () => {
    const {census} = await runSimulation(
      PINNED_CONFIG,
      pinned([
        insert(1),
        {kind: 'deliver', n: 1},
        {kind: 'deleteChangeLog'},
        {kind: 'rmRestart'},
        insert(2),
        {kind: 'deliver', n: 5},
      ]),
    );
    expect(census.count('reconcile:reseeded/created')).toBeGreaterThan(1);
  });

  // Found by the sweep. After a stream restart, `UpstreamAcker` counted no
  // transaction as outstanding, so it acked a keepalive's watermark past
  // transactions that only the change log held. The task restored from the
  // backup then resumed below the slot, which the upstream moves forward
  // silently, and the create-table in between was never replicated.
  test('a keepalive after a reconnect does not move the slot past the backup', async () => {
    await runSimulation(
      {...PINNED_CONFIG, viewSyncers: 1},
      pinned([
        {kind: 'backupTake'},
        insert(1),
        {kind: 'deliver', n: 1},
        {kind: 'sourceDisconnect', partial: undefined},
        {kind: 'advance', ms: 1_000},
        {kind: 'idle'},
        {kind: 'rmReplace'},
        {
          kind: 'commit',
          ops: [{op: 'insert', table: 0, id: 2, value: 2}],
          gap: 3,
        },
        {kind: 'deliver', n: 5},
      ]),
    );
  });

  // Found by the sweep. The stream loop forwarded a commit with flow control
  // and awaited the stalled backup replicator. A disconnect during that wait
  // aborted it with the transaction's watermark still set, so the streamer
  // "rolled back" a transaction it had already forwarded whole, and its
  // transaction bookkeeping failed the change-streamer.
  test('a disconnect while a forwarded commit awaits flow control does not roll it back', async () => {
    await runSimulation(
      {
        ...PINNED_CONFIG,
        highWaterMark: 256,
        viewSyncers: 1,
        retentionMs: 1_000,
        checkpointThresholdPages: 1,
        flowControlConsensusTimeoutProportion: -1,
      },
      pinned([
        insert(1),
        {kind: 'backupStall'},
        {kind: 'commit', ops: [{op: 'delete', table: 0, id: 1}], gap: 1},
        {kind: 'deliver', n: 2},
        {kind: 'sourceDisconnect', partial: undefined},
        {kind: 'advance', ms: 60_000},
      ]),
    );
  });

  test(
    'sweep',
    async () => {
      const census = new Census();
      const started = performance.now();
      await fc.assert(
        fc.asyncProperty(
          runConfigArb,
          stepsArb(MAX_STEPS),
          async (config, steps) => {
            try {
              const result = await runSimulation(config, steps, {
                traceFile: TRACE,
              });
              census.merge(result.census);
            } catch (e) {
              const known = knownBug(e);
              if (!known) {
                throw e;
              }
              census.note(`known-bug:${known}`);
            }
          },
        ),
        {
          numRuns: RUNS,
          ...(SEED === undefined ? {} : {seed: Number(SEED)}),
          ...(PATH === undefined ? {} : {path: PATH}),
          // Shrinking replays many runs, so it is opt-in, for a counterexample
          // already found (with ZERO_SIM_SEED and ZERO_SIM_PATH).
          endOnFailure: process.env['ZERO_SIM_SHRINK'] === undefined,
        },
      );
      if (CENSUS_OUT) {
        writeFileSync(
          CENSUS_OUT,
          JSON.stringify(
            {
              runs: RUNS,
              ms: Math.round(performance.now() - started),
              census: Object.fromEntries(census.entries()),
            },
            null,
            2,
          ),
        );
      }
      if (RUNS >= CENSUS_MIN_RUNS && PATH === undefined) {
        expect(
          census.missing(EXPECTED_OUTCOMES),
          JSON.stringify(census.entries()),
        ).toEqual([]);
      }
    },
    SWEEP_TIMEOUT_MS,
  );
});
