import {describe, expect, test, vi} from 'vitest';
import type {Census} from './census.ts';
import {runSimulation} from './simulation.ts';
import {
  PINNED_CONFIG,
  pinned,
  type RunConfig,
  type StepAction,
} from './steps.ts';
import type {WorkloadOp} from './workload.ts';

vi.mock(import('../../../../zqlite/src/db.ts'), async importOriginal => {
  const {trackDatabases} = await import('./sqlite.ts');
  return trackDatabases(await importOriginal());
});
vi.mock(import('../../observability/metrics.ts'), async importOriginal => {
  const {observeMetrics} = await import('./census.ts');
  return observeMetrics(await importOriginal());
});

/** A table of six rows. */
const table: StepAction = {
  kind: 'commit',
  ops: [
    {op: 'createTable', types: ['text']},
    ...[1, 2, 3, 4, 5, 6].map((id): WorkloadOp => ({
      op: 'insert',
      table: 0,
      id,
      value: id,
    })),
  ],
  gap: 1,
};

/**
 * A column whose values only a backfill can supply, delivered. Its run commits
 * a row per transaction (`PINNED_CONFIG`), so each row moves the mark.
 */
const backfilledColumn: StepAction[] = [
  {
    kind: 'commit',
    ops: [
      {op: 'addColumn', table: 0, type: 'text', value: null, backfilling: true},
    ],
    gap: 1,
  },
  {kind: 'deliver', n: 1},
];

/** The next run fails after `batches` rows, and the manager backs off. */
const failAfter = (batches: number): StepAction => ({
  kind: 'backfillFault',
  fault: {at: 'copy', afterBatches: batches},
});

/** Long enough for the manager's backoff and a run of six rows. */
const settle: StepAction = {kind: 'advance', ms: 30_000};

async function run(
  actions: StepAction[],
  config: Partial<RunConfig> = {},
): Promise<Census> {
  const {census} = await runSimulation(
    {...PINNED_CONFIG, ...config},
    pinned([
      table,
      {kind: 'deliver', n: 1},
      // Every view-syncer finishes its first restore, which the log's reseed
      // at startup sends back to reserve again.
      {kind: 'advance', ms: 10_000},
      ...actions,
    ]),
    {traceFile: process.env['ZERO_SIM_TRACE']},
  );
  return census;
}

// B§7's scenarios, end to end: SimPG, the real backfill manager and
// change-streamer, a backup replicator, and view-syncers. Each run heals, and
// then requires every backfill to have completed on every replica, and every
// replica to equal upstream. Scenario J (`sqliteChangeLogMode = off`) has no
// counterpart here: with the PG change log off, the SQLite log is the only one.
describe('sim/backfills in the composed system', () => {
  test('scenario A: a view-syncer that finished moves to a manager mid-run', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      // Mid-run: the backup holds the mark the run failed at.
      {kind: 'backupTake'},
      // The run finishes here, and so do the view-syncers.
      settle,
      // A new task, restored from the mid-run backup, resumes the run.
      {kind: 'slotTakeover', overlap: false},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('rm:replaced-mid-run'), entries).toBe(1);
    expect(census.count('backfill:run/resumed'), entries).toBeGreaterThan(0);
  });

  test('scenario B: a view-syncer at a mark moves to a manager that finished', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      // vs-0 stops at the mark the run failed at.
      {kind: 'vsPause', vs: 0},
      settle,
      // The run finishes here, and a new task restores from after it.
      {kind: 'backupTake'},
      {kind: 'rmReplace'},
      {kind: 'vsResume', vs: 0},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('declaration:forwarded'), entries).toBeGreaterThan(0);
  });

  test('scenario C: a view-syncer at a mark moves to a manager mid-run elsewhere', async () => {
    const census = await run([
      failAfter(1),
      ...backfilledColumn,
      // vs-0 stops at the first mark.
      {kind: 'vsPause', vs: 0},
      // The next run, which starts from the beginning after a failure, fails
      // two rows in. A backup holds that mark before the run after it, which
      // waits out a longer backoff.
      failAfter(2),
      {kind: 'advance', ms: 2_500},
      {kind: 'backupTake'},
      // Upstream moves on, undelivered, so that the next run cannot complete:
      // it waits for the stream to reach its snapshot.
      {kind: 'commit', ops: [{op: 'createTable', types: ['int8']}], gap: 1},
      // A new task resumes from the backup's mark, which vs-0 is not at, and
      // is still running when vs-0 declares its own.
      {kind: 'slotTakeover', overlap: false},
      {kind: 'vsResume', vs: 0},
      settle,
      {kind: 'deliver', n: 1},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('rm:replaced-mid-run'), entries).toBe(1);
    expect(census.count('backfill:run/resumed'), entries).toBeGreaterThan(0);
    expect(census.count('backfill:restart/declaration'), entries).toBe(1);
  });

  test('scenario D: a view-syncer reconnects to the same manager mid-run', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      {kind: 'vsDisconnect', vs: 0, error: false},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('backfill:restart/declaration'), entries).toBe(0);
  });

  test('scenario E: the manager restarts mid-run', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      {kind: 'rmRestart'},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('connect:resume-mark'), entries).toBe(0);
    expect(census.count('backfill:run/resumed'), entries).toBeGreaterThan(0);
  });

  test('scenario F: a view-syncer restores from a backup taken mid-run', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      {kind: 'backupTake'},
      {kind: 'vsWipe', vs: 0},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('restore:mid-run'), entries).toBe(1);
    expect(census.count('backfill:restart/declaration'), entries).toBe(0);
  });

  test('scenario G: a row key change passes a view-syncer that is behind', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      {kind: 'vsPause', vs: 0},
      // Row 5 moves below the mark.
      {
        kind: 'commit',
        ops: [{op: 'keyChange', table: 0, id: 5, to: 0, value: 5}],
        gap: 1,
      },
      {kind: 'deliver', n: 1},
      settle,
      {kind: 'vsResume', vs: 0},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('backfill:run/zero'), entries).toBeGreaterThan(1);
  });

  test('scenario H: ten view-syncers restore from one backup mid-run', async () => {
    const census = await run(
      [
        failAfter(2),
        ...backfilledColumn,
        {kind: 'backupTake'},
        ...Array.from({length: 10}, (_, vs): StepAction => ({
          kind: 'vsWipe',
          vs,
        })),
        settle,
      ],
      {viewSyncers: 10},
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('restore:mid-run'), entries).toBe(10);
    expect(
      census.count('backfill:restart/declaration'),
      entries,
    ).toBeLessThanOrEqual(1);
  });

  test('scenario I: a run that cannot resume starts from the beginning', async () => {
    const census = await run(
      [failAfter(2), ...backfilledColumn, {kind: 'rmRestart'}, settle],
      {resume: false},
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('backfill:run/resumed'), entries).toBe(0);
    expect(census.count('backfill:run/zero'), entries).toBeGreaterThan(1);
  });

  // Found by the sweep. A task restored from a backup older than the table
  // starts its stream there. A view-syncer ahead of it declares the table's
  // column in flight, and the manager, which has no entry for the table, runs
  // it at once, at the head. The run's rows reach the task's own backup
  // replicator before the stream has created the table, and `processBackfill`
  // looked the table up before its column guard, so the replicator failed.
  test('a replica behind a run skips rows for a table it has not created yet', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      // From the backup at the replica version, taken before the table.
      {kind: 'rmReplace'},
      settle,
      {kind: 'deliver', n: 5},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('declaration:forwarded'), entries).toBeGreaterThan(0);
  });

  test('an overlapping replication-manager exits once it reads that it lost the slot', async () => {
    const census = await run([
      failAfter(2),
      ...backfilledColumn,
      {kind: 'backupTake'},
      {kind: 'slotTakeover', overlap: true},
      {kind: 'deliver', n: 1},
      settle,
    ]);
    const entries = JSON.stringify(census.entries());
    expect(census.count('rm:takeover/overlap'), entries).toBe(1);
    expect(census.count('slot:takeover'), entries).toBe(1);
    expect(census.count('rm:exited/slot-taken'), entries).toBe(1);
  });
});
