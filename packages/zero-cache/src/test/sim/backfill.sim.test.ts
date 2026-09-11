import {writeFileSync} from 'node:fs';
import fc from 'fast-check';
import {describe, expect, test, vi} from 'vitest';
import {
  backfillRunConfigArb,
  backfillStepsArb,
  PINNED_BACKFILL_CONFIG,
  pinnedBackfill,
  runBackfillSimulation,
  type BackfillStepAction,
} from './backfill-sim.ts';
import {Census} from './census.ts';
import {knownBug} from './known-bugs.ts';
import type {WorkloadOp} from './workload.ts';

vi.mock(import('../../observability/metrics.ts'), async importOriginal => {
  const {observeMetrics} = await import('./census.ts');
  return observeMetrics(await importOriginal());
});

const RUNS = Number(process.env['ZERO_SIM_RUNS'] ?? 10);
const MAX_STEPS = Number(process.env['ZERO_SIM_STEPS'] ?? 60);
const SEED = process.env['ZERO_SIM_SEED'];
const PATH = process.env['ZERO_SIM_PATH'];
const CENSUS_OUT = process.env['ZERO_SIM_CENSUS_OUT'];

const SWEEP_TIMEOUT_MS = 30 * 60_000;

/** A table of six rows. */
const table: BackfillStepAction = {
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

/** A column whose values only a backfill can supply. */
const backfilledColumn: BackfillStepAction = {
  kind: 'commit',
  ops: [
    {op: 'addColumn', table: 0, type: 'text', value: null, backfilling: true},
  ],
  gap: 1,
};

describe('sim/backfills, against one replica', () => {
  test('a backfill runs to completion', async () => {
    const {census} = await runBackfillSimulation(
      PINNED_BACKFILL_CONFIG,
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        backfilledColumn,
        {kind: 'deliver', n: 1},
      ]),
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('backfill:run/zero'), entries).toBeGreaterThan(0);
    expect(census.count('completion:honored'), entries).toBe(1);
  });

  test('a manager restart resumes the run from the replica mark (scenario E)', async () => {
    const {census} = await runBackfillSimulation(
      PINNED_BACKFILL_CONFIG,
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        {kind: 'pause'},
        backfilledColumn,
        {kind: 'deliver', n: 1},
        // The add-column transaction, then the run's announcement and its
        // first rows, committed.
        {kind: 'pull', n: 3},
        {kind: 'pull', n: 6},
        {kind: 'managerRestart'},
        {kind: 'resume'},
      ]),
    );
    expect(
      census.count('backfill:run/resumed'),
      JSON.stringify(census.entries()),
    ).toBeGreaterThan(0);
  });

  // Known issue, found by this harness. When a source restarts, the
  // change-streamer re-sends every connected subscriber's declarations at once
  // (`requestBackfills(true)`), before the run the new session resumed from the
  // replica's mark has sent a batch. `BackfillManager.onBackfillRequest`
  // restarts a run with no `lastMark` from the beginning, as it must for an
  // unordered run, even though this one is ordered and the subscriber declares
  // the very mark it resumes from. So no run resumes across an upstream
  // reconnect while a subscriber is connected. When that is fixed, expect
  // `backfill:run/resumed` here instead.
  test('an upstream reconnect restarts the resumed run from the beginning (known issue)', async () => {
    const {census} = await runBackfillSimulation(
      PINNED_BACKFILL_CONFIG,
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        {kind: 'pause'},
        backfilledColumn,
        {kind: 'deliver', n: 1},
        {kind: 'pull', n: 3},
        {kind: 'pull', n: 6},
        {kind: 'sourceDisconnect', partial: undefined},
        // Reads until it finds the stream gone; the next step reconnects.
        {kind: 'pull', n: 20},
        {kind: 'resume'},
      ]),
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('connect:resume-mark'), entries).toBeGreaterThan(0);
    expect(census.count('backfill:restart/declaration'), entries).toBe(1);
    expect(census.count('backfill:run/resumed'), entries).toBe(0);
  });

  // Known bug, found by this harness. A run whose snapshot is ahead of the
  // replica sends the row at the new key of a row key change that the replica
  // has not yet applied, as a phantom. The replicated key change then updates
  // the old row's key onto the phantom's (`ChangeProcessor.processUpdate`),
  // which fails on the row key's uniqueness and stops replication. When that
  // is fixed, expect the run to complete instead.
  test('a row key change onto a phantom row stops replication (known bug)', async () => {
    await expect(
      runBackfillSimulation(
        PINNED_BACKFILL_CONFIG,
        pinnedBackfill([
          table,
          {kind: 'deliver', n: 1},
          backfilledColumn,
          // Committed upstream, but not delivered: the run's snapshot has it.
          {
            kind: 'commit',
            ops: [{op: 'keyChange', table: 0, id: 1, to: 7, value: 1}],
            gap: 1,
          },
          {kind: 'deliver', n: 1},
          {kind: 'deliver', n: 1},
        ]),
      ),
    ).rejects.toThrow('UNIQUE constraint failed: t1.id');
  });

  // Found by this harness. A backfill stream that failed mid-run (a lost COPY
  // connection) left the manager's backfill transaction open and its
  // reservation of the change stream held: `#runBackfill` neither ended the
  // transaction nor released on an error, and the multiplexer had nothing that
  // would. Nothing was replicated again until the change stream restarted.
  test('a backfill that fails mid-run releases the change stream, and is retried', async () => {
    const {census} = await runBackfillSimulation(
      PINNED_BACKFILL_CONFIG,
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        {kind: 'backfillFault', fault: {at: 'copy', afterBatches: 1}},
        backfilledColumn,
        {kind: 'deliver', n: 1},
      ]),
    );
    expect(
      census.count('completion:honored'),
      JSON.stringify(census.entries()),
    ).toBe(1);
  });

  // §5 row 7, which the sweep does not line up by itself. A v17 zero-cache
  // replicates a row key change that moves a row below the replica's mark and
  // leaves the column in flight out (a TOASTed value), and voids no mark. The
  // roll forward to v18 must forget the mark: a run resumed from it skips the
  // moved row, and completes with that row's column empty.
  test('rolling forward from v17 forgets a mark a row key change passed', async () => {
    const {census} = await runBackfillSimulation(
      {...PINNED_BACKFILL_CONFIG, batchRows: 2},
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        // The run commits rows 1-2, and so a mark at 2, then fails: the next
        // run waits out the manager's backoff.
        {kind: 'backfillFault', fault: {at: 'copy', afterBatches: 2}},
        backfilledColumn,
        {kind: 'deliver', n: 1},
        // Before it: row 5 moves to 0, below the mark, under v17.
        {kind: 'v17KeyChange', table: 0, from: 5, to: 0},
      ]),
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('v17:rollback'), entries).toBe(1);
    expect(census.count('completion:honored'), entries).toBe(1);
  });

  test('a stale run is ignored by a subscriber that finished (scenario A)', async () => {
    const {census} = await runBackfillSimulation(
      PINNED_BACKFILL_CONFIG,
      pinnedBackfill([
        table,
        {kind: 'deliver', n: 1},
        backfilledColumn,
        {kind: 'deliver', n: 1},
        // Upstream moves on: every row changes after the backfill.
        {
          kind: 'commit',
          ops: [1, 2, 3, 4, 5, 6].map((id): WorkloadOp => ({
            op: 'update',
            table: 0,
            id,
            value: 40 + id,
          })),
          gap: 1,
        },
        {kind: 'deliver', n: 1},
        {kind: 'replayStaleRun'},
      ]),
    );
    const entries = JSON.stringify(census.entries());
    expect(census.count('backfill:stale-replay'), entries).toBe(1);
    expect(census.count('completion:honored'), entries).toBe(1);
  });

  test.each([true, false])(
    'a running manager covers a subscriber from another run (scenario C, resume=%s)',
    async resume => {
      const {census} = await runBackfillSimulation(
        {...PINNED_BACKFILL_CONFIG, resume},
        pinnedBackfill([
          table,
          {kind: 'deliver', n: 1},
          {kind: 'pause'},
          backfilledColumn,
          {kind: 'deliver', n: 1},
          // The add-column transaction, then the run's announcement and its
          // first rows, committed.
          {kind: 'pull', n: 3},
          {kind: 'pull', n: 6},
          {kind: 'forgetRun'},
          {kind: 'resume'},
        ]),
      );
      const entries = JSON.stringify(census.entries());
      expect(census.count('backfill:forgot-run'), entries).toBe(1);
      // An unordered run restarts; an ordered one re-announces or restarts.
      expect(
        census.count('backfill:restart/declaration') +
          census.count('backfill:reannounced'),
        entries,
      ).toBeGreaterThan(0);
      expect(census.count('completion:honored'), entries).toBe(1);
    },
  );

  test(
    'sweep',
    async () => {
      const census = new Census();
      const started = performance.now();
      await fc.assert(
        fc.asyncProperty(
          backfillRunConfigArb,
          backfillStepsArb(MAX_STEPS),
          async (config, steps) => {
            try {
              census.merge((await runBackfillSimulation(config, steps)).census);
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
    },
    SWEEP_TIMEOUT_MS,
  );
});
