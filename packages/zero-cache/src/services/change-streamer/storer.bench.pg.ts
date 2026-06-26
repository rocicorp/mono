// Benchmarks synthetic replication messages entering the ChangeDB through the
// change-streamer Storer. This is not an end-to-end logical replication benchmark.

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {type PgTest, test} from '../../test/db.ts';
import {
  BENCHMARK_FIXTURE_TABLE_KEYS,
  makeBenchmarkFixtureRows,
  type BenchmarkFixtureRow,
} from '../../test/pg-bench.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cdcSchema} from '../../types/shards.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer} from './storer.ts';

const SINGLE_TRANSACTION_CHANGES = 200_000;
const SUSTAINED_TRANSACTIONS = 200;
const SUSTAINED_CHANGES_PER_TRANSACTION = 1_000;
const CHANGE_LOG_BATCH_SIZE = 2000;
const WARMUP_REPS = 1;
const REPS = 10;

const REPLICA_VERSION = '00';
const APP_ID = 'storer_bench_app';
const SHARD_NUM = 5;
const TEST_TIMEOUT_MS = 900_000;

const lc = createSilentLogContext();
const shard = {appID: APP_ID, shardNum: SHARD_NUM};
const schema = cdcSchema(shard);
const messages = new ReplicationMessages(BENCHMARK_FIXTURE_TABLE_KEYS);
const benchmarkRecorder = createManualBenchmarkRecorder();
const storerOptions = {
  backPressureLimitHeapProportion: 0.04,
  statementTimeoutMs: 60_000,
  changeLogBatchSize: CHANGE_LOG_BATCH_SIZE,
};

let cleanup: (() => Promise<void>)[] = [];

async function runCleanup() {
  for (const fn of cleanup.reverse()) {
    await fn();
  }
  cleanup = [];
}

afterEach(async () => {
  await runCleanup();
});

async function makeStorer(
  testDBs: PgTest['testDBs'],
  name: string,
): Promise<{db: PostgresDB; storer: Storer}> {
  const db = await testDBs.create(name, {
    typeOpts: {sendStringAsJson: true},
  });
  await db.begin(tx => setupCDCTables(lc, tx, shard));
  await ensureReplicationConfig(
    lc,
    db,
    {
      replicaVersion: REPLICA_VERSION,
      publications: [],
      watermark: REPLICA_VERSION,
    },
    shard,
    true,
  );

  const storer = new Storer(
    lc,
    shard,
    'task-id',
    'change-streamer:12345',
    'ws',
    db,
    REPLICA_VERSION,
    () => {},
    err => {
      throw err;
    },
    storerOptions,
  );
  await storer.assumeOwnership();
  const done = storer.run();

  cleanup.push(async () => {
    await storer.stop();
    await done;
    await testDBs.drop(db);
  });
  return {db, storer};
}

async function feedTransaction(
  storer: Storer,
  watermark: string,
  rows: readonly BenchmarkFixtureRow[],
) {
  storer.store(watermark, [
    'begin',
    messages.begin(),
    {commitWatermark: watermark},
  ]);
  for (const {table, row} of rows) {
    storer.store(watermark, ['data', messages.insert(table, row)]);
    const ready = storer.readyForMore();
    if (ready) {
      await ready;
    }
  }
  storer.store(watermark, ['commit', messages.commit(), {watermark}]);
}

async function changeLogRowCount(db: PostgresDB): Promise<number> {
  const [{n}] = await db<{n: number}[]>`
      SELECT count(*)::int AS n FROM ${db(schema)}.${db('changeLog')}`;
  return n;
}

describe('change-streamer/storer throughput', () => {
  test(
    'single transaction changes/sec',
    {timeout: TEST_TIMEOUT_MS},
    async ({testDBs}: PgTest) => {
      const samples: number[] = [];

      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        const watermark = versionToLexi(1000 + rep);
        const rows = makeBenchmarkFixtureRows(
          rep * SINGLE_TRANSACTION_CHANGES + 1,
          SINGLE_TRANSACTION_CHANGES,
        );
        const {db, storer} = await makeStorer(testDBs, `bench_single_${rep}`);
        const seed = await changeLogRowCount(db);
        const start = performance.now();
        await feedTransaction(storer, watermark, rows);
        await storer.allProcessed();
        const elapsed = performance.now() - start;

        const changeLogRows = (await changeLogRowCount(db)) - seed;
        expect(changeLogRows).toBe(SINGLE_TRANSACTION_CHANGES + 2);
        if (rep >= WARMUP_REPS) {
          samples.push(elapsed);
        }
        await runCleanup();
      }

      benchmarkRecorder.recordThroughput(
        'change-streamer/storer single transaction changes',
        samples,
        SINGLE_TRANSACTION_CHANGES,
      );
    },
  );

  test(
    'sustained stream changes/sec and commits/sec',
    {timeout: TEST_TIMEOUT_MS},
    async ({testDBs}: PgTest) => {
      const samples: number[] = [];
      const totalChanges =
        SUSTAINED_TRANSACTIONS * SUSTAINED_CHANGES_PER_TRANSACTION;

      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        const rows = makeBenchmarkFixtureRows(
          rep * totalChanges + 1,
          totalChanges,
        );
        const transactions = Array.from(
          {length: SUSTAINED_TRANSACTIONS},
          (_, i) =>
            rows.slice(
              i * SUSTAINED_CHANGES_PER_TRANSACTION,
              (i + 1) * SUSTAINED_CHANGES_PER_TRANSACTION,
            ),
        );
        const {db, storer} = await makeStorer(
          testDBs,
          `bench_sustained_${rep}`,
        );
        const seed = await changeLogRowCount(db);
        const start = performance.now();
        for (let i = 0; i < SUSTAINED_TRANSACTIONS; i++) {
          await feedTransaction(
            storer,
            versionToLexi(2000 + rep * SUSTAINED_TRANSACTIONS + i),
            transactions[i],
          );
        }
        await storer.allProcessed();
        const elapsed = performance.now() - start;

        const changeLogRows = (await changeLogRowCount(db)) - seed;
        expect(changeLogRows).toBe(
          SUSTAINED_TRANSACTIONS * (SUSTAINED_CHANGES_PER_TRANSACTION + 2),
        );
        if (rep >= WARMUP_REPS) {
          samples.push(elapsed);
        }
        await runCleanup();
      }

      benchmarkRecorder.recordThroughput(
        'change-streamer/storer sustained stream changes',
        samples,
        totalChanges,
      );
      benchmarkRecorder.recordThroughput(
        'change-streamer/storer sustained stream commits',
        samples,
        SUSTAINED_TRANSACTIONS,
      );
    },
  );
});
