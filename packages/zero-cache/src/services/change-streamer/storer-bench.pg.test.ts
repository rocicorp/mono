// oxlint-disable no-console
//
// Perf benchmark for the change-streamer Storer's changeLog ingestion.
//
// Opt-in: this file's timing assertions and logging are gated behind
// `ZERO_BENCH=1` so they stay out of normal CI. Run with:
//
//   ZERO_BENCH=1 pnpm --filter zero-cache run test --run storer-bench
//
// Optionally scale up to reproduce INC-810 magnitude:
//
//   ZERO_BENCH=1 ZERO_BENCH_CHANGES=50000 pnpm --filter zero-cache run test --run storer-bench
//
// Env tunables:
//   ZERO_BENCH_CHANGES   (default 20000) changes in the single giant txn
//   ZERO_BENCH_TXNS      (default 50)    transactions in the sustained stream
//   ZERO_BENCH_TXN_SIZE  (default 400)   changes per txn in the sustained stream
import {appendFileSync} from 'node:fs';
import {beforeEach, describe, expect} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Queue} from '../../../../shared/src/queue.ts';
import {type PgTest, test} from '../../test/db.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import type {PostgresDB} from '../../types/pg.ts';
import {cdcSchema} from '../../types/shards.ts';
import type {Commit} from '../change-source/protocol/current/downstream.ts';
import type {UpstreamStatusMessage} from '../change-source/protocol/current/status.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {ensureReplicationConfig, setupCDCTables} from './schema/tables.ts';
import {Storer, type TuningOptions} from './storer.ts';

const BENCH = process.env.ZERO_BENCH === '1';
const NUM_CHANGES = Number(process.env.ZERO_BENCH_CHANGES ?? 20_000);
const NUM_TXNS = Number(process.env.ZERO_BENCH_TXNS ?? 50);
const TXN_SIZE = Number(process.env.ZERO_BENCH_TXN_SIZE ?? 400);

const REPLICA_VERSION = '00';
const APP_ID = 'xero';
const SHARD_NUM = 5;

const baseOpts: Omit<TuningOptions, 'changeLogBatchSize'> = {
  backPressureLimitHeapProportion: 0.04,
  statementTimeoutMs: 60_000,
};

const messages = new ReplicationMessages({issues: 'id'});

// Reports a result line. Always logs to the console; also appends to the file
// named by ZERO_BENCH_OUT (useful since vitest's `silent: 'passed-only'`
// swallows console output for passing tests).
function report(summary: string) {
  console.log(summary);
  const out = process.env.ZERO_BENCH_OUT;
  if (out) {
    appendFileSync(out, summary + '\n');
  }
}

describe.skipIf(!BENCH)('change-streamer/storer (bench)', () => {
  const lc = createSilentLogContext();
  const shard = {appID: APP_ID, shardNum: SHARD_NUM};
  const schema = cdcSchema(shard);

  let cleanup: (() => Promise<void>)[] = [];

  beforeEach(() => {
    cleanup = [];
    return async () => {
      for (const fn of cleanup.reverse()) {
        await fn();
      }
    };
  });

  /** Creates a fresh CDC database and a running Storer with the given batch size. */
  async function makeStorer(
    testDBs: PgTest['testDBs'],
    name: string,
    changeLogBatchSize: number,
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

    const consumed = new Queue<Commit | UpstreamStatusMessage>();
    const storer = new Storer(
      lc,
      shard,
      'task-id',
      'change-streamer:12345',
      'ws',
      db,
      REPLICA_VERSION,
      msg => consumed.enqueue(msg),
      err => {
        throw err;
      },
      {...baseOpts, changeLogBatchSize},
    );
    await storer.assumeOwnership();
    const done = storer.run();

    cleanup.push(async () => {
      void storer.stop();
      await done;
      await testDBs.drop(db);
    });
    return {db, storer};
  }

  /** Feeds a single transaction of `numChanges` inserts and waits for it to land. */
  async function feedTransaction(
    storer: Storer,
    watermark: string,
    numChanges: number,
  ) {
    storer.store(watermark, [
      'begin',
      messages.begin(),
      {commitWatermark: watermark},
    ]);
    for (let i = 0; i < numChanges; i++) {
      storer.store(watermark, [
        'data',
        messages.insert('issues', {id: `${watermark}-${i}`, title: 'row'}),
      ]);
      // Respect back-pressure so the in-memory queue stays bounded, exactly as
      // the real change-source feed does.
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

  test('single giant transaction: batched vs unbatched', async ({
    testDBs,
  }: PgTest) => {
    const watermark = versionToLexi(1000);

    // Baseline: batching disabled (one INSERT per change).
    const unbatched = await makeStorer(testDBs, 'bench_unbatched', 1);
    const unbatchedSeed = await changeLogRowCount(unbatched.db);
    const heapBefore0 = process.memoryUsage().heapUsed;
    const t0 = performance.now();
    await feedTransaction(unbatched.storer, watermark, NUM_CHANGES);
    await unbatched.storer.allProcessed();
    const unbatchedMs = performance.now() - t0;
    const unbatchedHeapMB =
      (process.memoryUsage().heapUsed - heapBefore0) / 1024 ** 2;
    const unbatchedRows =
      (await changeLogRowCount(unbatched.db)) - unbatchedSeed;

    // Fix: multi-row INSERT batches.
    const batched = await makeStorer(testDBs, 'bench_batched', 2000);
    const batchedSeed = await changeLogRowCount(batched.db);
    const heapBefore1 = process.memoryUsage().heapUsed;
    const t1 = performance.now();
    await feedTransaction(batched.storer, watermark, NUM_CHANGES);
    await batched.storer.allProcessed();
    const batchedMs = performance.now() - t1;
    const batchedHeapMB =
      (process.memoryUsage().heapUsed - heapBefore1) / 1024 ** 2;
    const batchedRows = (await changeLogRowCount(batched.db)) - batchedSeed;

    report(
      `\n[storer-bench] single transaction of ${NUM_CHANGES} changes:\n` +
        `  unbatched (batchSize=1):    ${unbatchedMs.toFixed(0)} ms  (${unbatchedHeapMB.toFixed(1)} MB heap delta)\n` +
        `  batched   (batchSize=2000): ${batchedMs.toFixed(0)} ms  (${batchedHeapMB.toFixed(1)} MB heap delta)\n` +
        `  speedup:                    ${(unbatchedMs / batchedMs).toFixed(2)}x\n`,
    );

    // Correctness: begin + N data + commit.
    expect(unbatchedRows).toBe(NUM_CHANGES + 2);
    expect(batchedRows).toBe(NUM_CHANGES + 2);
    // Regression gate: batching must not be slower than the per-change baseline.
    expect(batchedMs).toBeLessThanOrEqual(unbatchedMs);
  });

  test('sustained transaction stream', async ({testDBs}: PgTest) => {
    const {storer, db} = await makeStorer(testDBs, 'bench_sustained', 2000);
    const seed = await changeLogRowCount(db);

    const totalChanges = NUM_TXNS * TXN_SIZE;
    const t0 = performance.now();
    for (let i = 0; i < NUM_TXNS; i++) {
      await feedTransaction(storer, versionToLexi(2000 + i), TXN_SIZE);
    }
    await storer.allProcessed();
    const elapsedMs = performance.now() - t0;

    const rows = (await changeLogRowCount(db)) - seed;

    report(
      `\n[storer-bench] sustained stream: ${NUM_TXNS} txns x ${TXN_SIZE} changes:\n` +
        `  total:        ${elapsedMs.toFixed(0)} ms\n` +
        `  changes/sec:  ${((totalChanges / elapsedMs) * 1000).toFixed(0)}\n` +
        `  commits/sec:  ${((NUM_TXNS / elapsedMs) * 1000).toFixed(0)}\n`,
    );

    // begin + TXN_SIZE data + commit, per transaction.
    expect(rows).toBe(NUM_TXNS * (TXN_SIZE + 2));
  });
});
