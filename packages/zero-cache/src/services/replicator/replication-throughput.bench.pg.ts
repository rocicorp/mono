// Benchmarks end-to-end logical replication throughput from upstream Postgres
// writes through the change-streamer and into the SQLite replica.
//
// The change log is written by the change-streamer rather than by the
// replicator. The three modes below measure the complete PG-to-RM path with the
// legacy PG log, both logs, and only the SQLite log. No view-syncer subscribes,
// so its backpressure cannot hide the change-streamer's throughput ceiling.
//
//   pnpm --filter zero-cache run bench:pg replication-throughput

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {getConnectionURI, type PgTest, test} from '../../test/db.ts';
import {DbFile} from '../../test/lite.ts';
import {
  BENCHMARK_FIXTURE_PUBLICATION,
  benchmarkFixturePayloadMB,
  benchmarkFixtureReplicaRowCount,
  insertBenchmarkFixtureRowBatches,
  insertBenchmarkFixtureRows,
  makeBenchmarkFixtureRowBatches,
  setupBenchmarkFixture,
} from '../../test/pg-bench.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Source} from '../../types/streams.ts';
import {getPragmaConfig, setupReplica} from '../../workers/replicator.ts';
import {initializePostgresChangeSource} from '../change-source/pg/change-source.ts';
import {
  initializeStreamer,
  type TuningOptions,
} from '../change-streamer/change-streamer-service.ts';
import {
  type ChangeStreamer,
  type ChangeStreamerService,
  type Downstream,
  type SerializedDownstream,
} from '../change-streamer/change-streamer.ts';
import {initChangeStreamerSchema} from '../change-streamer/schema/init.ts';
import {changeLogFileName, deleteChangeLogDB} from './change-log-db.ts';
import {ReplicationStatusPublisher} from './replication-status.ts';
import {ReplicatorService} from './replicator.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const CHANGE_LOG_BATCH_SIZE = 2000;
const MEASURED_WARMUP_REPS = 1;
const REPS = 10;

const APP_ID = 'logical_replication_bench_app';
const SHARD_NUM = 0;
const TASK_ID = 'logical-replication-throughput-bench';
const TEST_TIMEOUT_MS = 900_000;

const lc = createSilentLogContext();
const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [BENCHMARK_FIXTURE_PUBLICATION],
};
const benchmarkRecorder = createManualBenchmarkRecorder();
const baseStreamerOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusTimeoutProportion: 2,
  statementTimeoutMs: 60_000,
  changeLogBatchSize: CHANGE_LOG_BATCH_SIZE,
} satisfies Omit<TuningOptions, 'pgChangeLogEnabled'>;

const changeLogModes = [
  {name: 'pg-only', pgChangeLogEnabled: true, sqliteChangeLogEnabled: false},
  {name: 'dual', pgChangeLogEnabled: true, sqliteChangeLogEnabled: true},
  {
    name: 'sqlite-only',
    pgChangeLogEnabled: false,
    sqliteChangeLogEnabled: true,
  },
] as const;

type ChangeLogMode = (typeof changeLogModes)[number];

const workloads = [
  {
    name: 'single-row-transactions',
    warmupRows: 2_500,
    measuredRows: 5_000,
    rowsPerTransaction: 1,
  },
  {
    name: '500-row-transactions',
    warmupRows: 25_000,
    measuredRows: 100_000,
    rowsPerTransaction: 500,
  },
] as const;

type Workload = (typeof workloads)[number];

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

function parseStringifiedSource(
  source: Source<string>,
): Source<SerializedDownstream> {
  return {
    cancel: err => source.cancel(err),
    signal: source.signal,
    async *[Symbol.asyncIterator]() {
      for await (const json of source) {
        yield {data: BigIntJSON.parse(json) as Downstream, json};
      }
    },
  };
}

function parseStringifiedChangeStreamer(
  streamer: ChangeStreamerService,
): ChangeStreamer {
  return {
    async subscribe(ctx) {
      return parseStringifiedSource(await streamer.subscribe(ctx));
    },
  };
}

function replicaRowCount(replicaPath: string): number {
  return benchmarkFixtureReplicaRowCount(lc, replicaPath);
}

async function waitForReplicaRows(replicaPath: string, expected: number) {
  const deadline = performance.now() + TEST_TIMEOUT_MS;
  let actual = 0;
  while (performance.now() < deadline) {
    actual = replicaRowCount(replicaPath);
    if (actual >= expected) {
      return actual;
    }
    await sleep(50);
  }
  throw new Error(
    `timed out waiting for replica rows: expected ${expected}, got ${actual}`,
  );
}

async function waitForPGChangeLog(
  changeDB: PostgresDB,
  targetWatermark: string,
): Promise<void> {
  const deadline = performance.now() + TEST_TIMEOUT_MS;
  let lastWatermark = '';
  while (performance.now() < deadline) {
    [{lastWatermark}] = await changeDB<{lastWatermark: string}[]> /*sql*/ `
      SELECT "lastWatermark"
        FROM ${changeDB(`${shard.appID}_${shard.shardNum}/cdc`)}."replicationState"`;
    if (lastWatermark >= targetWatermark) {
      return;
    }
    await sleep(50);
  }
  throw new Error(
    `timed out waiting for PG change log: expected ${targetWatermark}, got ${lastWatermark}`,
  );
}

async function waitForReplication(
  replicaPath: string,
  expectedRows: number,
  changeDB: PostgresDB,
  mode: ChangeLogMode,
): Promise<number> {
  const actualRows = await waitForReplicaRows(replicaPath, expectedRows);
  if (mode.pgChangeLogEnabled) {
    using replica = new Database(lc, replicaPath, {readonly: true});
    const {stateVersion} = replica
      .prepare(`SELECT "stateVersion" FROM "_zero.replicationState"`)
      .get<{stateVersion: string}>();
    await waitForPGChangeLog(changeDB, stateVersion);
  }
  return actualRows;
}

async function startReplicationPipeline(
  testDBs: PgTest['testDBs'],
  mode: ChangeLogMode,
) {
  const upstream = await testDBs.create(
    `logical_replication_bench_upstream_${mode.name}`,
  );
  const changeDB = await testDBs.create(
    `logical_replication_bench_change_${mode.name}`,
    {typeOpts: {sendStringAsJson: true}},
  );
  const replicaDbFile = new DbFile(
    `logical-replication-throughput-bench-${mode.name}`,
  );

  // oxlint-disable require-await
  cleanup.push(async () => {
    deleteChangeLogDB(replicaDbFile.path);
    replicaDbFile.delete();
  });
  cleanup.push(async () => {
    await testDBs.drop(upstream, changeDB);
  });

  await setupBenchmarkFixture(upstream, {
    publication: BENCHMARK_FIXTURE_PUBLICATION,
  });
  const upstreamURI = getConnectionURI(upstream);
  const {subscriptionState, changeSource, replicaID} =
    await initializePostgresChangeSource(
      lc,
      upstreamURI,
      shard,
      replicaDbFile.path,
      {tableCopyWorkers: 5},
      {bench: 'logical-replication-throughput'},
    );

  await setupReplica(lc, 'serving', {file: replicaDbFile.path});
  await initChangeStreamerSchema(lc, changeDB, shard);
  const sqliteOptions = mode.sqliteChangeLogEnabled
    ? {
        sqliteChangeLogWriter: {
          replicaFile: replicaDbFile.path,
          identity: {
            epoch: null,
            generation: subscriptionState.replicaVersion,
            replicaID,
          },
        },
        sqliteChangeLogPurge: {retentionMs: 60_000, batchRows: 1_000},
        sqliteCatchup: {
          changeLogFile: changeLogFileName(replicaDbFile.path),
          readBatchRows: 1_000,
          barrierTimeoutMs: 300_000,
        },
        sqliteChangeLogServe: {
          readPercent: 100,
          coldReadPercent: 100,
          retentionMs: 60_000,
        },
      }
    : {};
  const changeStreamer = await initializeStreamer(
    lc,
    shard,
    TASK_ID,
    'change-streamer:12345',
    'ws',
    changeDB,
    changeSource,
    ReplicationStatusPublisher.forReplicaFile(replicaDbFile.path, () =>
      Promise.resolve(),
    ),
    subscriptionState,
    // This satisfies the SQLite-only production guard and gives every mode
    // the same ACK policy. The benchmark deliberately does not report backup
    // watermarks: it isolates PG-to-RM replication from Litestream and purge.
    {backupURL: 's3://logical-replication-bench', litestreamVersion: 'v5'},
    null,
    true,
    {
      ...baseStreamerOptions,
      pgChangeLogEnabled: mode.pgChangeLogEnabled,
      ...sqliteOptions,
    },
  );
  const streamerDone = changeStreamer.run();
  cleanup.push(async () => {
    await changeStreamer.stop();
    await streamerDone;
  });

  const worker = new ThreadWriteWorkerClient();
  await worker.init(replicaDbFile.path, 'serving', getPragmaConfig('serving'), {
    level: 'error',
    format: 'text',
  });

  const replicator = new ReplicatorService(
    lc,
    TASK_ID,
    'logical-replication-throughput-replicator',
    'serving',
    parseStringifiedChangeStreamer(changeStreamer),
    worker,
    null,
  );
  const replicatorDone = replicator.run();
  cleanup.push(async () => {
    await replicator.stop();
    await replicatorDone;
  });

  return {upstream, changeDB, replicaPath: replicaDbFile.path};
}

describe.each(changeLogModes)(
  'replicator/logical replication throughput ($name)',
  mode => {
    describe.each(workloads)('$name', workload => {
      test(
        'end-to-end throughput',
        {timeout: TEST_TIMEOUT_MS},
        async ({testDBs}: PgTest) => {
          const {upstream, changeDB, replicaPath} =
            await startReplicationPipeline(testDBs, mode);
          const samples: {elapsedMs: number; operations: number}[] = [];

          expect(replicaRowCount(replicaPath)).toBe(0);

          await insertBenchmarkFixtureRows(
            upstream,
            1,
            workload.warmupRows,
            workload.rowsPerTransaction,
          );
          expect(
            await waitForReplication(
              replicaPath,
              workload.warmupRows,
              changeDB,
              mode,
            ),
          ).toBe(workload.warmupRows);

          for (
            let warmupRep = 0;
            warmupRep < MEASURED_WARMUP_REPS;
            warmupRep++
          ) {
            const startID =
              workload.warmupRows + warmupRep * workload.measuredRows + 1;
            const expectedRows =
              workload.warmupRows + (warmupRep + 1) * workload.measuredRows;
            await insertPrecomputedFixtureRows(upstream, startID, workload);
            expect(
              await waitForReplication(
                replicaPath,
                expectedRows,
                changeDB,
                mode,
              ),
            ).toBe(expectedRows);
          }

          for (let rep = 0; rep < REPS; rep++) {
            const startID =
              workload.warmupRows +
              (MEASURED_WARMUP_REPS + rep) * workload.measuredRows +
              1;
            const expectedRows =
              workload.warmupRows +
              (MEASURED_WARMUP_REPS + rep + 1) * workload.measuredRows;

            const batches = makeBenchmarkFixtureRowBatches(
              startID,
              workload.measuredRows,
              workload.rowsPerTransaction,
            );
            const start = performance.now();
            await insertBenchmarkFixtureRowBatches(upstream, batches);
            expect(
              await waitForReplication(
                replicaPath,
                expectedRows,
                changeDB,
                mode,
              ),
            ).toBe(expectedRows);
            samples.push({
              elapsedMs: performance.now() - start,
              operations: benchmarkFixturePayloadMB(
                startID,
                workload.measuredRows,
              ),
            });
          }

          const dimensions =
            `changeLog=${mode.name} ` +
            `rowsPerTransaction=${workload.rowsPerTransaction}`;
          benchmarkRecorder.recordThroughputSamples(
            `replicator/logical replication end-to-end payload MB ` +
              dimensions,
            samples,
          );
          benchmarkRecorder.recordThroughputSamples(
            `replicator/logical replication end-to-end transactions ` +
              dimensions,
            samples.map(({elapsedMs}) => ({
              elapsedMs,
              operations: workload.measuredRows / workload.rowsPerTransaction,
            })),
          );
        },
      );
    });
  },
);

async function insertPrecomputedFixtureRows(
  upstream: PostgresDB,
  startID: number,
  workload: Workload,
) {
  const batches = makeBenchmarkFixtureRowBatches(
    startID,
    workload.measuredRows,
    workload.rowsPerTransaction,
  );
  await insertBenchmarkFixtureRowBatches(upstream, batches);
}
