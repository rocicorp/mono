// Benchmarks end-to-end logical replication throughput from upstream Postgres
// writes through the change-streamer and into the SQLite replica.
//
// Set SQLITE_CHANGE_LOG_WRITER=1 to also write the canonical change stream to
// the change-log database beside the replica, which is what the replicator
// does with `sqliteChangeLogMode` enabled. Running the bench both ways is the
// slice 7D measurement: the cost of the second, log-first commit on the real
// write path rather than on the synthetic ceiling harness.
//
//   pnpm --filter zero-cache run bench:pg replication-throughput
//   SQLITE_CHANGE_LOG_WRITER=1 pnpm --filter zero-cache run bench:pg replication-throughput

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {BigIntJSON} from '../../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../../shared/src/sleep.ts';
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
import {initializeStreamer} from '../change-streamer/change-streamer-service.ts';
import {
  type ChangeStreamer,
  type ChangeStreamerService,
  type Downstream,
  type SerializedDownstream,
} from '../change-streamer/change-streamer.ts';
import {initChangeStreamerSchema} from '../change-streamer/schema/init.ts';
import {deleteChangeLogDB} from './change-log-db.ts';
import {ReplicationStatusPublisher} from './replication-status.ts';
import {ReplicatorService} from './replicator.ts';
import {ThreadWriteWorkerClient} from './write-worker-client.ts';

const WARMUP_ROWS = 25_000;
const MEASURED_ROWS = 100_000;
const ROWS_PER_TRANSACTION = 500;
const CHANGE_LOG_BATCH_SIZE = 2000;
const MEASURED_WARMUP_REPS = 1;
const REPS = 10;

const APP_ID = 'logical_replication_bench_app';
const SHARD_NUM = 0;
const TASK_ID = 'logical-replication-throughput-bench';
const TEST_TIMEOUT_MS = 900_000;

const LOGS_CHANGE_STREAM = process.env['SQLITE_CHANGE_LOG_WRITER'] === '1';

const lc = createSilentLogContext();
const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [BENCHMARK_FIXTURE_PUBLICATION],
};
const benchmarkRecorder = createManualBenchmarkRecorder();
const streamerOptions = {
  backPressureLimitHeapProportion: 0.04,
  flowControlConsensusPaddingSeconds: 1,
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

function parseStringifiedSource(
  source: Source<string>,
): Source<SerializedDownstream> {
  return {
    cancel: err => source.cancel(err),
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

async function startReplicationPipeline(testDBs: PgTest['testDBs']) {
  const upstream = await testDBs.create('logical_replication_bench_upstream');
  const changeDB = await testDBs.create('logical_replication_bench_change', {
    typeOpts: {sendStringAsJson: true},
  });
  const replicaDbFile = new DbFile('logical-replication-throughput-bench');

  // oxlint-disable require-await
  cleanup.push(async () => replicaDbFile.delete());
  cleanup.push(async () => {
    await testDBs.drop(upstream, changeDB);
  });

  await setupBenchmarkFixture(upstream, {
    publication: BENCHMARK_FIXTURE_PUBLICATION,
  });
  const upstreamURI = getConnectionURI(upstream);
  const {subscriptionState, changeSource} =
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
    null,
    true,
    streamerOptions,
  );
  const streamerDone = changeStreamer.run();
  cleanup.push(async () => {
    await changeStreamer.stop();
    await streamerDone;
  });

  const worker = new ThreadWriteWorkerClient();
  await worker.init(
    replicaDbFile.path,
    'serving',
    LOGS_CHANGE_STREAM,
    getPragmaConfig('serving'),
    {
      level: 'error',
      format: 'text',
    },
  );
  cleanup.push(() => {
    deleteChangeLogDB(replicaDbFile.path);
    return Promise.resolve();
  });

  const replicator = new ReplicatorService(
    lc,
    TASK_ID,
    'logical-replication-throughput-replicator',
    'serving',
    parseStringifiedChangeStreamer(changeStreamer),
    worker,
    LOGS_CHANGE_STREAM,
    null,
    undefined,
  );
  const replicatorDone = replicator.run();
  cleanup.push(async () => {
    await replicator.stop();
    await replicatorDone;
  });

  return {upstream, replicaPath: replicaDbFile.path};
}

describe('replicator/logical replication throughput', () => {
  test(
    'end-to-end payload MB/sec',
    {timeout: TEST_TIMEOUT_MS},
    async ({testDBs}: PgTest) => {
      const {upstream, replicaPath} = await startReplicationPipeline(testDBs);
      const samples: {elapsedMs: number; operations: number}[] = [];

      expect(replicaRowCount(replicaPath)).toBe(0);

      await insertBenchmarkFixtureRows(
        upstream,
        1,
        WARMUP_ROWS,
        ROWS_PER_TRANSACTION,
      );
      expect(await waitForReplicaRows(replicaPath, WARMUP_ROWS)).toBe(
        WARMUP_ROWS,
      );

      for (let warmupRep = 0; warmupRep < MEASURED_WARMUP_REPS; warmupRep++) {
        const startID = WARMUP_ROWS + warmupRep * MEASURED_ROWS + 1;
        const expectedRows = WARMUP_ROWS + (warmupRep + 1) * MEASURED_ROWS;
        await insertPrecomputedFixtureRows(upstream, startID, MEASURED_ROWS);
        expect(await waitForReplicaRows(replicaPath, expectedRows)).toBe(
          expectedRows,
        );
      }

      for (let rep = 0; rep < REPS; rep++) {
        const startID =
          WARMUP_ROWS + (MEASURED_WARMUP_REPS + rep) * MEASURED_ROWS + 1;
        const expectedRows =
          WARMUP_ROWS + (MEASURED_WARMUP_REPS + rep + 1) * MEASURED_ROWS;

        const batches = makeBenchmarkFixtureRowBatches(
          startID,
          MEASURED_ROWS,
          ROWS_PER_TRANSACTION,
        );
        const start = performance.now();
        await insertBenchmarkFixtureRowBatches(upstream, batches);
        expect(await waitForReplicaRows(replicaPath, expectedRows)).toBe(
          expectedRows,
        );
        samples.push({
          elapsedMs: performance.now() - start,
          operations: benchmarkFixturePayloadMB(startID, MEASURED_ROWS),
        });
      }

      benchmarkRecorder.recordThroughputSamples(
        'replicator/logical replication end-to-end payload MB ' +
          `changeLogWriter=${LOGS_CHANGE_STREAM ? 'on' : 'off'}`,
        samples,
      );
    },
  );
});

async function insertPrecomputedFixtureRows(
  upstream: PostgresDB,
  startID: number,
  count: number,
) {
  const batches = makeBenchmarkFixtureRowBatches(
    startID,
    count,
    ROWS_PER_TRANSACTION,
  );
  await insertBenchmarkFixtureRowBatches(upstream, batches);
}
