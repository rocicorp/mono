// Benchmarks initial sync throughput from a generated PostgreSQL fixture into
// a SQLite replica.

import {afterEach, describe, expect} from 'vitest';
import {createManualBenchmarkRecorder} from '../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {initialSync} from '../services/change-source/pg/initial-sync.ts';
import {getConnectionURI, type PgTest, test} from '../test/db.ts';
import {DbFile} from '../test/lite.ts';
import {
  BENCHMARK_FIXTURE_PUBLICATION,
  benchmarkFixturePayloadMB,
  benchmarkFixtureReplicaRowCount,
  setupBenchmarkFixture,
} from '../test/pg-bench.ts';

const FIXTURE_ROWS = 250_000;
const ROWS_PER_TRANSACTION = 500;
const TABLE_COPY_WORKERS = 4;
const WARMUP_REPS = 1;
const REPS = 10;

const APP_ID = 'initial_sync_bench_app';
const SHARD_NUM = 0;
const TEST_TIMEOUT_MS = 3_600_000;

const lc = createSilentLogContext();
const shard = {
  appID: APP_ID,
  shardNum: SHARD_NUM,
  publications: [BENCHMARK_FIXTURE_PUBLICATION],
};
const benchmarkRecorder = createManualBenchmarkRecorder();

let cleanup: (() => Promise<void> | void)[] = [];

async function runCleanup() {
  for (const fn of cleanup.reverse()) {
    await fn();
  }
  cleanup = [];
}

afterEach(async () => {
  await runCleanup();
});

describe('zero-cache/initial-sync throughput', () => {
  test(
    'generated fixture payload MB/sec',
    {timeout: TEST_TIMEOUT_MS},
    async ({testDBs}: PgTest) => {
      const samples: number[] = [];
      const fixturePayloadMB = benchmarkFixturePayloadMB(1, FIXTURE_ROWS);

      for (let rep = 0; rep < WARMUP_REPS + REPS; rep++) {
        const upstream = await testDBs.create(`initial_sync_bench_${rep}`);
        const replicaDbFile = new DbFile('initial-sync-bench');
        cleanup.push(() => replicaDbFile.delete());
        cleanup.push(async () => {
          await testDBs.drop(upstream);
        });

        await setupBenchmarkFixture(upstream, {
          publication: BENCHMARK_FIXTURE_PUBLICATION,
          rows: FIXTURE_ROWS,
          rowsPerTransaction: ROWS_PER_TRANSACTION,
        });

        const start = performance.now();
        await initReplica(
          lc,
          'initial-sync-bench',
          replicaDbFile.path,
          (log, tx) =>
            initialSync(
              log,
              shard,
              tx,
              getConnectionURI(upstream),
              {tableCopyWorkers: TABLE_COPY_WORKERS},
              {bench: 'initial-sync-throughput'},
            ),
        );
        const elapsed = performance.now() - start;

        expect(benchmarkFixtureReplicaRowCount(lc, replicaDbFile.path)).toBe(
          FIXTURE_ROWS,
        );
        if (rep >= WARMUP_REPS) {
          samples.push(elapsed);
        }
        await runCleanup();
      }

      benchmarkRecorder.recordThroughput(
        'zero-cache/initial-sync generated fixture payload MB',
        samples,
        fixturePayloadMB,
      );
    },
  );
});
