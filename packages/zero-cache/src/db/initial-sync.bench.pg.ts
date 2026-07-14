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
  type InitialSyncBenchmarkFixture,
  initialSyncBenchmarkPayloadMB,
  setupInitialSyncBenchmarkFixture,
  validateInitialSyncBenchmarkReplica,
} from '../test/pg-bench.ts';

const PROFILE_ENV = 'ZERO_INITIAL_SYNC_BENCH_PROFILE';
type Profile = {
  fixture: InitialSyncBenchmarkFixture;
  warmupReps: number;
  reps: number;
  tableCopyWorkers: number;
  timeoutMs: number;
};

const PROFILES = {
  'mixed-regression': {
    fixture: {fixture: 'mixed', rows: 250_000},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 3_600_000,
  },
  'wide-text-scaled': {
    fixture: {fixture: 'wide-text', rows: 1_000, payloadBytes: 683_000},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 3_600_000,
  },
  'wide-text-full': {
    fixture: {fixture: 'wide-text', rows: 10_000, payloadBytes: 683_000},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 7_200_000,
  },
  'wide-text-narrow': {
    fixture: {fixture: 'wide-text', rows: 250_000, payloadBytes: 128},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 3_600_000,
  },
  'large-payload-scaled': {
    fixture: {fixture: 'large-payload', rows: 2_000, payloadBytes: 275_000},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 3_600_000,
  },
  'large-payload-full': {
    fixture: {fixture: 'large-payload', rows: 10_000, payloadBytes: 275_000},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 7_200_000,
  },
  'large-payload-narrow': {
    fixture: {fixture: 'large-payload', rows: 250_000, payloadBytes: 128},
    warmupReps: 1,
    reps: 10,
    tableCopyWorkers: 4,
    timeoutMs: 3_600_000,
  },
} as const satisfies Record<string, Profile>;

const profileName = process.env[PROFILE_ENV] ?? 'mixed-regression';
if (!Object.hasOwn(PROFILES, profileName)) {
  throw new Error(
    `${PROFILE_ENV} must be one of ${Object.keys(PROFILES).join(', ')}; got ${JSON.stringify(profileName)}`,
  );
}
const profile = PROFILES[profileName as keyof typeof PROFILES];

const APP_ID = 'initial_sync_bench_app';
const SHARD_NUM = 0;
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
    `${profileName} generated fixture payload MB/sec`,
    {timeout: profile.timeoutMs},
    async ({testDBs}: PgTest) => {
      const samples: number[] = [];
      const fixturePayloadMB = initialSyncBenchmarkPayloadMB(profile.fixture);

      for (let rep = 0; rep < profile.warmupReps + profile.reps; rep++) {
        const upstream = await testDBs.create(
          `initial_sync_bench_${profileName.replaceAll('-', '_')}_${rep}`,
        );
        const replicaDbFile = new DbFile('initial-sync-bench');
        cleanup.push(() => replicaDbFile.delete());
        cleanup.push(async () => {
          await testDBs.drop(upstream);
        });

        await setupInitialSyncBenchmarkFixture(
          upstream,
          profile.fixture,
          BENCHMARK_FIXTURE_PUBLICATION,
        );

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
              {tableCopyWorkers: profile.tableCopyWorkers},
              {bench: 'initial-sync-throughput', profile: profileName},
            ),
        );
        const elapsed = performance.now() - start;

        const validation = validateInitialSyncBenchmarkReplica(
          lc,
          replicaDbFile.path,
          profile.fixture,
        );
        expect(validation.totalRows).toBe(profile.fixture.rows);
        if (rep >= profile.warmupReps) {
          samples.push(elapsed);
        }
        await runCleanup();
      }

      benchmarkRecorder.recordThroughput(
        `zero-cache/initial-sync ${profileName} generated fixture payload MB`,
        samples,
        fixturePayloadMB,
      );
    },
  );
});
