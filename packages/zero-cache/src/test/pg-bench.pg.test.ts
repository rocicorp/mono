import {expect} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {initReplica} from '../services/change-source/common/replica-schema.ts';
import {initialSync} from '../services/change-source/pg/initial-sync.ts';
import {getConnectionURI, type PgTest, test} from './db.ts';
import {DbFile} from './lite.ts';
import {
  BENCHMARK_FIXTURE_PUBLICATION,
  type InitialSyncBenchmarkFixture,
  setupInitialSyncBenchmarkFixture,
  validateInitialSyncBenchmarkReplica,
} from './pg-bench.ts';

const lc = createSilentLogContext();

for (const {fixture, table, indexes, id} of [
  {
    fixture: {
      fixture: 'wide-text',
      rows: 3,
      payloadBytes: 683_000,
    },
    table: 'bench_wide_text',
    indexes: 4,
    id: 'wide_text',
  },
  {
    fixture: {
      fixture: 'large-payload',
      rows: 3,
      payloadBytes: 275_000,
    },
    table: 'benchmark.bench_large_payload',
    indexes: 6,
    id: 'large_payload',
  },
] as const satisfies readonly {
  fixture: InitialSyncBenchmarkFixture;
  table: string;
  indexes: number;
  id: string;
}[]) {
  test(
    `validates a TOAST-sized ${fixture.fixture} benchmark replica`,
    {timeout: 60_000},
    async ({testDBs}: PgTest) => {
      const upstream = await testDBs.create(`${id}_bench_fixture_validation`);
      const replicaFile = new DbFile(`${fixture.fixture}-bench-fixture`);
      const shard = {
        appID: `${id}_bench_fixture_validation`,
        shardNum: 0,
        publications: [BENCHMARK_FIXTURE_PUBLICATION],
      };

      try {
        await setupInitialSyncBenchmarkFixture(upstream, fixture);
        await initReplica(
          lc,
          `${fixture.fixture}-bench-fixture`,
          replicaFile.path,
          (log, tx) =>
            initialSync(
              log,
              shard,
              tx,
              getConnectionURI(upstream),
              {tableCopyWorkers: 2},
              {test: 'pg-bench-fixture-validation'},
            ),
        );

        expect(
          validateInitialSyncBenchmarkReplica(lc, replicaFile.path, fixture),
        ).toMatchObject({
          rowCounts: {[table]: 3},
          totalRows: 3,
          indexes: {length: indexes},
          samplesValidated: 3,
        });
      } finally {
        replicaFile.delete();
        await testDBs.drop(upstream);
      }
    },
  );
}
