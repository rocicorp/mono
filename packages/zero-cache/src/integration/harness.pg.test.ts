import {describe, expect} from 'vitest';
import type {BenchmarkConfig} from '../../bench/pipeline/config.ts';
import {getDefaultZbugsQueries} from '../../bench/pipeline/default-queries.ts';
import {seedZbugsDatabase} from '../../bench/pipeline/default-seed.ts';
import {runPipelineBenchmark} from '../../bench/pipeline/harness.ts';
import {createZbugsLoadGenerator} from '../../bench/pipeline/load-generators.ts';
import {getConnectionURI, test} from '../test/db.ts';

describe('pipeline benchmark harness', () => {
  test('runs multi-node benchmark end-to-end with 1 RM and 1 VS', async ({
    testDBs,
  }) => {
    const upDB = await testDBs.create('bench_test_up');
    const cvrDB = await testDBs.create('bench_test_cvr');
    const changeDB = await testDBs.create('bench_test_chg');

    const config: BenchmarkConfig = {
      numReplicationManagers: 1,
      numViewSyncers: 1,
      clientsPerViewSyncer: 2,
      clientQueries: getDefaultZbugsQueries(),
      loadGenerator: createZbugsLoadGenerator(),
      writeRatePerSecond: 10,
      loadDurationSeconds: 3,
      drainTimeoutSeconds: 2,
      seedDatabase: db =>
        seedZbugsDatabase(db, {
          appID: 'bench_app',
          numIssues: 20,
          numUsers: 3,
          numProjects: 2,
        }),
      dbMode: 'external',
      upstreamDB: getConnectionURI(upDB),
      cvrDB: getConnectionURI(cvrDB),
      changeDB: getConnectionURI(changeDB),
      sqliteChangeLogMode: 'serve',
      appID: 'bench_app',
      profileReplicationManager: false,
      profileViewSyncer: false,
      outputDir: '/tmp/bench-test-results',
      logLevel: 'error',
    };

    const result = await runPipelineBenchmark(config);

    expect(result.topology.numReplicationManagers).toBe(1);
    expect(result.topology.numViewSyncers).toBe(1);
    expect(result.topology.totalClients).toBe(2);

    expect(result.loadStats.writesAttempted).toBeGreaterThan(0);
    expect(result.loadStats.writesSucceeded).toBeGreaterThan(0);

    expect(result.clientStats).toHaveLength(2);
    for (const client of result.clientStats) {
      expect(client.initialHydrationDurationMs).not.toBeNull();
      expect(client.initialHydrationDurationMs).toBeGreaterThanOrEqual(0);
    }
  }, 60_000);
});
