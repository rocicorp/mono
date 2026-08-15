#!/usr/bin/env node
/* oxlint-disable no-console */
import '../../../shared/src/dotenv.ts';

import {parseArgs} from 'node:util';
import type {BenchmarkConfig, SQLiteChangeLogMode} from './config.ts';
import {getDefaultZbugsQueries} from './default-queries.ts';
import {seedZbugsDatabase} from './default-seed.ts';
import {runPipelineBenchmark} from './harness.ts';
import {createZbugsLoadGenerator} from './load-generators.ts';
import {formatBenchmarkReport, saveBenchmarkResults} from './results.ts';

async function main() {
  const {values} = parseArgs({
    options: {
      'num-rms': {type: 'string', default: '1'},
      'num-view-syncers': {type: 'string', default: '1'},
      'clients-per-view-syncer': {type: 'string', default: '5'},
      'write-rate': {type: 'string', default: '50'},
      'load-duration': {type: 'string', default: '20'},
      'drain-timeout': {type: 'string', default: '5'},
      'db-mode': {type: 'string', default: 'docker'},
      'upstream-db': {type: 'string'},
      'cvr-db': {type: 'string'},
      'change-db': {type: 'string'},
      'sqlite-change-log-mode': {type: 'string', default: 'serve'},
      'profile-rm': {type: 'boolean', default: false},
      'profile-vs': {type: 'boolean', default: false},
      'output-dir': {type: 'string', default: './bench-results'},
      'log-level': {type: 'string', default: 'info'},
      'help': {type: 'boolean', short: 'h', default: false},
    },
    allowPositionals: true,
  });

  // Clear process.argv so childWorker does not forward benchmark CLI args to zero-cache processes
  process.argv = process.argv.slice(0, 2);

  if (values.help) {
    console.log(`
Zero-Cache Pipeline Benchmark Runner

Options:
  --num-rms <1|2>                 Number of replication managers (default: 1)
  --num-view-syncers <N>          Number of view syncers (default: 1)
  --clients-per-view-syncer <N>   Simulated clients per view syncer (default: 5)
  --write-rate <N>                Writes per second target (default: 50)
  --load-duration <N>             Load duration in seconds (default: 20)
  --drain-timeout <N>             Drain settling timeout in seconds (default: 5)
  --db-mode <docker|external>     Database mode (default: docker)
  --upstream-db <uri>             Upstream DB connection string
  --cvr-db <uri>                  CVR DB connection string
  --change-db <uri>               Change DB connection string
  --sqlite-change-log-mode <mode> off|write|compare|serve (default: serve)
  --profile-rm                    Collect CPU profile from RM (default: false)
  --profile-vs                    Collect CPU profile from View-Syncers (default: false)
  --output-dir <path>             Results output directory (default: ./bench-results)
  --log-level <level>             Worker log level (default: info)
  -h, --help                      Show this help message
    `);
    process.exit(0);
  }

  const numRMs = Number(values['num-rms']) === 2 ? 2 : 1;
  const numVS = Math.max(1, Number(values['num-view-syncers']));
  const clientsPerVS = Math.max(1, Number(values['clients-per-view-syncer']));
  const writeRate = Number(values['write-rate']);
  const loadDuration = Number(values['load-duration']);
  const drainTimeout = Number(values['drain-timeout']);
  const dbMode = values['db-mode'] === 'external' ? 'external' : 'docker';
  const sqliteMode = (values['sqlite-change-log-mode'] ??
    'serve') as SQLiteChangeLogMode;
  const outputDir = values['output-dir'] ?? './bench-results';
  const logLevel = (values['log-level'] ?? 'info') as
    | 'error'
    | 'warn'
    | 'info'
    | 'debug';

  const config: BenchmarkConfig = {
    numReplicationManagers: numRMs,
    numViewSyncers: numVS,
    clientsPerViewSyncer: clientsPerVS,
    clientQueries: getDefaultZbugsQueries(),
    loadGenerator: createZbugsLoadGenerator(),
    writeRatePerSecond: writeRate,
    loadDurationSeconds: loadDuration,
    drainTimeoutSeconds: drainTimeout,
    seedDatabase: db => seedZbugsDatabase(db, {appID: 'zero', numIssues: 100}),
    dbMode,
    upstreamDB: values['upstream-db'],
    cvrDB: values['cvr-db'],
    changeDB: values['change-db'],
    sqliteChangeLogMode: sqliteMode,
    appID: 'zero',
    profileReplicationManager: values['profile-rm'] ?? false,
    profileViewSyncer: values['profile-vs'] ?? false,
    outputDir,
    logLevel,
  };

  console.log(`Starting Zero-Cache Pipeline Benchmark...`);
  console.log(
    `Topology: ${numRMs} RM(s), ${numVS} View-Syncer(s), ${clientsPerVS} Client(s)/VS`,
  );
  console.log(`Load: ${writeRate} writes/sec for ${loadDuration}s`);

  const result = await runPipelineBenchmark(config);

  const report = formatBenchmarkReport(result);
  console.log('\n' + report);

  const savedPath = await saveBenchmarkResults(result, outputDir);
  console.log(`\nResults saved to: ${savedPath}`);
}

main().catch(err => {
  console.error('Benchmark failed:', err);
  process.exit(1);
});
