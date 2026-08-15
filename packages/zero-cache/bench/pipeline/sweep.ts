#!/usr/bin/env node
/* oxlint-disable no-console */
import '../../../shared/src/dotenv.ts';

import {mkdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {parseArgs} from 'node:util';
import type {BenchmarkConfig, SQLiteChangeLogMode} from './config.ts';
import {getDefaultZbugsQueries} from './default-queries.ts';
import {seedZbugsDatabase} from './default-seed.ts';
import {runPipelineBenchmark} from './harness.ts';
import {createZbugsLoadGenerator} from './load-generators.ts';
import type {BenchmarkResult} from './results.ts';

export interface SweepResult {
  readonly numViewSyncers: number;
  readonly result: BenchmarkResult;
}

export interface SweepSummary {
  readonly timestamp: string;
  readonly writeRatePerSecond: number;
  readonly loadDurationSeconds: number;
  readonly clientsPerViewSyncer: number;
  readonly runs: readonly SweepResult[];
}

export function formatSweepTable(runs: readonly SweepResult[]): string {
  const lines: string[] = [
    '========================================================================================================================',
    '                                  VIEW-SYNCER SCALE SWEEP RESULTS (2 -> 10 NODES)                                      ',
    '========================================================================================================================',
    ' VS Count | Clients | Writes/s | Pokes Rx | Repl Lag (avg/p50/p95) | IVM Advance (avg/p50/p95) | E2E Lag (avg/p50/p95) | Hydration ',
    '----------+---------+----------+----------+------------------------+---------------------------+-----------------------+-----------',
  ];

  for (const {numViewSyncers, result} of runs) {
    const clients = result.topology.totalClients;
    const writeRate = result.loadStats.actualRate.toFixed(1);
    const totalPokes = result.clientStats.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );

    const repl = result.metrics.replicationLagMs;
    const replStr = repl
      ? `${repl.avg.toFixed(1)}/${repl.p50.toFixed(1)}/${repl.p95.toFixed(1)}ms`
      : 'N/A';

    const ivm = result.metrics.advancementLatencyMs;
    const ivmStr = ivm
      ? `${ivm.avg.toFixed(1)}/${ivm.p50.toFixed(1)}/${ivm.p95.toFixed(1)}ms`
      : 'N/A';

    const e2e = result.metrics.e2eServingLagMs;
    const e2eStr = e2e
      ? `${e2e.avg.toFixed(1)}/${e2e.p50.toFixed(1)}/${e2e.p95.toFixed(1)}ms`
      : 'N/A';

    const hydDurations = result.clientStats
      .map(c => c.initialHydrationDurationMs)
      .filter((d): d is number => d !== null);
    const hydAvg =
      hydDurations.length > 0
        ? hydDurations.reduce((a, b) => a + b, 0) / hydDurations.length
        : 0;
    const hydStr = `${hydAvg.toFixed(1)}ms`;

    lines.push(
      ` ${String(numViewSyncers).padEnd(8)} | ` +
        `${String(clients).padEnd(7)} | ` +
        `${writeRate.padEnd(8)} | ` +
        `${String(totalPokes).padEnd(8)} | ` +
        `${replStr.padEnd(22)} | ` +
        `${ivmStr.padEnd(25)} | ` +
        `${e2eStr.padEnd(21)} | ` +
        `${hydStr.padEnd(9)}`,
    );
  }

  lines.push(
    '========================================================================================================================',
  );

  return lines.join('\n');
}

async function main() {
  const {values} = parseArgs({
    options: {
      'vs-counts': {type: 'string', default: '2,4,6,8,10'},
      'clients-per-vs': {type: 'string', default: '3'},
      'write-rate': {type: 'string', default: '30'},
      'load-duration': {type: 'string', default: '8'},
      'drain-timeout': {type: 'string', default: '3'},
      'num-rms': {type: 'string', default: '1'},
      'db-mode': {type: 'string', default: 'docker'},
      'upstream-db': {type: 'string'},
      'cvr-db': {type: 'string'},
      'change-db': {type: 'string'},
      'sqlite-change-log-mode': {type: 'string', default: 'serve'},
      'output-dir': {type: 'string', default: './bench-results/sweep'},
      'log-level': {type: 'string', default: 'error'},
      'help': {type: 'boolean', short: 'h', default: false},
    },
    allowPositionals: true,
  });

  // Clear process.argv so childWorker does not forward benchmark CLI args
  process.argv = process.argv.slice(0, 2);

  if (values.help) {
    console.log(`
Zero-Cache Pipeline View-Syncer Scale Sweep Runner

Options:
  --vs-counts <list>        Comma-separated list of VS counts (default: 2,4,6,8,10)
  --clients-per-vs <N>      Clients per view syncer (default: 3)
  --write-rate <N>          Target write rate per sec (default: 30)
  --load-duration <N>       Load duration in seconds per run (default: 8)
  --drain-timeout <N>       Drain settling timeout in seconds (default: 3)
  --num-rms <1|2>           Number of replication managers (default: 1)
  --output-dir <path>       Sweep results output directory (default: ./bench-results/sweep)
  --log-level <level>       Worker log level (default: error)
  -h, --help                Show help
    `);
    process.exit(0);
  }

  const vsCounts = (values['vs-counts'] ?? '2,4,6,8,10')
    .split(',')
    .map(s => Number(s.trim()))
    .filter(n => Number.isInteger(n) && n > 0);

  const clientsPerVS = Math.max(1, Number(values['clients-per-vs'] ?? '3'));
  const writeRate = Number(values['write-rate'] ?? '30');
  const loadDuration = Number(values['load-duration'] ?? '8');
  const drainTimeout = Number(values['drain-timeout'] ?? '3');
  const numRMs = Number(values['num-rms']) === 2 ? 2 : 1;
  const dbMode = values['db-mode'] === 'external' ? 'external' : 'docker';
  const sqliteMode = (values['sqlite-change-log-mode'] ??
    'serve') as SQLiteChangeLogMode;
  const outputDir = values['output-dir'] ?? './bench-results/sweep';
  const logLevel = (values['log-level'] ?? 'error') as
    | 'error'
    | 'warn'
    | 'info'
    | 'debug';

  await mkdir(outputDir, {recursive: true});

  console.log('====================================================');
  console.log('Starting View-Syncer Scaling Sweep');
  console.log(`Sweep Targets: ${vsCounts.join(', ')} View-Syncers`);
  console.log(`Clients Per VS: ${clientsPerVS}`);
  console.log(
    `Write Rate: ${writeRate} writes/sec for ${loadDuration}s per run`,
  );
  console.log('====================================================\n');

  const runs: SweepResult[] = [];

  for (let i = 0; i < vsCounts.length; i++) {
    const vsCount = vsCounts[i];
    const totalClients = vsCount * clientsPerVS;

    console.log(
      `\n[Run ${i + 1}/${vsCounts.length}] Testing with ${vsCount} View-Syncers (${totalClients} Total Clients)...`,
    );

    const config: BenchmarkConfig = {
      numReplicationManagers: numRMs,
      numViewSyncers: vsCount,
      clientsPerViewSyncer: clientsPerVS,
      clientQueries: getDefaultZbugsQueries(),
      loadGenerator: createZbugsLoadGenerator(),
      writeRatePerSecond: writeRate,
      loadDurationSeconds: loadDuration,
      drainTimeoutSeconds: drainTimeout,
      seedDatabase: db =>
        seedZbugsDatabase(db, {appID: `sweep_${vsCount}`, numIssues: 100}),
      dbMode,
      upstreamDB: values['upstream-db'],
      cvrDB: values['cvr-db'],
      changeDB: values['change-db'],
      sqliteChangeLogMode: sqliteMode,
      appID: `sweep_${vsCount}`,
      profileReplicationManager: false,
      profileViewSyncer: false,
      outputDir,
      logLevel,
    };

    const startTime = Date.now();
    const result = await runPipelineBenchmark(config);
    const elapsedSec = ((Date.now() - startTime) / 1000).toFixed(1);

    const totalPokes = result.clientStats.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );
    const replAvg = result.metrics.replicationLagMs?.avg.toFixed(1) ?? 'N/A';
    const ivmAvg = result.metrics.advancementLatencyMs?.avg.toFixed(1) ?? 'N/A';
    const e2eAvg = result.metrics.e2eServingLagMs?.avg.toFixed(1) ?? 'N/A';

    console.log(
      `✓ Completed ${vsCount} VS in ${elapsedSec}s: ` +
        `Pokes=${totalPokes} | ReplLag=${replAvg}ms | IVMLatency=${ivmAvg}ms | E2ELag=${e2eAvg}ms`,
    );

    runs.push({numViewSyncers: vsCount, result});

    const runFile = path.join(outputDir, `sweep_vs_${vsCount}.json`);
    await writeFile(runFile, JSON.stringify(result, null, 2), 'utf-8');
  }

  const tableOutput = formatSweepTable(runs);
  console.log('\n' + tableOutput);

  const summary: SweepSummary = {
    timestamp: new Date().toISOString(),
    writeRatePerSecond: writeRate,
    loadDurationSeconds: loadDuration,
    clientsPerViewSyncer: clientsPerVS,
    runs,
  };

  const summaryPath = path.join(outputDir, 'sweep_summary.json');
  await writeFile(summaryPath, JSON.stringify(summary, null, 2), 'utf-8');
  console.log(`\nFull sweep summary saved to: ${summaryPath}`);
}

main().catch(err => {
  console.error('Sweep execution failed:', err);
  process.exit(1);
});
