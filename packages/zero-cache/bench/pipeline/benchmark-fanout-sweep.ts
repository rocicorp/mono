#!/usr/bin/env node
/* oxlint-disable no-console */
import '../../../shared/src/dotenv.ts';

import {mkdir, writeFile} from 'node:fs/promises';
import path from 'node:path';
import {parseArgs} from 'node:util';
import type {BenchmarkConfig} from './config.ts';
import {getDefaultZbugsQueries} from './default-queries.ts';
import {seedZbugsDatabase} from './default-seed.ts';
import {runPipelineBenchmark} from './harness.ts';
import {createZbugsLoadGenerator} from './load-generators.ts';
import type {BenchmarkResult} from './results.ts';

export interface FanoutRunResult {
  readonly clientCount: number;
  readonly modeName: string;
  readonly result: BenchmarkResult;
}

export function formatFanoutTable(runs: readonly FanoutRunResult[]): string {
  const lines: string[] = [
    '========================================================================================================================',
    '                        CLIENT FANOUT SCALING SWEEP (1 View-Syncer @ 200 writes/sec)                                    ',
    '========================================================================================================================',
    ' Clients | Mode                | Total Pokes | Pokes/Client | IVM Adv (p50/p95) | E2E Lag (avg/p50/p95)    | Status     ',
    '---------+---------------------+-------------+--------------+-------------------+--------------------------+------------',
  ];

  for (const {clientCount, modeName, result} of runs) {
    const totalPokes = result.clientStats.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );
    const pokesPerClient = (totalPokes / Math.max(1, clientCount)).toFixed(1);

    const ivm = result.metrics.advancementLatencyMs;
    const ivmStr = ivm
      ? `${ivm.p50.toFixed(1)} / ${ivm.p95.toFixed(1)}ms`
      : 'N/A';

    const e2e = result.metrics.e2eServingLagMs;
    const e2eStr = e2e
      ? `${e2e.avg.toFixed(1)} / ${e2e.p50.toFixed(1)} / ${e2e.p95.toFixed(1)}ms`
      : 'N/A';

    const avgLag = e2e?.avg ?? 0;
    const status =
      avgLag > 2000 ? 'COLLAPSED' : avgLag > 500 ? 'DEGRADED' : 'HEALTHY';

    lines.push(
      ` ${String(clientCount).padEnd(7)} | ` +
        `${modeName.padEnd(19)} | ` +
        `${String(totalPokes).padEnd(11)} | ` +
        `${pokesPerClient.padEnd(12)} | ` +
        `${ivmStr.padEnd(17)} | ` +
        `${e2eStr.padEnd(24)} | ` +
        `${status.padEnd(10)}`,
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
      'clients': {type: 'string', default: '5,20,50,100,200'},
      'write-rate': {type: 'string', default: '200'},
      'load-duration': {type: 'string', default: '6'},
      'drain-timeout': {type: 'string', default: '3'},
      'output-dir': {type: 'string', default: './bench-results/fanout-sweep'},
      'log-level': {type: 'string', default: 'info'},
      'help': {type: 'boolean', short: 'h', default: false},
    },
    allowPositionals: true,
  });

  process.argv = process.argv.slice(0, 2);

  if (values.help) {
    console.log(`
Client Fanout Sweep Benchmark Runner

Options:
  --clients <counts>     Comma-separated client counts (default: 5,20,50,100,200)
  --write-rate <N>       Write rate in transactions/sec (default: 200)
  --load-duration <N>    Load duration in seconds per test (default: 6)
  --drain-timeout <N>    Drain timeout in seconds (default: 3)
  --output-dir <path>    Output directory
  -h, --help             Show this help message
    `);
    process.exit(0);
  }

  const clientCounts = (values['clients'] ?? '5,20,50,100,200')
    .split(',')
    .map(s => Number(s.trim()))
    .filter(n => Number.isFinite(n) && n > 0);

  const writeRate = Number(values['write-rate']);
  const loadDuration = Number(values['load-duration']);
  const drainTimeout = Number(values['drain-timeout']);
  const outputDir = values['output-dir'] ?? './bench-results/fanout-sweep';
  const logLevel = (values['log-level'] ?? 'info') as
    | 'error'
    | 'warn'
    | 'info'
    | 'debug';

  console.log(
    `=================================================================`,
  );
  console.log(`Client Fanout Scaling Sweep (1 VS @ ${writeRate} writes/sec)`);
  console.log(`Client Counts to test: ${clientCounts.join(', ')} clients`);
  console.log(
    `=================================================================\n`,
  );

  const runs: FanoutRunResult[] = [];

  for (const clientCount of clientCounts) {
    for (const mode of [
      'Baseline (Uncapped)',
      '20 FPS (50ms)',
      'Adaptive (Dynamic)',
    ]) {
      const isAdaptive = mode.startsWith('Adaptive');
      const minAdvanceIntervalMs = mode.startsWith('20 FPS') ? 50 : 0;
      const modeKey = isAdaptive
        ? 'adaptive'
        : minAdvanceIntervalMs > 0
          ? '20fps'
          : 'baseline';

      console.log(`---> Testing ${mode} with ${clientCount} clients...`);

      const config: BenchmarkConfig = {
        numReplicationManagers: 1,
        numViewSyncers: 1,
        clientsPerViewSyncer: clientCount,
        clientQueries: getDefaultZbugsQueries(),
        loadGenerator: createZbugsLoadGenerator(),
        writeRatePerSecond: writeRate,
        loadDurationSeconds: loadDuration,
        drainTimeoutSeconds: drainTimeout,
        seedDatabase: db =>
          seedZbugsDatabase(db, {appID: 'zero', numIssues: 100}),
        dbMode: 'docker',
        sqliteChangeLogMode: 'serve',
        appID: 'zero',
        profileReplicationManager: false,
        profileViewSyncer: false,
        outputDir: path.join(outputDir, `clients-${clientCount}-${modeKey}`),
        logLevel,
        minAdvanceIntervalMs,
        adaptiveFrameRate: isAdaptive,
      };

      try {
        const result = await runPipelineBenchmark(config);
        runs.push({
          clientCount,
          modeName: mode,
          result,
        });
        const pokes = result.clientStats.reduce(
          (s, c) => s + c.pokesReceived,
          0,
        );
        console.log(
          `     [DONE] ${mode} with ${clientCount} clients -> Total Pokes: ${pokes}, Avg E2E Lag: ${result.metrics.e2eServingLagMs?.avg.toFixed(1) ?? 'N/A'}ms\n`,
        );
      } catch (err) {
        console.error(
          `     [ERROR] ${mode} with ${clientCount} clients failed:`,
          err,
        );
      }
    }
  }

  const report = formatFanoutTable(runs);
  console.log('\n' + report);

  await mkdir(outputDir, {recursive: true});
  await writeFile(path.join(outputDir, 'fanout-summary.txt'), report, 'utf-8');
  await writeFile(
    path.join(outputDir, 'fanout-summary.json'),
    JSON.stringify(runs, null, 2),
    'utf-8',
  );

  console.log(`\nSummary saved to: ${outputDir}/fanout-summary.txt`);
}

void main();
