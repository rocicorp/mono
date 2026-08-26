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

export interface FrameRateRunResult {
  readonly modeName: string;
  readonly minAdvanceIntervalMs: number;
  readonly adaptiveFrameRate: boolean;
  readonly result: BenchmarkResult;
}

export function formatFrameRateTable(
  runs: readonly FrameRateRunResult[],
): string {
  const lines: string[] = [
    '========================================================================================================================',
    '                         ADAPTIVE FRAME-RATE & POKE COALESCING BENCHMARK RESULTS                                        ',
    '========================================================================================================================',
    ' Configuration     | Target FPS | Pokes Rx | Repl Lag (p50/p95) | IVM Advance (p50/p95) | E2E Serving Lag (avg/p50/p95) ',
    '-------------------+------------+----------+--------------------+-----------------------+-------------------------------',
  ];

  for (const {
    modeName,
    minAdvanceIntervalMs,
    adaptiveFrameRate,
    result,
  } of runs) {
    const fpsLabel = adaptiveFrameRate
      ? 'Adaptive'
      : minAdvanceIntervalMs === 0
        ? 'Uncapped'
        : `${Math.round(1000 / minAdvanceIntervalMs)} FPS`;

    const totalPokes = result.clientStats.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );

    const repl = result.metrics.replicationLagMs;
    const replStr = repl
      ? `${repl.p50.toFixed(1)} / ${repl.p95.toFixed(1)}ms`
      : 'N/A';

    const ivm = result.metrics.advancementLatencyMs;
    const ivmStr = ivm
      ? `${ivm.p50.toFixed(1)} / ${ivm.p95.toFixed(1)}ms`
      : 'N/A';

    const e2e = result.metrics.e2eServingLagMs;
    const e2eStr = e2e
      ? `${e2e.avg.toFixed(1)} / ${e2e.p50.toFixed(1)} / ${e2e.p95.toFixed(1)}ms`
      : 'N/A';

    lines.push(
      ` ${modeName.padEnd(17)} | ` +
        `${fpsLabel.padEnd(10)} | ` +
        `${String(totalPokes).padEnd(8)} | ` +
        `${replStr.padEnd(18)} | ` +
        `${ivmStr.padEnd(21)} | ` +
        `${e2eStr.padEnd(29)}`,
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
      'clients-per-vs': {type: 'string', default: '5'},
      'write-rate': {type: 'string', default: '100'},
      'load-duration': {type: 'string', default: '10'},
      'drain-timeout': {type: 'string', default: '3'},
      'db-mode': {type: 'string', default: 'docker'},
      'upstream-db': {type: 'string'},
      'cvr-db': {type: 'string'},
      'change-db': {type: 'string'},
      'output-dir': {type: 'string', default: './bench-results/frame-rate'},
      'log-level': {type: 'string', default: 'info'},
      'help': {type: 'boolean', short: 'h', default: false},
    },
    allowPositionals: true,
  });

  process.argv = process.argv.slice(0, 2);

  if (values.help) {
    console.log(`
Adaptive Frame-Rate Benchmark Runner

Options:
  --clients-per-vs <N>   Simulated clients per view syncer (default: 5)
  --write-rate <N>       Writes per second target (default: 100)
  --load-duration <N>    Load duration in seconds per run (default: 10)
  --drain-timeout <N>    Drain timeout in seconds (default: 3)
  --output-dir <path>    Results output directory
  -h, --help             Show this help message
    `);
    process.exit(0);
  }

  const clientsPerVS = Math.max(1, Number(values['clients-per-vs']));
  const writeRate = Number(values['write-rate']);
  const loadDuration = Number(values['load-duration']);
  const drainTimeout = Number(values['drain-timeout']);
  const outputDir = values['output-dir'] ?? './bench-results/frame-rate';
  const logLevel = (values['log-level'] ?? 'info') as
    | 'error'
    | 'warn'
    | 'info'
    | 'debug';

  const testConfigs = [
    {
      name: 'Baseline (Uncapped)',
      minAdvanceIntervalMs: 0,
      adaptiveFrameRate: false,
    },
    {name: '20 FPS (50ms)', minAdvanceIntervalMs: 50, adaptiveFrameRate: false},
    {
      name: '10 FPS (100ms)',
      minAdvanceIntervalMs: 100,
      adaptiveFrameRate: false,
    },
    {
      name: 'Adaptive (Dynamic)',
      minAdvanceIntervalMs: 0,
      adaptiveFrameRate: true,
    },
  ];

  console.log(
    `=================================================================`,
  );
  console.log(`Starting Adaptive Frame-Rate Comparison Benchmark`);
  console.log(`Topology: 1 RM, 1 View-Syncer, ${clientsPerVS} Clients`);
  console.log(`Load: ${writeRate} writes/sec for ${loadDuration}s per run`);
  console.log(
    `=================================================================\n`,
  );

  const runs: FrameRateRunResult[] = [];

  for (let i = 0; i < testConfigs.length; i++) {
    const test = testConfigs[i];
    console.log(
      `[Run ${i + 1}/${testConfigs.length}] Testing: ${test.name}...`,
    );

    const config: BenchmarkConfig = {
      numReplicationManagers: 1,
      numViewSyncers: 1,
      clientsPerViewSyncer: clientsPerVS,
      clientQueries: getDefaultZbugsQueries(),
      loadGenerator: createZbugsLoadGenerator(),
      writeRatePerSecond: writeRate,
      loadDurationSeconds: loadDuration,
      drainTimeoutSeconds: drainTimeout,
      seedDatabase: db =>
        seedZbugsDatabase(db, {appID: 'zero', numIssues: 100}),
      dbMode: (values['db-mode'] ?? 'docker') as 'docker' | 'external',
      upstreamDB: values['upstream-db'],
      cvrDB: values['cvr-db'],
      changeDB: values['change-db'],
      sqliteChangeLogMode: 'serve',
      appID: 'zero',
      profileReplicationManager: false,
      profileViewSyncer: false,
      outputDir: path.join(
        outputDir,
        `run-${i}-${test.name.toLowerCase().replace(/[^a-z0-9]/g, '-')}`,
      ),
      logLevel,
      minAdvanceIntervalMs: test.minAdvanceIntervalMs,
      adaptiveFrameRate: test.adaptiveFrameRate,
    };

    const result = await runPipelineBenchmark(config);
    runs.push({
      modeName: test.name,
      minAdvanceIntervalMs: test.minAdvanceIntervalMs,
      adaptiveFrameRate: test.adaptiveFrameRate,
      result,
    });

    console.log(
      `   -> Completed ${test.name}. Total Pokes: ${result.clientStats.reduce((s, c) => s + c.pokesReceived, 0)}, Avg E2E Lag: ${result.metrics.e2eServingLagMs?.avg.toFixed(1) ?? 'N/A'}ms\n`,
    );
  }

  const report = formatFrameRateTable(runs);
  console.log('\n' + report);

  await mkdir(outputDir, {recursive: true});
  await writeFile(
    path.join(outputDir, 'frame-rate-summary.txt'),
    report,
    'utf-8',
  );
  await writeFile(
    path.join(outputDir, 'frame-rate-summary.json'),
    JSON.stringify(runs, null, 2),
    'utf-8',
  );

  console.log(`\nSummary saved to: ${outputDir}/frame-rate-summary.txt`);
}

void main();
