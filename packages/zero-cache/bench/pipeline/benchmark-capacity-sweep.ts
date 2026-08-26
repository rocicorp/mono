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

export interface CapacityRunResult {
  readonly writeRate: number;
  readonly modeName: string;
  readonly result: BenchmarkResult;
}

export function formatCapacityTable(
  runs: readonly CapacityRunResult[],
): string {
  const lines: string[] = [
    '========================================================================================================================',
    '                        SINGLE VIEW-SYNCER CAPACITY & MAX-OUT WRITE RATE SWEEP                                          ',
    '========================================================================================================================',
    ' Target Rate | Mode                | Actual Rate | Pokes Rx | IVM Adv (p50/p95) | E2E Lag (avg/p50/p95)    | Status     ',
    '-------------+---------------------+-------------+----------+-------------------+--------------------------+------------',
  ];

  for (const {writeRate, modeName, result} of runs) {
    const actualRate = result.loadStats.actualRate.toFixed(1);
    const totalPokes = result.clientStats.reduce(
      (sum, c) => sum + c.pokesReceived,
      0,
    );

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
      ` ${String(writeRate).padEnd(11)} | ` +
        `${modeName.padEnd(19)} | ` +
        `${actualRate.padEnd(11)} | ` +
        `${String(totalPokes).padEnd(8)} | ` +
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
      'write-rates': {type: 'string', default: '100,250,500,1000,1500,2000'},
      'clients-per-vs': {type: 'string', default: '5'},
      'rows-per-tx': {type: 'string', default: '1'},
      'load-duration': {type: 'string', default: '6'},
      'drain-timeout': {type: 'string', default: '3'},
      'output-dir': {type: 'string', default: './bench-results/capacity-sweep'},
      'log-level': {type: 'string', default: 'info'},
      'help': {type: 'boolean', short: 'h', default: false},
    },
    allowPositionals: true,
  });

  process.argv = process.argv.slice(0, 2);

  if (values.help) {
    console.log(`
Capacity Sweep Benchmark Runner

Options:
  --write-rates <rates>  Comma-separated write rates (default: 100,250,500,1000,1500,2000)
  --clients-per-vs <N>   Simulated clients per view syncer (default: 5)
  --rows-per-tx <N>      Rows per database transaction (default: 1)
  --load-duration <N>    Load duration in seconds per test (default: 6)
  --drain-timeout <N>    Drain timeout in seconds (default: 3)
  --output-dir <path>    Output directory
  -h, --help             Show this help message
    `);
    process.exit(0);
  }

  const writeRates = (values['write-rates'] ?? '100,250,500,1000,1500,2000')
    .split(',')
    .map(s => Number(s.trim()))
    .filter(n => Number.isFinite(n) && n > 0);

  const clientsPerVS = Math.max(1, Number(values['clients-per-vs']));
  const rowsPerTx = Math.max(1, Number(values['rows-per-tx'] ?? '1'));
  const loadDuration = Number(values['load-duration']);
  const drainTimeout = Number(values['drain-timeout']);
  const outputDir = values['output-dir'] ?? './bench-results/capacity-sweep';
  const logLevel = (values['log-level'] ?? 'info') as
    | 'error'
    | 'warn'
    | 'info'
    | 'debug';

  console.log(
    `=================================================================`,
  );
  console.log(`Single View-Syncer Capacity Sweep (Baseline vs Adaptive)`);
  console.log(
    `Topology: 1 RM, 1 View-Syncer, ${clientsPerVS} Clients, ${rowsPerTx} Rows/Tx`,
  );
  console.log(`Write Rates to test: ${writeRates.join(', ')} writes/sec`);
  console.log(
    `=================================================================\n`,
  );

  const runs: CapacityRunResult[] = [];

  for (const rate of writeRates) {
    for (const mode of ['Baseline (Uncapped)', 'Adaptive (Dynamic)']) {
      const isAdaptive = mode.startsWith('Adaptive');
      console.log(`---> Testing ${mode} @ ${rate} writes/sec...`);

      const config: BenchmarkConfig = {
        numReplicationManagers: 1,
        numViewSyncers: 1,
        clientsPerViewSyncer: clientsPerVS,
        clientQueries: getDefaultZbugsQueries(),
        loadGenerator: createZbugsLoadGenerator({rowsPerTx}),
        writeRatePerSecond: rate,
        loadDurationSeconds: loadDuration,
        drainTimeoutSeconds: drainTimeout,
        seedDatabase: db =>
          seedZbugsDatabase(db, {appID: 'zero', numIssues: 100}),
        dbMode: 'docker',
        sqliteChangeLogMode: 'serve',
        appID: 'zero',
        profileReplicationManager: false,
        profileViewSyncer: false,
        outputDir: path.join(
          outputDir,
          `rate-${rate}-${isAdaptive ? 'adaptive' : 'baseline'}`,
        ),
        logLevel,
        minAdvanceIntervalMs: 0,
        adaptiveFrameRate: isAdaptive,
      };

      try {
        const result = await runPipelineBenchmark(config);
        runs.push({
          writeRate: rate,
          modeName: mode,
          result,
        });
        const pokes = result.clientStats.reduce(
          (s, c) => s + c.pokesReceived,
          0,
        );
        console.log(
          `     [DONE] ${mode} @ ${rate} w/s -> Pokes: ${pokes}, Avg E2E Lag: ${result.metrics.e2eServingLagMs?.avg.toFixed(1) ?? 'N/A'}ms\n`,
        );
      } catch (err) {
        console.error(`     [ERROR] ${mode} @ ${rate} w/s failed:`, err);
      }
    }
  }

  const report = formatCapacityTable(runs);
  console.log('\n' + report);

  await mkdir(outputDir, {recursive: true});
  await writeFile(
    path.join(outputDir, 'capacity-summary.txt'),
    report,
    'utf-8',
  );
  await writeFile(
    path.join(outputDir, 'capacity-summary.json'),
    JSON.stringify(runs, null, 2),
    'utf-8',
  );

  console.log(`\nSummary saved to: ${outputDir}/capacity-summary.txt`);
}

void main();
