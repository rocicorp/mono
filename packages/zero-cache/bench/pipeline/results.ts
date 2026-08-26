import {writeFile} from 'node:fs/promises';
import {join} from 'node:path';
import type {ClientStats} from './client-simulator.ts';
import type {LoadStats} from './config.ts';
import type {MetricSummary, PercentileStats} from './metrics-collector.ts';

export interface BenchmarkResult {
  readonly timestamp: string;
  readonly topology: {
    readonly numReplicationManagers: number;
    readonly numViewSyncers: number;
    readonly totalClients: number;
    readonly clientsPerViewSyncer: number;
  };
  readonly config: {
    readonly writeRatePerSecond: number;
    readonly loadDurationSeconds: number;
    readonly sqliteChangeLogMode: string;
    readonly dbMode: string;
  };
  readonly loadStats: LoadStats;
  readonly clientStats: readonly ClientStats[];
  readonly metrics: MetricSummary;
}

function formatPercentiles(stats: PercentileStats | null): string {
  if (!stats || stats.count === 0) return 'N/A';
  return (
    `avg: ${stats.avg.toFixed(1)}ms | ` +
    `p50: ${stats.p50.toFixed(1)}ms | ` +
    `p95: ${stats.p95.toFixed(1)}ms | ` +
    `p99: ${stats.p99.toFixed(1)}ms | ` +
    `max: ${stats.max.toFixed(1)}ms (${stats.count} samples)`
  );
}

export function formatBenchmarkReport(result: BenchmarkResult): string {
  const {metrics, loadStats, topology, config} = result;

  const lines: string[] = [
    '================================================================================',
    '                   ZERO-CACHE PIPELINE BENCHMARK REPORT',
    '================================================================================',
    `Timestamp:           ${result.timestamp}`,
    `Topology:            ${topology.numReplicationManagers} RM(s) | ${topology.numViewSyncers} View-Syncer(s) | ${topology.totalClients} Connected Client(s)`,
    `Change-Log Mode:     ${config.sqliteChangeLogMode}`,
    `Database Mode:       ${config.dbMode}`,
    `Load Target:         ${config.writeRatePerSecond} writes/sec for ${config.loadDurationSeconds}s`,
    `Actual Load:         ${loadStats.writesSucceeded}/${loadStats.writesAttempted} writes (${loadStats.actualRate.toFixed(1)} writes/sec, ${loadStats.durationMs.toFixed(0)}ms)`,
    '--------------------------------------------------------------------------------',
    'KEY METRICS:',
    '--------------------------------------------------------------------------------',
    `Replication Lag (PG -> RM):    ${formatPercentiles(metrics.replicationLagMs)}`,
    `Advancement Latency (IVM):     ${formatPercentiles(metrics.advancementLatencyMs)}`,
    `E2E Serving Lag (PG -> Poke):  ${formatPercentiles(metrics.e2eServingLagMs)}`,
    `View-Syncer Lag:               ${formatPercentiles(metrics.viewSyncerLagMs)}`,
    `Hydration Time:                ${formatPercentiles(metrics.hydrationTimeMs)}`,
    `Flow Control Wait Duration:    ${formatPercentiles(metrics.flowControlWaitDurationMs)}`,
    `Transactions Replicated:       ${metrics.transactionsReplicated}`,
    `Changes Replicated:            ${metrics.changesReplicated}`,
    `Flow Control Checkpoints:      ${metrics.flowControlWaits}`,
    `Pipeline Resets:               ${metrics.pipelineResets}`,
    '--------------------------------------------------------------------------------',
    'CLIENT SYNC METRICS:',
    '--------------------------------------------------------------------------------',
  ];

  const hydrationTimes = result.clientStats
    .map(c => c.initialHydrationDurationMs)
    .filter((t): t is number => t !== null);

  if (hydrationTimes.length > 0) {
    const avgHydration =
      hydrationTimes.reduce((a, b) => a + b, 0) / hydrationTimes.length;
    const maxHydration = Math.max(...hydrationTimes);
    lines.push(
      `Client Initial Hydration:      avg: ${avgHydration.toFixed(1)}ms | max: ${maxHydration.toFixed(1)}ms`,
    );
  }

  const totalPokes = result.clientStats.reduce(
    (sum, c) => sum + c.pokesReceived,
    0,
  );
  lines.push(`Total Pokes Received:          ${totalPokes}`);

  const totalErrors = result.clientStats.reduce(
    (sum, c) => sum + c.errors.length,
    0,
  );
  lines.push(`Client Errors:                 ${totalErrors}`);
  lines.push(
    '================================================================================',
  );

  return lines.join('\n');
}

export async function saveBenchmarkResults(
  result: BenchmarkResult,
  outputDir: string,
): Promise<string> {
  const jsonPath = join(outputDir, `benchmark_${Date.now()}.json`);
  const latestPath = join(outputDir, 'latest.json');
  const jsonContent = JSON.stringify(result, null, 2);

  await writeFile(jsonPath, jsonContent);
  await writeFile(latestPath, jsonContent);

  return jsonPath;
}
