import {execFileSync} from 'node:child_process';
import {mkdir, writeFile} from 'node:fs/promises';
import {dirname} from 'node:path';
import type {ClientStats, SyntheticClient} from './client.ts';
import type {BenchmarkConfig} from './config.ts';
import {appPath, appRoot} from './config.ts';
import type {ProcessCommand} from './processes.ts';
import {average, max, percentile} from './util.ts';
import type {WriterStats} from './writer.ts';

export type MetricSample = {
  readonly elapsedMs: number;
  readonly committedSeq: number;
  readonly minObservedSeq: number;
  readonly seqLag: number;
  readonly connectedClients: number;
};

export type BenchmarkResult = {
  readonly gitCommit: string | undefined;
  readonly profile: string;
  readonly config: BenchmarkConfig;
  readonly processes: readonly ProcessCommand[];
  readonly environment: {
    readonly node: string;
    readonly platform: string;
    readonly arch: string;
  };
  readonly samples: readonly MetricSample[];
  readonly clients: readonly ClientStats[];
  readonly summary: {
    readonly targetWriteRate: number;
    readonly achievedWriteRate: number;
    readonly committedRows: number;
    readonly committedTransactions: number;
    readonly highestCommittedSeq: number;
    readonly minObservedSeq: number;
    readonly maxSeqLag: number;
    readonly lagSlopeSeqPerSec: number;
    readonly p50ClientVisibleLagMs: number;
    readonly p95ClientVisibleLagMs: number;
    readonly p99ClientVisibleLagMs: number;
    readonly maxClientVisibleLagMs: number;
    readonly txLatencyP50Ms: number;
    readonly txLatencyP95Ms: number;
    readonly txLatencyP99Ms: number;
    readonly txLatencyAverageMs: number;
    readonly pass: boolean;
    readonly failureReasons: readonly string[];
  };
};

export function sampleMetrics(
  startedAtMs: number,
  committedSeq: number,
  clients: readonly SyntheticClient[],
): MetricSample {
  const observedSeqs = clients.map(client => client.minObservedSeq());
  const minObservedSeq =
    observedSeqs.length === 0 ? 0 : Math.min(...observedSeqs);
  return {
    elapsedMs: Date.now() - startedAtMs,
    committedSeq,
    minObservedSeq,
    seqLag: Math.max(0, committedSeq - minObservedSeq),
    connectedClients: clients.filter(client => client.stats().connected).length,
  };
}

export function buildResult(args: {
  readonly config: BenchmarkConfig;
  readonly processes: readonly ProcessCommand[];
  readonly writerStats: WriterStats;
  readonly samples: readonly MetricSample[];
  readonly clients: readonly SyntheticClient[];
}): BenchmarkResult {
  const clientStats = args.clients.map(client => client.stats());
  const latencySamples = args.clients.flatMap(client =>
    client.latencySamplesMs(),
  );
  const minObservedSeq =
    args.clients.length === 0
      ? 0
      : Math.min(...args.clients.map(client => client.minObservedSeq()));
  const maxSeqLag = max(args.samples.map(sample => sample.seqLag));
  const measuredSeconds =
    (args.writerStats.finishedAtMs - args.writerStats.startedAtMs) / 1000;
  const failureReasons = failureReasonsFor({
    config: args.config,
    clientStats,
    p99ClientVisibleLagMs: percentile(latencySamples, 99),
    maxSeqLag,
    lagSlopeSeqPerSec: lagSlope(args.samples),
  });

  return {
    gitCommit: gitCommit(),
    profile: args.config.profile,
    config: args.config,
    processes: args.processes,
    environment: {
      node: process.version,
      platform: process.platform,
      arch: process.arch,
    },
    samples: args.samples,
    clients: clientStats,
    summary: {
      targetWriteRate: args.config.writeRate,
      achievedWriteRate:
        measuredSeconds === 0
          ? 0
          : args.writerStats.committedRows / measuredSeconds,
      committedRows: args.writerStats.committedRows,
      committedTransactions: args.writerStats.committedTransactions,
      highestCommittedSeq: args.writerStats.highestCommittedSeq,
      minObservedSeq,
      maxSeqLag,
      lagSlopeSeqPerSec: lagSlope(args.samples),
      p50ClientVisibleLagMs: percentile(latencySamples, 50),
      p95ClientVisibleLagMs: percentile(latencySamples, 95),
      p99ClientVisibleLagMs: percentile(latencySamples, 99),
      maxClientVisibleLagMs: max(latencySamples),
      txLatencyP50Ms: percentile(args.writerStats.transactionLatencyMs, 50),
      txLatencyP95Ms: percentile(args.writerStats.transactionLatencyMs, 95),
      txLatencyP99Ms: percentile(args.writerStats.transactionLatencyMs, 99),
      txLatencyAverageMs: average(args.writerStats.transactionLatencyMs),
      pass: failureReasons.length === 0,
      failureReasons,
    },
  };
}

export async function writeResult(
  config: BenchmarkConfig,
  result: BenchmarkResult,
): Promise<string> {
  const outputPath = resultOutputPath(config);
  await mkdir(dirname(outputPath), {recursive: true});
  await writeFile(outputPath, `${JSON.stringify(result, null, 2)}\n`);
  return outputPath;
}

export function resultOutputPath(config: BenchmarkConfig): string {
  return appPath(config.outputPath);
}

function failureReasonsFor(args: {
  readonly config: BenchmarkConfig;
  readonly clientStats: readonly ClientStats[];
  readonly p99ClientVisibleLagMs: number;
  readonly maxSeqLag: number;
  readonly lagSlopeSeqPerSec: number;
}): string[] {
  const reasons: string[] = [];
  const disconnected = args.clientStats.filter(client => !client.connected);
  if (disconnected.length > 0) {
    reasons.push(`${disconnected.length} clients were disconnected at the end`);
  }
  if (
    args.clientStats.some(client =>
      client.queries.some(query => query.initialSyncMs === undefined),
    )
  ) {
    reasons.push('at least one query did not complete initial sync');
  }
  if (args.p99ClientVisibleLagMs > args.config.sloP99LagMs) {
    reasons.push(
      `p99 client-visible lag ${args.p99ClientVisibleLagMs}ms exceeded SLO ${args.config.sloP99LagMs}ms`,
    );
  }
  const allowedSeqLag = Math.ceil(
    args.config.writeRate * (args.config.sloP99LagMs / 1000),
  );
  if (args.maxSeqLag > allowedSeqLag) {
    reasons.push(
      `max seq lag ${args.maxSeqLag} exceeded SLO-equivalent ${allowedSeqLag}`,
    );
  }
  if (args.lagSlopeSeqPerSec > args.config.writeRate * 0.05) {
    reasons.push(
      `lag slope ${args.lagSlopeSeqPerSec.toFixed(2)} seq/s was positive`,
    );
  }
  return reasons;
}

function lagSlope(samples: readonly MetricSample[]): number {
  if (samples.length < 2) {
    return 0;
  }
  const first = samples.at(0);
  const last = samples.at(-1);
  if (first === undefined || last === undefined) {
    return 0;
  }
  const elapsedSeconds = (last.elapsedMs - first.elapsedMs) / 1000;
  if (elapsedSeconds <= 0) {
    return 0;
  }
  return (last.seqLag - first.seqLag) / elapsedSeconds;
}

function gitCommit(): string | undefined {
  try {
    return execFileSync('git', ['rev-parse', 'HEAD'], {
      cwd: appRoot,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'ignore'],
    }).trim();
  } catch {
    return undefined;
  }
}
