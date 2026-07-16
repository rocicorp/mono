import {readFile} from 'node:fs/promises';
import {inspect} from 'node:util';
import {startSyntheticClients, type SyntheticClient} from './client.ts';
import {loadConfig} from './config.ts';
import {
  connectBenchmarkDB,
  resetBenchmarkDatabase,
  waitForPostgres,
} from './db.ts';
import {
  analyzeProfileQueries,
  deployPermissions,
  queryPlanAnalysisLogPath,
  removeReplicaFiles,
  startPostgres,
  startZeroCache,
  stopPostgres,
  waitForZeroCache,
  type ManagedProcess,
  type ProcessCommand,
} from './processes.ts';
import {
  buildResult,
  resultOutputPath,
  sampleMetrics,
  writeResult,
  type BenchmarkResult,
  type MetricSample,
  type RecoveryMeasurement,
} from './results.ts';
import {formatDuration, log, warn, sleep} from './util.ts';
import {
  FixedRateWriter,
  MigrationWriter,
  type WriterProgress,
  type WriterStats,
} from './writer.ts';

async function main(): Promise<void> {
  const config = loadConfig();
  const cleanup = new CleanupStack();
  const processes: ProcessCommand[] = [];
  let zeroCache: ManagedProcess | undefined;
  let clients: SyntheticClient[] = [];
  let result: BenchmarkResult | undefined;
  let outputPath: string | undefined;
  let error: unknown;

  const onSigint = () => {
    warn('Interrupted. Cleaning up benchmark processes...');
    void cleanup.run().finally(() => process.exit(130));
  };
  process.once('SIGINT', onSigint);

  try {
    log(
      `zero-throughput ${config.benchmark} ${config.profile}:${config.model} run ${config.runID}`,
    );
    log(`Results will be written to ${resultOutputPath(config)}`);

    if (config.pg.start) {
      log('Starting PostgreSQL...');
      processes.push(await startPostgres());
      if (config.pg.stopAfterRun) {
        cleanup.push(() => stopPostgres());
      }
    }

    log('Waiting for PostgreSQL...');
    await waitForPostgres(config.pg.url, config.pg.readyTimeoutMs);
    const sql = connectBenchmarkDB(config.pg.url);
    cleanup.push(() => sql.end());

    if (config.reset) {
      log('Resetting benchmark database...');
      await resetBenchmarkDatabase(sql, config);
      await removeReplicaFiles(config.zero.replicaFile);
    }

    log('Deploying benchmark permissions...');
    processes.push(await deployPermissions(config));

    if (config.zero.start) {
      log('Starting zero-cache...');
      zeroCache = startZeroCache(config);
      processes.push(zeroCache);
      cleanup.push(() => zeroCache?.stop() ?? Promise.resolve());
      if (zeroCache.logPath !== undefined) {
        log(`zero-cache logs: ${zeroCache.logPath}`);
      }
    }

    log('Waiting for zero-cache...');
    await waitForZeroCache(
      config.cacheURL,
      config.zero.readyTimeoutMs,
      zeroCache,
    );

    log('Analyzing profile query plans...');
    log(`query-plan logs: ${queryPlanAnalysisLogPath(config)}`);
    const queryPlanAnalysis = await analyzeProfileQueries(config);
    processes.push(queryPlanAnalysis);

    log(`Starting ${config.users} synthetic clients...`);
    clients = await startSyntheticClients(config);
    cleanup.push(async () => {
      await Promise.all(clients.map(client => client.close()));
    });

    const recoveryBenchmark = config.benchmark !== 'throughput';
    if (config.benchmark === 'migration') {
      log(
        `Initial sync complete. Migrating ${config.migration.totalRows} rows in transactions of up to ${config.batchSize} rows at ${config.writeRate} rows/s...`,
      );
    } else {
      const phase = recoveryBenchmark ? 'overload' : 'measurement';
      log(
        `Initial sync complete. Starting ${phase} writes for ${formatDuration(config.durationMs)} at ${config.writeRate} logical writes/s...`,
      );
    }
    let writer: WriterProgress;
    let runWriter: () => Promise<WriterStats>;
    if (config.benchmark === 'migration') {
      const migrationWriter = new MigrationWriter(sql, config);
      writer = migrationWriter;
      runWriter = () => migrationWriter.run(config.migration.totalRows);
    } else {
      const fixedRateWriter = new FixedRateWriter(sql, config);
      writer = fixedRateWriter;
      runWriter = () => fixedRateWriter.run(config.durationMs);
    }
    const samples: MetricSample[] = [];
    const sampleStartedAtMs = Date.now();
    let nextProgressAtMs = sampleStartedAtMs + config.progressIntervalMs;
    const recordSample = () => {
      const sample = sampleMetrics(
        sampleStartedAtMs,
        writer.highestCommittedSeq,
        clients,
      );
      samples.push(sample);
      if (config.progressIntervalMs > 0 && Date.now() >= nextProgressAtMs) {
        printProgress(sample, config, config.users);
        nextProgressAtMs = Date.now() + config.progressIntervalMs;
      }
    };
    const sampler = setInterval(recordSample, config.sampleIntervalMs);

    let writerStats: WriterStats;
    let recovery: RecoveryMeasurement | undefined;
    try {
      writerStats = await runWriter();
      recordSample();
      if (recoveryBenchmark) {
        log(
          `${config.benchmark === 'migration' ? 'Migration' : 'Overload'} complete at seq ${writer.highestCommittedSeq}. Stopping writes and waiting up to ${formatDuration(config.recovery.timeoutMs)} for stable recovery...`,
        );
        const recoveryTiming = await waitForRecovery({
          clients,
          config,
          targetSeq: writer.highestCommittedSeq,
          sampleStartedAtMs,
          recordSample,
        });
        const recoveryEvents = await readRecoveryEvents(zeroCache?.logPath);
        recovery = {...recoveryTiming, ...recoveryEvents};
        recordSample();
      }
    } finally {
      clearInterval(sampler);
    }

    if (config.benchmark === 'throughput' && config.settleMs > 0) {
      log(`Waiting ${config.settleMs}ms for final client observations...`);
      await sleep(config.settleMs);
      samples.push(
        sampleMetrics(sampleStartedAtMs, writer.highestCommittedSeq, clients),
      );
    }

    result = buildResult({
      config,
      processes,
      writerStats,
      samples,
      clients,
      recovery,
    });
    outputPath = await writeResult(config, result);
  } catch (caught) {
    error = caught;
  } finally {
    process.off('SIGINT', onSigint);
    await cleanup.run();
  }

  if (result !== undefined && outputPath !== undefined) {
    printSummary(result.summary, outputPath);
  }
  if (error !== undefined) {
    warn(`Benchmark failed before writing results: ${formatError(error)}`);
    throw error;
  }
}

class CleanupStack {
  readonly #callbacks: (() => Promise<void>)[] = [];
  #running = false;

  push(callback: () => Promise<void>): void {
    this.#callbacks.push(callback);
  }

  async run(): Promise<void> {
    if (this.#running) {
      return;
    }
    this.#running = true;
    const callbacks = this.#callbacks.splice(0).reverse();
    for (const callback of callbacks) {
      try {
        await callback();
      } catch (error) {
        warn(`Cleanup failed: ${String(error)}`);
      }
    }
  }
}

function printSummary(
  summary: ReturnType<typeof buildResult>['summary'],
  outputPath: string,
): void {
  log('');
  log(`Result: ${summary.pass ? 'PASS' : 'FAIL'}`);
  log(
    `Target write rate: ${summary.targetWriteRate.toFixed(2)} logical writes/s`,
  );
  log(
    `Achieved write rate: ${summary.achievedWriteRate.toFixed(2)} logical writes/s`,
  );
  if (summary.recovery !== undefined) {
    log(
      `Recovery: ${summary.recovery.recovered ? 'stable' : 'timed out'} after ${formatDuration(summary.recovery.timeToStableRecoveryMs)}`,
    );
    log(
      `Time to first catch-up: ${summary.recovery.timeToFirstCaughtUpMs === undefined ? 'not reached' : formatDuration(summary.recovery.timeToFirstCaughtUpMs)}`,
    );
    log(`Overload peak seq lag: ${summary.recovery.overloadPeakSeqLag}`);
    log(`Final seq lag: ${summary.recovery.finalSeqLag}`);
    log(
      `Pipeline resets: ${summary.recovery.pipelineResets?.toString() ?? 'unavailable'}`,
    );
    log(
      `Forced rehydrations: ${summary.recovery.forcedRehydrations?.toString() ?? 'unavailable'}`,
    );
  }
  log(
    `Active-query impact rate: ${(summary.writeImpact.affectedActiveClientGroupWriteRatio * 100).toFixed(2)}%`,
  );
  log(`p95 client-visible lag: ${summary.p95ClientVisibleLagMs.toFixed(2)}ms`);
  log(`p99 client-visible lag: ${summary.p99ClientVisibleLagMs.toFixed(2)}ms`);
  log(`max seq lag: ${summary.maxSeqLag}`);
  log(`lag slope: ${summary.lagSlopeSeqPerSec.toFixed(2)} seq/s`);
  if (summary.failureReasons.length > 0) {
    log(`failure reasons: ${summary.failureReasons.join('; ')}`);
  }
  log(`details: ${outputPath}`);
}

async function waitForRecovery(args: {
  readonly clients: readonly SyntheticClient[];
  readonly config: ReturnType<typeof loadConfig>;
  readonly targetSeq: number;
  readonly sampleStartedAtMs: number;
  readonly recordSample: () => void;
}): Promise<RecoveryMeasurement> {
  const startedAtMs = Date.now();
  const startedAtElapsedMs = startedAtMs - args.sampleStartedAtMs;
  const deadlineMs = startedAtMs + args.config.recovery.timeoutMs;
  let firstCaughtUpAtMs: number | undefined;
  let stableSinceMs: number | undefined;

  for (;;) {
    const now = Date.now();
    const caughtUp = args.clients.every(client => {
      const stats = client.stats();
      return stats.connected && client.minObservedSeq() >= args.targetSeq;
    });
    if (caughtUp) {
      firstCaughtUpAtMs ??= now;
      stableSinceMs ??= now;
      if (now - stableSinceMs >= args.config.recovery.stableMs) {
        args.recordSample();
        return {
          targetSeq: args.targetSeq,
          startedAtElapsedMs,
          firstCaughtUpAtElapsedMs: firstCaughtUpAtMs - args.sampleStartedAtMs,
          finishedAtElapsedMs: now - args.sampleStartedAtMs,
          stableMs: args.config.recovery.stableMs,
          timedOut: false,
          pipelineResets: undefined,
          forcedRehydrations: undefined,
        };
      }
    } else {
      stableSinceMs = undefined;
    }

    if (now >= deadlineMs) {
      args.recordSample();
      return {
        targetSeq: args.targetSeq,
        startedAtElapsedMs,
        firstCaughtUpAtElapsedMs:
          firstCaughtUpAtMs === undefined
            ? undefined
            : firstCaughtUpAtMs - args.sampleStartedAtMs,
        finishedAtElapsedMs: now - args.sampleStartedAtMs,
        stableMs: args.config.recovery.stableMs,
        timedOut: true,
        pipelineResets: undefined,
        forcedRehydrations: undefined,
      };
    }
    await sleep(
      Math.min(args.config.recovery.pollMs, Math.max(1, deadlineMs - now)),
    );
  }
}

async function readRecoveryEvents(logPath: string | undefined): Promise<{
  readonly pipelineResets: number | undefined;
  readonly forcedRehydrations: number | undefined;
}> {
  if (logPath === undefined) {
    return {pipelineResets: undefined, forcedRehydrations: undefined};
  }
  try {
    const contents = await readFile(logPath, 'utf8');
    return {
      pipelineResets: countOccurrences(contents, 'resetting pipelines:'),
      forcedRehydrations: countOccurrences(
        contents,
        'post-reset catchup timed out',
      ),
    };
  } catch {
    return {pipelineResets: undefined, forcedRehydrations: undefined};
  }
}

function countOccurrences(contents: string, needle: string): number {
  let count = 0;
  let offset = 0;
  while ((offset = contents.indexOf(needle, offset)) !== -1) {
    count++;
    offset += needle.length;
  }
  return count;
}

function printProgress(
  sample: MetricSample,
  config: ReturnType<typeof loadConfig>,
  expectedClients: number,
): void {
  const progress =
    config.benchmark === 'migration'
      ? `${sample.committedSeq} / ${config.migration.totalRows} rows`
      : `${formatDuration(Math.min(sample.elapsedMs, config.durationMs))} / ${formatDuration(config.durationMs)}`;
  log(
    `Progress: ${progress}, committed=${sample.committedSeq}, seqLag=${sample.seqLag}, connected=${sample.connectedClients}/${expectedClients}`,
  );
}

function formatError(error: unknown): string {
  if (error instanceof Error) {
    return error.stack ?? error.message;
  }
  if (typeof error === 'string') {
    return error;
  }
  return inspect(error);
}

await main();
