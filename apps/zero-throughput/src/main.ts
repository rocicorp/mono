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
} from './results.ts';
import {formatDuration, log, warn, sleep} from './util.ts';
import {FixedRateWriter, type WriterStats} from './writer.ts';

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
    log(`zero-throughput ${config.profile} run ${config.runID}`);
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

    log(
      `Initial sync complete. Writing for ${formatDuration(config.durationMs)} at ${config.writeRate} logical writes/s...`,
    );
    const writer = new FixedRateWriter(sql, config);
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
        printProgress(sample, config.durationMs, config.users);
        nextProgressAtMs = Date.now() + config.progressIntervalMs;
      }
    };
    const sampler = setInterval(recordSample, config.sampleIntervalMs);

    let writerStats: WriterStats;
    try {
      writerStats = await writer.run(config.durationMs);
      recordSample();
    } finally {
      clearInterval(sampler);
    }

    if (config.settleMs > 0) {
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
  log(`p95 client-visible lag: ${summary.p95ClientVisibleLagMs.toFixed(2)}ms`);
  log(`p99 client-visible lag: ${summary.p99ClientVisibleLagMs.toFixed(2)}ms`);
  log(`max seq lag: ${summary.maxSeqLag}`);
  log(`lag slope: ${summary.lagSlopeSeqPerSec.toFixed(2)} seq/s`);
  if (summary.failureReasons.length > 0) {
    log(`failure reasons: ${summary.failureReasons.join('; ')}`);
  }
  log(`details: ${outputPath}`);
}

function printProgress(
  sample: MetricSample,
  durationMs: number,
  expectedClients: number,
): void {
  log(
    `Progress: ${formatDuration(Math.min(sample.elapsedMs, durationMs))} / ${formatDuration(durationMs)}, committed=${sample.committedSeq}, seqLag=${sample.seqLag}, connected=${sample.connectedClients}/${expectedClients}`,
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
