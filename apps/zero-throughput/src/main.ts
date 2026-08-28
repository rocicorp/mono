import {inspect} from 'node:util';
import {startSyntheticClients, type SyntheticClient} from './client.ts';
import {loadConfig} from './config.ts';
import {
  connectBenchmarkDB,
  resetBenchmarkDatabase,
  waitForPostgres,
} from './db.ts';
import {OTelMetricsCollector} from './metrics.ts';
import {
  analyzeProfileQueries,
  deployPermissions,
  queryPlanAnalysisLogPath,
  removeReplicaFiles,
  startPostgres,
  startZeroTopology,
  stopPostgres,
  waitForZeroCache,
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
      `zero-throughput ${config.profile}:${config.model} run ${config.runID}`,
    );
    log(`Results will be written to ${resultOutputPath(config)}`);

    const metricsCollector = new OTelMetricsCollector();
    await metricsCollector.start();
    cleanup.push(() => metricsCollector.stop());

    if (config.pg.start) {
      log('Starting PostgreSQL...');
      processes.push(await startPostgres());
      if (config.pg.stopAfterRun) {
        cleanup.push(() => stopPostgres());
      }
    }

    log('Waiting for PostgreSQL...');
    await waitForPostgres(config.pg.url, config.pg.readyTimeoutMs);
    const sql = connectBenchmarkDB(
      config.pg.url,
      Math.max(20, config.writeConcurrency * 2),
    );
    cleanup.push(() => sql.end());

    if (config.reset) {
      log('Resetting benchmark database...');
      await resetBenchmarkDatabase(sql, config);
      await removeReplicaFiles(config.zero.replicaFile);
    }

    log('Deploying benchmark permissions...');
    processes.push(await deployPermissions(config));

    if (config.zero.start) {
      log(
        `Starting zero topology (${config.topology}${config.topology === 'distributed' ? `, ${config.numViewSyncers} VS, 1 RM` : ''})...`,
      );
      const topology = await startZeroTopology(
        config,
        metricsCollector.endpoint,
      );
      processes.push(...topology.processes);
      cleanup.push(() => topology.stop());
      for (const p of topology.processes) {
        if (p.logPath !== undefined) {
          log(`${p.name} logs: ${p.logPath}`);
        }
      }

      log('Waiting for zero-cache instances...');
      for (let i = 0; i < topology.readyURLs.length; i++) {
        const url = topology.readyURLs[i];
        const proc =
          topology.processes.find(p => p.name === `vs-${i}`) ??
          topology.processes[0];
        await waitForZeroCache(url, config.zero.readyTimeoutMs, proc);
      }
    }

    if (config.topology === 'single') {
      log('Analyzing profile query plans...');
      log(`query-plan logs: ${queryPlanAnalysisLogPath(config)}`);
      const queryPlanAnalysis = await analyzeProfileQueries(config);
      processes.push(queryPlanAnalysis);
    }

    log(`Starting ${config.users} synthetic clients...`);
    clients = await startSyntheticClients(config);
    cleanup.push(async () => {
      await Promise.all(clients.map(client => client.close()));
    });

    log(
      `Initial sync complete. Writing for ${formatDuration(config.durationMs)} at ${config.writeRate} logical writes/s (concurrency=${config.writeConcurrency}, batch=${config.batchSize})...`,
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
      log(
        `Draining pipeline (waiting up to ${config.settleMs}ms for clients to observe seq ${writer.highestCommittedSeq})...`,
      );
      const settleDeadline = Date.now() + config.settleMs;
      while (Date.now() < settleDeadline) {
        const minObserved =
          clients.length === 0
            ? 0
            : Math.min(...clients.map(c => c.minObservedSeq()));
        if (minObserved >= writer.highestCommittedSeq) {
          break;
        }
        await sleep(100);
      }
      samples.push(
        sampleMetrics(sampleStartedAtMs, writer.highestCommittedSeq, clients),
      );
    }

    const metricsSummary = metricsCollector.getSummary();
    result = buildResult({
      config,
      processes,
      writerStats,
      samples,
      clients,
      metricsSummary,
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
  log(
    `Active-query impact rate: ${(summary.writeImpact.affectedActiveClientGroupWriteRatio * 100).toFixed(2)}%`,
  );
  log(`p95 client-visible lag: ${summary.p95ClientVisibleLagMs.toFixed(2)}ms`);
  log(`p99 client-visible lag: ${summary.p99ClientVisibleLagMs.toFixed(2)}ms`);
  log(`max seq lag: ${summary.maxSeqLag}`);
  log(`lag slope: ${summary.lagSlopeSeqPerSec.toFixed(2)} seq/s`);
  if (summary.replicationLagMs) {
    log(
      `RM replication lag: p50=${summary.replicationLagMs.p50.toFixed(1)}ms, p95=${summary.replicationLagMs.p95.toFixed(1)}ms, max=${summary.replicationLagMs.max.toFixed(1)}ms`,
    );
  }
  if (summary.advancementLatencyMs) {
    log(
      `IVM advance duration: p50=${summary.advancementLatencyMs.p50.toFixed(1)}ms, p95=${summary.advancementLatencyMs.p95.toFixed(1)}ms, max=${summary.advancementLatencyMs.max.toFixed(1)}ms`,
    );
  }
  if (summary.e2eServingLagMs) {
    log(
      `E2E serving lag: avg=${summary.e2eServingLagMs.avg.toFixed(1)}ms, p50=${summary.e2eServingLagMs.p50.toFixed(1)}ms, p95=${summary.e2eServingLagMs.p95.toFixed(1)}ms`,
    );
  }
  if (summary.pipelineResets !== undefined && summary.pipelineResets > 0) {
    log(`Pipeline resets: ${summary.pipelineResets}`);
  }
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
