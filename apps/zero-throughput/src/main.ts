import {startSyntheticClients, type SyntheticClient} from './client.ts';
import {loadConfig} from './config.ts';
import {
  connectBenchmarkDB,
  resetBenchmarkDatabase,
  waitForPostgres,
} from './db.ts';
import {
  deployPermissions,
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
  sampleMetrics,
  writeResult,
  type MetricSample,
} from './results.ts';
import {log, warn, sleep} from './util.ts';
import {FixedRateWriter} from './writer.ts';

async function main(): Promise<void> {
  const config = loadConfig();
  const cleanup = new CleanupStack();
  const processes: ProcessCommand[] = [];
  let zeroCache: ManagedProcess | undefined;
  let clients: SyntheticClient[] = [];

  const onSigint = () => {
    warn('Interrupted. Cleaning up benchmark processes...');
    void cleanup.run().finally(() => process.exit(130));
  };
  process.once('SIGINT', onSigint);

  try {
    log(`zero-throughput ${config.profile} run ${config.runID}`);

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
    }

    log('Waiting for zero-cache...');
    await waitForZeroCache(
      config.cacheURL,
      config.zero.readyTimeoutMs,
      zeroCache,
    );

    log(`Starting ${config.users} synthetic clients...`);
    clients = await startSyntheticClients(config);
    cleanup.push(async () => {
      await Promise.all(clients.map(client => client.close()));
    });

    log('Initial sync complete. Starting fixed-rate writer...');
    const writer = new FixedRateWriter(sql, config);
    const samples: MetricSample[] = [];
    const sampleStartedAtMs = Date.now();
    const sampler = setInterval(() => {
      samples.push(
        sampleMetrics(sampleStartedAtMs, writer.highestCommittedSeq, clients),
      );
    }, config.sampleIntervalMs);

    const writerStats = await writer.run(config.durationMs);
    samples.push(
      sampleMetrics(sampleStartedAtMs, writer.highestCommittedSeq, clients),
    );

    if (config.settleMs > 0) {
      log(`Waiting ${config.settleMs}ms for final client observations...`);
      await sleep(config.settleMs);
      samples.push(
        sampleMetrics(sampleStartedAtMs, writer.highestCommittedSeq, clients),
      );
    }
    clearInterval(sampler);

    const result = buildResult({
      config,
      processes,
      writerStats,
      samples,
      clients,
    });
    const outputPath = await writeResult(config, result);

    printSummary(result.summary, outputPath);
  } finally {
    process.off('SIGINT', onSigint);
    await cleanup.run();
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
  log(`Target write rate: ${summary.targetWriteRate.toFixed(2)} rows/s`);
  log(`Achieved write rate: ${summary.achievedWriteRate.toFixed(2)} rows/s`);
  log(`p95 client-visible lag: ${summary.p95ClientVisibleLagMs.toFixed(2)}ms`);
  log(`p99 client-visible lag: ${summary.p99ClientVisibleLagMs.toFixed(2)}ms`);
  log(`max seq lag: ${summary.maxSeqLag}`);
  log(`lag slope: ${summary.lagSlopeSeqPerSec.toFixed(2)} seq/s`);
  if (summary.failureReasons.length > 0) {
    log(`failure reasons: ${summary.failureReasons.join('; ')}`);
  }
  log(`details: ${outputPath}`);
}

await main();
