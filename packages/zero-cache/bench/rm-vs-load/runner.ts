import {performance} from 'node:perf_hooks';
import type {LogContext} from '@rocicorp/logger';
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {
  ChangeTag,
  WatermarkedChange,
} from '../../src/services/change-streamer/change-streamer-service.ts';
import {Forwarder} from '../../src/services/change-streamer/forwarder.ts';
import {
  ensureReplicationConfig,
  setupCDCTables,
} from '../../src/services/change-streamer/schema/tables.ts';
import {Storer} from '../../src/services/change-streamer/storer.ts';
import {postgresTypeConfig, type PostgresDB} from '../../src/types/pg.ts';
import {cdcSchema, type ShardID} from '../../src/types/shards.ts';
import {ConsumerFactory} from './consumers.ts';
import {emptyOperationCounts, watermarkFor} from './fixtures.ts';
import {percentile, sleep, sum} from './perf-utils.ts';
import {cleanupSQLite, initializeReplica} from './replica.ts';
import type {
  BenchmarkConfig,
  LoadConsumer,
  Scenario,
  ScenarioSummary,
  Summary,
} from './types.ts';
import {
  addOperationCounts,
  createTransactionGenerator,
  workloadName,
} from './workloads.ts';

const defaultShard: ShardID = {appID: 'bench', shardNum: 0};
const defaultReplicaVersion = '000000000000';

export type BenchmarkSuiteOptions = {
  readonly lc?: LogContext | undefined;
  readonly shard?: ShardID | undefined;
  readonly replicaVersion?: string | undefined;
  readonly sqlitePath?: string | undefined;
};

export class BenchmarkSuite {
  readonly #config: BenchmarkConfig;
  readonly #scenarios: readonly Scenario[];
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #replicaVersion: string;
  readonly #sqlitePath: string;

  constructor(
    config: BenchmarkConfig,
    scenarios: readonly Scenario[],
    {
      lc = createSilentLogContext(),
      shard = defaultShard,
      replicaVersion = defaultReplicaVersion,
      sqlitePath = `/tmp/zero-cache-rm-vs-load-${process.pid}.db`,
    }: BenchmarkSuiteOptions = {},
  ) {
    this.#config = config;
    this.#scenarios = scenarios;
    this.#lc = lc;
    this.#shard = shard;
    this.#replicaVersion = replicaVersion;
    this.#sqlitePath = sqlitePath;
  }

  async run(): Promise<Summary> {
    const container = await new PostgreSqlContainer(
      this.#config.pgImage,
    ).start();
    try {
      const changeDB = postgres(container.getConnectionUri(), {
        ...postgresTypeConfig({sendStringAsJson: true}),
        onnotice: () => {},
      });
      try {
        const runner = new ScenarioRunner(changeDB, {
          config: this.#config,
          lc: this.#lc,
          shard: this.#shard,
          replicaVersion: this.#replicaVersion,
          sqlitePath: this.#sqlitePath,
        });
        const scenarios: ScenarioSummary[] = [];
        for (const scenario of this.#scenarios) {
          scenarios.push(await runner.run(scenario));
        }
        return {
          name: 'zero-cache-rm-vs-load',
          mode: this.#config.mode,
          generatedAt: new Date().toISOString(),
          rmCount: 1,
          viewSyncerCount: this.#config.consumer.count,
          scenarios,
        };
      } finally {
        await changeDB.end();
      }
    } finally {
      await container.stop();
      cleanupSQLite(this.#sqlitePath);
    }
  }
}

type ScenarioRunnerOptions = {
  readonly config: BenchmarkConfig;
  readonly lc: LogContext;
  readonly shard: ShardID;
  readonly replicaVersion: string;
  readonly sqlitePath: string;
};

export class ScenarioRunner {
  readonly #changeDB: PostgresDB;
  readonly #config: BenchmarkConfig;
  readonly #lc: LogContext;
  readonly #shard: ShardID;
  readonly #replicaVersion: string;
  readonly #sqlitePath: string;
  readonly #consumers: ConsumerFactory;

  constructor(
    changeDB: PostgresDB,
    {config, lc, shard, replicaVersion, sqlitePath}: ScenarioRunnerOptions,
  ) {
    this.#changeDB = changeDB;
    this.#config = config;
    this.#lc = lc;
    this.#shard = shard;
    this.#replicaVersion = replicaVersion;
    this.#sqlitePath = sqlitePath;
    this.#consumers = new ConsumerFactory({
      lc,
      sqlitePath,
      replicaVersion,
      workerBatchMessages: config.workerBatchMessages,
    });
  }

  async run(scenario: Scenario): Promise<ScenarioSummary> {
    cleanupSQLite(this.#sqlitePath);
    let replica: Database | undefined;
    let processor: ReturnType<typeof initializeReplica> | undefined;
    if (this.#config.sourceApply) {
      replica = new Database(this.#lc, this.#sqlitePath);
      replica.pragma('journal_mode = WAL');
      processor = initializeReplica(this.#lc, replica, this.#replicaVersion);
    }

    const forwarder = new Forwarder(this.#lc, {
      flowControlConsensusPaddingSeconds: 0.001,
    });
    const consumers = await this.#consumers.createGroup(this.#config.consumer);
    for (const {sub} of consumers) {
      forwarder.add(sub);
    }

    await this.#resetChangeDB();
    const fatalErrors: Error[] = [];
    const storer = new Storer(
      this.#lc,
      this.#shard,
      `rm-vs-load-${scenario.name}`,
      'bench-rm:12345',
      'ws',
      this.#changeDB,
      this.#replicaVersion,
      () => {},
      err => fatalErrors.push(err),
      {
        backPressureLimitHeapProportion: 0.04,
        statementTimeoutMs: 20_000,
      },
    );
    await storer.assumeOwnership();
    const storerDone = storer.run().catch(e => {
      const err = e instanceof Error ? e : new Error(String(e));
      fatalErrors.push(err);
    });

    const latencies: number[] = [];
    let tx = 0;
    let rows = 0;
    const operationCounts = emptyOperationCounts();
    let storerBytes = 0;
    let fanoutMessages = 0;
    let unflushedBytes = 0;
    let reconnect: LoadConsumer | undefined;
    let reconnectCatchupFrom: string | null = null;
    let latestGeneratedWatermark: string | null = null;
    let loadPhaseMs = 0;
    let reconnectStartedAtTx: number | null = null;
    let reconnectStartedAtMs: number | null = null;
    let reconnectStartAbsoluteMs: number | null = null;
    let reconnectJoinWatermark: string | null = null;
    let reconnectCaughtUpToJoinMs: number | null = null;
    let reconnectCaughtUpToJoinDuringLoad = false;
    let reconnectCaughtUpToFinalMs: number | null = null;
    let reconnectFinalCatchupWaitMs: number | null = null;
    let summary: ScenarioSummary | undefined;
    const cpuStart = process.cpuUsage();
    const memoryStart = process.memoryUsage();
    let maxHeapUsedBytes = memoryStart.heapUsed;
    let maxRssBytes = memoryStart.rss;
    const sampleMemory = () => {
      const memory = process.memoryUsage();
      maxHeapUsedBytes = Math.max(maxHeapUsedBytes, memory.heapUsed);
      maxRssBytes = Math.max(maxRssBytes, memory.rss);
      return memory;
    };
    const start = performance.now();
    let nextDue = start;
    const generator = createTransactionGenerator(scenario);

    try {
      while (performance.now() - start < this.#config.durationMs) {
        tx++;
        const generated = generator.next(tx + 1);
        latestGeneratedWatermark = generated.watermark;
        const txStart = performance.now();

        for (const change of generated.changes) {
          processor?.processMessage(this.#lc, change);
          const watermark = generated.watermark;
          const json = storer.store(watermark, change);
          const jsonBytes = Buffer.byteLength(json);
          const entry: WatermarkedChange = [
            watermark,
            change[1].tag as ChangeTag,
            json,
          ];

          unflushedBytes += jsonBytes;
          const shouldWaitForFanout =
            unflushedBytes >= this.#config.flushBytesThreshold &&
            change[0] === 'commit';
          if (shouldWaitForFanout) {
            await forwarder.forwardWithFlowControl(entry);
            unflushedBytes = 0;
          } else {
            forwarder.forward(entry);
          }
          const readyForMore = storer.readyForMore();
          if (readyForMore !== undefined) {
            await readyForMore;
          }
          storerBytes += jsonBytes;
        }

        latencies.push(performance.now() - txStart);
        rows += generated.rows;
        addOperationCounts(operationCounts, generated.operationCounts);
        fanoutMessages += generated.changes.length * consumers.length;
        if (tx % 10 === 0) {
          sampleMemory();
        }

        if (
          reconnect === undefined &&
          tx > this.#config.reconnectLagTx + 2 &&
          performance.now() - start >= this.#config.durationMs / 2
        ) {
          const catchupTx = Math.max(2, tx - this.#config.reconnectLagTx);
          reconnectCatchupFrom = watermarkFor(catchupTx);
          reconnectStartedAtTx = tx;
          reconnectStartAbsoluteMs = performance.now();
          reconnectStartedAtMs = reconnectStartAbsoluteMs - start;
          reconnectJoinWatermark = generated.watermark;
          const reconnectConfig = ConsumerFactory.configForApplySlot(
            this.#config.consumer,
            consumers.length,
          );
          reconnect = await this.#consumers.create(
            `reconnect-${scenario.name}`,
            reconnectCatchupFrom,
            reconnectConfig.ackDelayMs,
            reconnectConfig,
            false,
          );
          consumers.push(reconnect);
          storer.catchup(reconnect.sub, 'serving');
          forwarder.add(reconnect.sub);
        }

        if (
          reconnect !== undefined &&
          reconnectJoinWatermark !== null &&
          reconnectStartAbsoluteMs !== null &&
          reconnectCaughtUpToJoinMs === null &&
          reconnect.sub.acked >= reconnectJoinWatermark
        ) {
          reconnectCaughtUpToJoinMs =
            performance.now() - reconnectStartAbsoluteMs;
          reconnectCaughtUpToJoinDuringLoad = true;
        }

        nextDue += 1000 / scenario.targetTxPerSec;
        const waitMs = nextDue - performance.now();
        if (waitMs > 0) {
          await sleep(waitMs);
        }
      }
      loadPhaseMs = performance.now() - start;

      const beforeDrain = performance.now();
      await storer.allProcessed();
      const storerDrainMs = performance.now() - beforeDrain;

      if (reconnect !== undefined && latestGeneratedWatermark !== null) {
        const finalCatchupStart = performance.now();
        if (reconnect.sub.acked < latestGeneratedWatermark) {
          await waitForConsumerWatermark(
            reconnect,
            latestGeneratedWatermark,
            this.#config.reconnectFinalCatchupTimeoutMs,
          );
        }
        reconnectFinalCatchupWaitMs = performance.now() - finalCatchupStart;
        if (
          reconnectStartAbsoluteMs !== null &&
          reconnect.sub.acked >= latestGeneratedWatermark
        ) {
          reconnectCaughtUpToFinalMs =
            performance.now() - reconnectStartAbsoluteMs;
        }
        if (
          reconnectJoinWatermark !== null &&
          reconnectStartAbsoluteMs !== null &&
          reconnectCaughtUpToJoinMs === null &&
          reconnect.sub.acked >= reconnectJoinWatermark
        ) {
          reconnectCaughtUpToJoinMs =
            performance.now() - reconnectStartAbsoluteMs;
        }
      }

      if (this.#config.settleMs > 0) {
        await sleep(this.#config.settleMs);
      }
      const elapsedMs = performance.now() - start;
      const memoryEnd = sampleMemory();
      const cpu = process.cpuUsage(cpuStart);
      const stats = consumers.map(consumer => consumer.stats());
      const websocketMessages = sum(stats.map(s => s.transportMessages));
      const websocketBytes = sum(stats.map(s => s.transportBytes));
      const websocketAcks = sum(stats.map(s => s.transportAcks));
      const websocketAckBytes = sum(stats.map(s => s.transportAckBytes));
      const maxAckLagMessages = Math.max(
        0,
        ...stats.map(s => s.maxAckLagMessages),
      );
      const samples = sum(stats.map(s => s.samples));
      const avgAckLagMessages =
        samples === 0
          ? 0
          : sum(stats.map(s => s.totalAckLagMessages)) / samples;
      const avgSubscriberParseMs =
        samples === 0 ? 0 : sum(stats.map(s => s.totalParseMs)) / samples;
      const avgSubscriberApplyMs =
        samples === 0 ? 0 : sum(stats.map(s => s.totalApplyMs)) / samples;
      const txApplySamples = sum(stats.map(s => s.txApplySamples));
      const avgSubscriberTxApplyMs =
        txApplySamples === 0
          ? 0
          : sum(stats.map(s => s.totalTxApplyMs)) / txApplySamples;
      const maxSubscriberTxApplyMs = Math.max(
        0,
        ...stats.map(s => s.maxTxApplyMs),
      );
      const avgSubscriberClientCpuMs =
        samples === 0 ? 0 : sum(stats.map(s => s.totalClientCpuMs)) / samples;
      const reconnectStats = reconnect?.stats();

      summary = {
        name: scenario.name,
        rowsPerTx: scenario.rowsPerTx,
        payload: scenario.payload.size,
        payloadBytes: scenario.payload.bytes,
        workload: workloadName(scenario.workload),
        operationCounts: {...operationCounts},
        targetTxPerSec: scenario.targetTxPerSec,
        durationMs: this.#config.durationMs,
        tx,
        rows,
        storerBytes,
        loadPhaseMs,
        elapsedMs,
        storerDrainMs,
        writeLoopTxPerSec: tx / (loadPhaseMs / 1000),
        writeLoopRowsPerSec: rows / (loadPhaseMs / 1000),
        ingestTxPerSec: tx / (elapsedMs / 1000),
        ingestRowsPerSec: rows / (elapsedMs / 1000),
        fanoutMessages,
        fanoutMessagesPerSec: fanoutMessages / (elapsedMs / 1000),
        websocketMessages,
        websocketMessagesPerSec: websocketMessages / (elapsedMs / 1000),
        websocketBytes,
        websocketBytesPerSec: websocketBytes / (elapsedMs / 1000),
        websocketAcks,
        websocketAckBytes,
        processCpuUserMs: cpu.user / 1000,
        processCpuSystemMs: cpu.system / 1000,
        processCpuTotalMs: (cpu.user + cpu.system) / 1000,
        processCpuUtilization: (cpu.user + cpu.system) / 1000 / elapsedMs,
        startHeapUsedBytes: memoryStart.heapUsed,
        maxHeapUsedBytes,
        endHeapUsedBytes: memoryEnd.heapUsed,
        startRssBytes: memoryStart.rss,
        maxRssBytes,
        endRssBytes: memoryEnd.rss,
        p50TxLatencyMs: percentile(latencies, 50),
        p95TxLatencyMs: percentile(latencies, 95),
        p99TxLatencyMs: percentile(latencies, 99),
        subscriberCount: this.#config.consumer.count,
        reconnectCatchup: reconnect !== undefined,
        reconnectCatchupFrom,
        reconnectMessages: reconnectStats?.processed ?? 0,
        reconnectLagTx: this.#config.reconnectLagTx,
        reconnectStartedAtTx,
        reconnectStartedAtMs,
        reconnectJoinWatermark,
        reconnectCaughtUpToJoinMs,
        reconnectCaughtUpToJoinDuringLoad,
        reconnectFinalWatermark: latestGeneratedWatermark,
        reconnectCaughtUpToFinalMs,
        reconnectFinalCatchupWaitMs,
        reconnectFinalAckedWatermark: reconnectStats?.ackedWatermark ?? null,
        reconnectEndLagTx:
          reconnectStats === undefined || latestGeneratedWatermark === null
            ? null
            : watermarkLagTx(
                reconnectStats.ackedWatermark,
                latestGeneratedWatermark,
              ),
        reconnectMaxAckLagMessages: reconnectStats?.maxAckLagMessages ?? null,
        subscriberAckDelayMs: this.#config.consumer.ackDelayMs,
        subscriberApplyMode: this.#config.consumer.applyMode,
        subscriberApplyMessages: this.#config.consumer.applyMessages,
        subscriberApplyLimit: this.#config.consumer.applyLimit,
        subscriberTransportMode: this.#config.consumer.transportMode,
        subscriberTransportAckMode: this.#config.consumer.transportAckMode,
        subscriberTransportBatchMessages:
          this.#config.consumer.transportBatchMessages,
        subscriberProtocolMode: this.#config.consumer.protocolMode,
        subscriberSynchronous: this.#config.consumer.synchronous,
        subscriberWalAutocheckpoint: this.#config.consumer.walAutocheckpoint,
        subscriberClientCpuMicros: this.#config.consumer.clientCpuMicros,
        avgSubscriberParseMs,
        avgSubscriberApplyMs,
        avgSubscriberTxApplyMs,
        maxSubscriberTxApplyMs,
        avgSubscriberClientCpuMs,
        slowSubscriberAckDelayMs: this.#config.consumer.slowAckDelayMs,
        slowSubscriberEvery: this.#config.consumer.slowEvery,
        sourceApply: this.#config.sourceApply,
        forwardFlushBytesThreshold: this.#config.flushBytesThreshold,
        maxAckLagMessages,
        avgAckLagMessages,
      };
    } finally {
      for (const consumer of consumers) {
        consumer.stop();
      }
      await Promise.all(consumers.map(consumer => consumer.done));
      await storer.stop();
      await storerDone;
      replica?.close();
    }
    if (fatalErrors.length > 0) {
      throw fatalErrors[0];
    }
    if (summary === undefined) {
      throw new Error(`Scenario ${scenario.name} did not complete`);
    }
    return summary;
  }

  async #resetChangeDB() {
    await this.#changeDB`DROP SCHEMA IF EXISTS ${this.#changeDB(
      cdcSchema(this.#shard),
    )} CASCADE`;
    await this.#changeDB.begin(tx => setupCDCTables(this.#lc, tx, this.#shard));
    await ensureReplicationConfig(
      this.#lc,
      this.#changeDB,
      {
        replicaVersion: this.#replicaVersion,
        publications: [],
        watermark: this.#replicaVersion,
      },
      this.#shard,
      true,
    );
  }
}

async function waitForConsumerWatermark(
  consumer: LoadConsumer,
  targetWatermark: string,
  timeoutMs: number,
) {
  const deadline = performance.now() + timeoutMs;
  while (consumer.sub.acked < targetWatermark && performance.now() < deadline) {
    await sleep(5);
  }
}

function watermarkLagTx(ackedWatermark: string, finalWatermark: string) {
  return Math.max(
    0,
    Number.parseInt(finalWatermark, 36) - Number.parseInt(ackedWatermark, 36),
  );
}

export function formatOperationCounts(counts: {
  readonly insert: number;
  readonly update: number;
  readonly delete: number;
}) {
  return `i=${counts.insert} u=${counts.update} d=${counts.delete}`;
}

export function formatReconnectSummary(result: ScenarioSummary) {
  if (!result.reconnectCatchup) {
    return '';
  }
  return (
    ` | catchup join ${formatOptionalMs(result.reconnectCaughtUpToJoinMs)}` +
    ` final ${formatOptionalMs(result.reconnectCaughtUpToFinalMs)}` +
    ` end lag ${result.reconnectEndLagTx ?? 'n/a'} tx`
  );
}

function formatOptionalMs(ms: number | null) {
  return ms === null ? 'n/a' : `${ms.toFixed(1)} ms`;
}
