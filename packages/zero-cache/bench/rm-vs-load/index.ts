/* oxlint-disable no-console */
import {performance} from 'node:perf_hooks';
import {Worker} from 'node:worker_threads';
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {
  FORWARDER_FLOW_CONTROL_BYTES_THRESHOLD,
  type ChangeTag,
  type WatermarkedChange,
} from '../../src/services/change-streamer/change-streamer-service.ts';
import {PROTOCOL_VERSION} from '../../src/services/change-streamer/change-streamer.ts';
import {Forwarder} from '../../src/services/change-streamer/forwarder.ts';
import {
  ensureReplicationConfig,
  setupCDCTables,
} from '../../src/services/change-streamer/schema/tables.ts';
import {Storer} from '../../src/services/change-streamer/storer.ts';
import {Subscriber} from '../../src/services/change-streamer/subscriber.ts';
import {WorkerMessageBatcher} from '../../src/services/replicator/worker-message-batcher.ts';
import {ThreadWriteWorkerClient} from '../../src/services/replicator/write-worker-client.ts';
import {postgresTypeConfig, type PostgresDB} from '../../src/types/pg.ts';
import {cdcSchema, type ShardID} from '../../src/types/shards.ts';
import type {StringifiedStreamPayload} from '../../src/types/streams.ts';
import {Subscription} from '../../src/types/subscription.ts';
import {SERVING_REPLICA_WAL_AUTOCHECKPOINT_PAGES} from '../../src/workers/replicator.ts';
import {emptyOperationCounts, watermarkFor} from './fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  envString,
  formatRate,
  percentile,
  sleep,
  sum,
  writeJsonSummary,
} from './perf-utils.ts';
import {
  createConsumerTransport,
  createConsumerWebSocketServer,
  mergeTransportStats,
  parseTransportBatch,
} from './protocol.ts';
import {cleanupSQLite, initializeReplica} from './replica.ts';
import {describeScenarios, loadScenarios} from './scenarios.ts';
import type {
  ConsumerApplyMode,
  ConsumerConfig,
  ConsumerProtocolMode,
  ConsumerRuntime,
  ConsumerTransportAckMode,
  ConsumerTransportMode,
  ConsumerWorkerData,
  LoadConsumer,
  LoadConsumerStats,
  Scenario,
  ScenarioSummary,
  Summary,
} from './types.ts';
import {
  addOperationCounts,
  createTransactionGenerator,
  workloadName,
} from './workloads.ts';

// End-to-end load driver for reviewing storer/changeLog throughput changes.
//
// This harness exists to keep storer performance work anchored to the same
// production-shaped bottleneck: one replication-manager, fanout to live
// view-syncers, optional reconnect catchup, SQLite apply, JSON serialization,
// and Postgres writes sharing the same event loop.
//
//   one RM writes the stream
//            |
//            v
//       Storer/changeLog  ---->  N live view-syncers
//            |
//            `---------------->  optional reconnect catchup

const lc = createSilentLogContext();
const shard: ShardID = {appID: 'bench', shardNum: 0};
const replicaVersion = '000000000000';
const sqlitePath = `/tmp/zero-cache-rm-vs-load-${process.pid}.db`;
let cpuSink = 0;
const workerBatchMessages = envInt('ZERO_RM_VS_WORKER_BATCH_MESSAGES', 64);

const full = envFlag('ZERO_RM_VS_FULL');
const durationMs = envInt('ZERO_RM_VS_DURATION_MS', full ? 2_500 : 1_000);
const flushBytesThreshold = envInt(
  'ZERO_RM_VS_FLUSH_BYTES',
  FORWARDER_FLOW_CONTROL_BYTES_THRESHOLD,
);
const reconnectLagTx = envInt('ZERO_RM_VS_RECONNECT_LAG_TX', 64);
const sourceApply = envFlag('ZERO_RM_VS_SOURCE_APPLY');
const reconnectFinalCatchupTimeoutMs = envInt(
  'ZERO_RM_VS_FINAL_CATCHUP_TIMEOUT_MS',
  full ? 15_000 : 5_000,
);
const applyMode = applyModeFromEnv();
const consumerConfig: ConsumerConfig = {
  count: envInt('ZERO_RM_VS_SUBSCRIBERS', full ? 16 : 4),
  ackDelayMs: envInt('ZERO_RM_VS_ACK_DELAY_MS', 0),
  runtime: runtimeFromEnv(),
  applyMode,
  applyMessages: applyMode !== 'none',
  applyLimit: optionalEnvInt('ZERO_RM_VS_APPLY_LIMIT'),
  transportMode: transportModeFromEnv(),
  transportAckMode: transportAckModeFromEnv(),
  transportBatchMessages: envInt('ZERO_RM_VS_WS_BATCH_MESSAGES', 64),
  protocolMode: protocolModeFromEnv(),
  synchronous: synchronousFromEnv(),
  walAutocheckpoint: optionalEnvInt(
    'ZERO_RM_VS_WAL_AUTOCHECKPOINT',
    SERVING_REPLICA_WAL_AUTOCHECKPOINT_PAGES,
  ),
  clientCpuMicros: envInt('ZERO_RM_VS_CLIENT_CPU_US', 0),
  slowAckDelayMs: envInt('ZERO_RM_VS_SLOW_ACK_DELAY_MS', full ? 2 : 1),
  slowEvery: envInt('ZERO_RM_VS_SLOW_EVERY', full ? 4 : 2),
};

const CONSUMER_WORKER_URL = new URL('./consumer-worker.ts', import.meta.url);

function applyModeFromEnv(): ConsumerApplyMode {
  const mode = envString('ZERO_RM_VS_APPLY_MODE');
  if (mode === undefined) {
    return envFlag('ZERO_RM_VS_APPLY_CLIENTS') ? 'direct' : 'none';
  }
  switch (mode) {
    case 'none':
    case 'direct':
    case 'worker-message':
    case 'worker-batch':
      return mode;
    default:
      throw new Error(
        `Invalid ZERO_RM_VS_APPLY_MODE=${mode}; expected ` +
          'none, direct, worker-message, or worker-batch',
      );
  }
}

function runtimeFromEnv(): ConsumerRuntime {
  const runtime = envString('ZERO_RM_VS_CONSUMER_RUNTIME') ?? 'inline';
  switch (runtime) {
    case 'inline':
    case 'worker':
      return runtime;
    default:
      throw new Error(
        `Invalid ZERO_RM_VS_CONSUMER_RUNTIME=${runtime}; expected ` +
          'inline or worker',
      );
  }
}

function optionalEnvInt(
  name: string,
  defaultValue?: number,
): number | undefined {
  const value = envString(name);
  if (value === undefined || value === '') {
    return defaultValue;
  }
  return envInt(name, 0);
}

function synchronousFromEnv(): 'OFF' | 'NORMAL' | 'FULL' | undefined {
  const value = envString('ZERO_RM_VS_SQLITE_SYNCHRONOUS');
  switch (value) {
    case undefined:
    case '':
      return undefined;
    case 'OFF':
    case 'NORMAL':
    case 'FULL':
      return value;
    default:
      throw new Error(`Invalid ZERO_RM_VS_SQLITE_SYNCHRONOUS ${value}`);
  }
}

function transportAckModeFromEnv(): ConsumerTransportAckMode {
  const mode = envString('ZERO_RM_VS_WS_ACK');
  if (mode === undefined) {
    return 'per-message';
  }
  switch (mode) {
    case 'per-message':
    case 'cumulative':
      return mode;
    default:
      throw new Error(
        `Invalid ZERO_RM_VS_WS_ACK=${mode}; expected per-message or cumulative`,
      );
  }
}

function transportModeFromEnv(): ConsumerTransportMode {
  const mode = envString('ZERO_RM_VS_TRANSPORT');
  if (mode === undefined) {
    return 'in-process';
  }
  switch (mode) {
    case 'in-process':
    case 'websocket':
      return mode;
    default:
      throw new Error(
        `Invalid ZERO_RM_VS_TRANSPORT=${mode}; expected in-process or websocket`,
      );
  }
}

function protocolModeFromEnv(): ConsumerProtocolMode {
  const mode = envString('ZERO_RM_VS_PROTOCOL') ?? 'v6';
  switch (mode) {
    case 'v6':
      return mode;
    default:
      throw new Error(`Invalid ZERO_RM_VS_PROTOCOL=${mode}; expected v6`);
  }
}

function protocolVersionForMode(_mode: ConsumerProtocolMode): number {
  return PROTOCOL_VERSION;
}

const scenarios = loadScenarios(full);
console.log(`scenario bytes: ${describeScenarios(scenarios)}`);
const container = await new PostgreSqlContainer(
  process.env.ZERO_RM_VS_PG_IMAGE ?? 'postgres:17',
).start();

try {
  const changeDB = postgres(container.getConnectionUri(), {
    ...postgresTypeConfig({sendStringAsJson: true}),
    onnotice: () => {},
  });
  try {
    const summaries: ScenarioSummary[] = [];
    for (const scenario of scenarios) {
      summaries.push(await runScenario(changeDB, scenario));
    }

    const summary: Summary = {
      name: 'zero-cache-rm-vs-load',
      mode: full ? 'full' : 'smoke',
      generatedAt: new Date().toISOString(),
      rmCount: 1,
      viewSyncerCount: consumerConfig.count,
      scenarios: summaries,
    };

    for (const result of summary.scenarios) {
      console.log(
        `${result.name}: ${formatRate(result.writeLoopTxPerSec)} tx/s load | ` +
          `${formatRate(result.writeLoopRowsPerSec)} rows/s load | ` +
          `ops ${formatOperationCounts(result.operationCounts)} | ` +
          `${formatRate(result.fanoutMessagesPerSec)} fanout msg/s | ` +
          `p95 ${result.p95TxLatencyMs.toFixed(3)} ms | ` +
          `vs-tx ${result.avgSubscriberTxApplyMs.toFixed(3)} ms | ` +
          `drain ${result.storerDrainMs.toFixed(1)} ms | ` +
          `max lag ${result.maxAckLagMessages}` +
          formatReconnectSummary(result),
      );
    }
    console.log(JSON.stringify(summary));

    await writeJsonSummary(
      summary,
      argValue('out') ?? process.env.ZERO_RM_VS_OUT,
    );
  } finally {
    await changeDB.end();
  }
} finally {
  await container.stop();
  cleanupSQLite(sqlitePath);
}

function formatOperationCounts(counts: {
  readonly insert: number;
  readonly update: number;
  readonly delete: number;
}) {
  return `i=${counts.insert} u=${counts.update} d=${counts.delete}`;
}

async function runScenario(
  changeDB: PostgresDB,
  scenario: Scenario,
): Promise<ScenarioSummary> {
  cleanupSQLite(sqlitePath);
  let replica: Database | undefined;
  let processor: ReturnType<typeof initializeReplica> | undefined;
  if (sourceApply) {
    replica = new Database(lc, sqlitePath);
    replica.pragma('journal_mode = WAL');
    processor = initializeReplica(lc, replica, replicaVersion);
  }
  const forwarder = new Forwarder(lc, {
    flowControlConsensusPaddingSeconds: 0.001,
  });
  const consumers = await createConsumers(consumerConfig);
  for (const {sub} of consumers) {
    forwarder.add(sub);
  }

  await resetChangeDB(changeDB);
  const fatalErrors: Error[] = [];
  const storer = new Storer(
    lc,
    shard,
    `rm-vs-load-${scenario.name}`,
    'bench-rm:12345',
    'ws',
    changeDB,
    replicaVersion,
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
    while (performance.now() - start < durationMs) {
      tx++;
      const generated = generator.next(tx + 1);
      latestGeneratedWatermark = generated.watermark;
      const txStart = performance.now();

      for (const change of generated.changes) {
        processor?.processMessage(lc, change);
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
          unflushedBytes >= flushBytesThreshold && change[0] === 'commit';
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
        tx > reconnectLagTx + 2 &&
        performance.now() - start >= durationMs / 2
      ) {
        const catchupTx = Math.max(2, tx - reconnectLagTx);
        reconnectCatchupFrom = watermarkFor(catchupTx);
        reconnectStartedAtTx = tx;
        reconnectStartAbsoluteMs = performance.now();
        reconnectStartedAtMs = reconnectStartAbsoluteMs - start;
        reconnectJoinWatermark = generated.watermark;
        const reconnectConfig = configForApplySlot(
          consumerConfig,
          consumers.length,
        );
        reconnect = await makeConsumer(
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
          reconnectFinalCatchupTimeoutMs,
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

    const settleMs = envInt('ZERO_RM_VS_SETTLE_MS', 100);
    if (settleMs > 0) {
      await sleep(settleMs);
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
      samples === 0 ? 0 : sum(stats.map(s => s.totalAckLagMessages)) / samples;
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
      durationMs,
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
      subscriberCount: consumerConfig.count,
      reconnectCatchup: reconnect !== undefined,
      reconnectCatchupFrom,
      reconnectMessages: reconnectStats?.processed ?? 0,
      reconnectLagTx,
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
      subscriberAckDelayMs: consumerConfig.ackDelayMs,
      subscriberApplyMode: consumerConfig.applyMode,
      subscriberApplyMessages: consumerConfig.applyMessages,
      subscriberApplyLimit: consumerConfig.applyLimit,
      subscriberTransportMode: consumerConfig.transportMode,
      subscriberTransportAckMode: consumerConfig.transportAckMode,
      subscriberTransportBatchMessages: consumerConfig.transportBatchMessages,
      subscriberProtocolMode: consumerConfig.protocolMode,
      subscriberSynchronous: consumerConfig.synchronous,
      subscriberWalAutocheckpoint: consumerConfig.walAutocheckpoint,
      subscriberClientCpuMicros: consumerConfig.clientCpuMicros,
      avgSubscriberParseMs,
      avgSubscriberApplyMs,
      avgSubscriberTxApplyMs,
      maxSubscriberTxApplyMs,
      avgSubscriberClientCpuMs,
      slowSubscriberAckDelayMs: consumerConfig.slowAckDelayMs,
      slowSubscriberEvery: consumerConfig.slowEvery,
      sourceApply,
      forwardFlushBytesThreshold: flushBytesThreshold,
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

async function resetChangeDB(db: PostgresDB) {
  await db`DROP SCHEMA IF EXISTS ${db(cdcSchema(shard))} CASCADE`;
  await db.begin(tx => setupCDCTables(lc, tx, shard));
  await ensureReplicationConfig(
    lc,
    db,
    {
      replicaVersion,
      publications: [],
      watermark: replicaVersion,
    },
    shard,
    true,
  );
}

function createConsumers(config: ConsumerConfig): Promise<LoadConsumer[]> {
  return Promise.all(
    Array.from({length: config.count}, (_, i) => {
      const isSlow = config.slowEvery > 0 && (i + 1) % config.slowEvery === 0;
      const consumerConfig = configForApplySlot(config, i);
      return makeConsumer(
        `vs-${i}`,
        replicaVersion,
        isSlow ? consumerConfig.slowAckDelayMs : consumerConfig.ackDelayMs,
        consumerConfig,
        true,
      );
    }),
  );
}

function configForApplySlot(
  config: ConsumerConfig,
  index: number,
): ConsumerConfig {
  if (config.applyLimit === undefined || index < config.applyLimit) {
    return config;
  }
  return {
    ...config,
    applyMode: 'none',
    applyMessages: false,
  };
}

async function makeConsumer(
  id: string,
  watermark: string,
  ackDelayMs: number,
  config: ConsumerConfig,
  caughtUp: boolean,
): Promise<LoadConsumer> {
  const downstream = Subscription.create<StringifiedStreamPayload>();
  const sub = new Subscriber(
    protocolVersionForMode(config.protocolMode),
    id,
    watermark,
    downstream,
    () => ({
      tag: 'status',
    }),
  );
  if (caughtUp) {
    sub.setCaughtUp();
  }
  if (config.runtime === 'worker') {
    return makeWorkerRuntimeConsumer(id, sub, downstream, ackDelayMs, config);
  }

  let active = true;
  let processed = 0;
  let maxAckLagMessages = 0;
  let totalAckLagMessages = 0;
  let totalParseMs = 0;
  let totalApplyMs = 0;
  let totalTxApplyMs = 0;
  let maxTxApplyMs = 0;
  let txApplySamples = 0;
  let txApplyStart: number | undefined;
  let totalClientCpuMs = 0;
  let samples = 0;
  let consumerReplica: Database | undefined;
  let processor: ReturnType<typeof initializeReplica> | undefined;
  let worker: ThreadWriteWorkerClient | undefined;
  let consumerSQLitePath: string | undefined;
  if (config.applyMode !== 'none') {
    consumerSQLitePath = `${sqlitePath}-${id}`;
    cleanupSQLite(consumerSQLitePath);
    consumerReplica = new Database(lc, consumerSQLitePath);
    consumerReplica.pragma('journal_mode = WAL2');
    consumerReplica.pragma(`synchronous = ${config.synchronous ?? 'NORMAL'}`);
    processor = initializeReplica(lc, consumerReplica, replicaVersion);
    if (config.applyMode !== 'direct') {
      consumerReplica.close();
      consumerReplica = undefined;
      processor = undefined;
      worker = new ThreadWriteWorkerClient();
      await worker.init(
        consumerSQLitePath,
        'serving',
        {
          busyTimeout: 30000,
          analysisLimit: 1000,
          synchronous: config.synchronous,
          walAutocheckpoint: config.walAutocheckpoint,
        },
        {level: 'error', format: 'text'},
      );
    }
  }

  const workerBatcher = worker
    ? new WorkerMessageBatcher(worker, workerBatchMessages, {
        flushOnCommit: false,
      })
    : undefined;

  const applyChange = (
    change: ChangeStreamData | undefined,
  ): Promise<unknown> | undefined => {
    if (!change) {
      return;
    }
    switch (config.applyMode) {
      case 'none':
        return;
      case 'direct':
        processor?.processMessage(lc, change);
        return;
      case 'worker-message':
        return worker?.processMessage(change);
      case 'worker-batch':
        return workerBatcher?.push(change);
    }
  };

  const transport = await createConsumerTransport(
    lc,
    downstream,
    config.transportMode,
    config.transportAckMode,
    config.transportBatchMessages,
    config.protocolMode,
  );

  const done = (async () => {
    try {
      for await (const batch of transport.messages) {
        if (!active) {
          break;
        }
        const parseStart = performance.now();
        const changes = parseTransportBatch(batch);
        totalParseMs += performance.now() - parseStart;

        for (const change of changes) {
          if (change?.[0] === 'begin') {
            txApplyStart = performance.now();
          }
          const applyStart = performance.now();
          const applyResult = applyChange(change);
          if (applyResult) {
            await applyResult;
          }
          totalApplyMs += performance.now() - applyStart;
          if (change?.[0] === 'commit' || change?.[0] === 'rollback') {
            if (txApplyStart !== undefined) {
              const elapsed = performance.now() - txApplyStart;
              totalTxApplyMs += elapsed;
              maxTxApplyMs = Math.max(maxTxApplyMs, elapsed);
              txApplySamples++;
              txApplyStart = undefined;
            }
          }
          const cpuStart = performance.now();
          burnCpu(config.clientCpuMicros);
          totalClientCpuMs += performance.now() - cpuStart;
          if (ackDelayMs > 0) {
            await sleep(ackDelayMs);
          }
          processed++;
          const lag = sub.numPending;
          maxAckLagMessages = Math.max(maxAckLagMessages, lag);
          totalAckLagMessages += lag;
          samples++;
        }
        const flushStart = performance.now();
        const flushResult = workerBatcher?.flush();
        if (flushResult) {
          await flushResult;
        }
        totalApplyMs += performance.now() - flushStart;
      }
    } finally {
      if (workerBatcher !== undefined && workerBatcher.size > 0) {
        workerBatcher.clear();
        worker?.abort();
      }
      await transport.close();
      await worker?.stop();
      consumerReplica?.close();
      if (consumerSQLitePath) {
        cleanupSQLite(consumerSQLitePath);
      }
    }
  })();

  return {
    sub,
    done,
    stop: () => {
      active = false;
      sub.close();
    },
    stats: () => ({
      processed,
      ackedWatermark: sub.acked,
      watermark: sub.watermark,
      pending: sub.numPending,
      transportMessages: transport.stats().messages,
      transportBytes: transport.stats().bytes,
      transportAcks: transport.stats().acks,
      transportAckBytes: transport.stats().ackBytes,
      maxAckLagMessages,
      totalAckLagMessages,
      totalParseMs,
      totalApplyMs,
      totalTxApplyMs,
      maxTxApplyMs,
      txApplySamples,
      totalClientCpuMs,
      samples,
    }),
  };
}

async function makeWorkerRuntimeConsumer(
  id: string,
  sub: Subscriber,
  downstream: Subscription<StringifiedStreamPayload>,
  ackDelayMs: number,
  config: ConsumerConfig,
): Promise<LoadConsumer> {
  if (config.transportMode !== 'websocket') {
    throw new Error(
      'ZERO_RM_VS_CONSUMER_RUNTIME=worker requires ' +
        'ZERO_RM_VS_TRANSPORT=websocket',
    );
  }

  const server = await createConsumerWebSocketServer(
    lc,
    downstream,
    config.transportBatchMessages,
    config.protocolMode,
  );
  const consumerSQLitePath =
    config.applyMode === 'none' ? undefined : `${sqlitePath}-${id}`;
  const worker = new Worker(CONSUMER_WORKER_URL, {
    workerData: {
      id,
      url: server.url,
      sqlitePath: consumerSQLitePath,
      replicaVersion,
      protocolMode: config.protocolMode,
      transportAckMode: config.transportAckMode,
      applyMode: config.applyMode,
      synchronous: config.synchronous,
      walAutocheckpoint: config.walAutocheckpoint,
      workerBatchMessages,
      clientCpuMicros: config.clientCpuMicros,
      ackDelayMs,
    } satisfies ConsumerWorkerData,
  });

  let latestWorkerStats = emptyConsumerStats();
  let maxAckLagMessages = 0;
  let totalAckLagMessages = 0;
  let ackLagSamples = 0;
  let lastObservedSamples = 0;
  let complete = false;

  const observeAckLag = () => {
    const lag = sub.numPending;
    maxAckLagMessages = Math.max(maxAckLagMessages, lag);
    const delta = Math.max(0, latestWorkerStats.samples - lastObservedSamples);
    if (delta > 0) {
      totalAckLagMessages += lag * delta;
      ackLagSamples += delta;
      lastObservedSamples = latestWorkerStats.samples;
    }
  };
  const ackLagInterval = setInterval(observeAckLag, 10);
  ackLagInterval.unref();

  let resolveReady!: () => void;
  let rejectReady!: (err: Error) => void;
  const ready = new Promise<void>((resolve, reject) => {
    resolveReady = resolve;
    rejectReady = reject;
  });

  let resolveDone!: () => void;
  let rejectDone!: (err: Error) => void;
  const workerDone = new Promise<void>((resolve, reject) => {
    resolveDone = resolve;
    rejectDone = reject;
  });

  const fail = (err: Error) => {
    rejectReady(err);
    rejectDone(err);
  };

  worker.on('message', msg => {
    if (typeof msg !== 'object' || msg === null || !('type' in msg)) {
      return;
    }
    switch (msg.type) {
      case 'ready':
        resolveReady();
        break;
      case 'stats':
        latestWorkerStats = msg.stats as LoadConsumerStats;
        observeAckLag();
        break;
      case 'done':
        latestWorkerStats = msg.stats as LoadConsumerStats;
        observeAckLag();
        complete = true;
        resolveDone();
        break;
      case 'error': {
        const error = msg.error as {message?: unknown; stack?: unknown};
        const message =
          typeof error.message === 'string'
            ? error.message
            : 'consumer worker error';
        const err = new Error(message);
        if (typeof error.stack === 'string') {
          err.stack = error.stack;
        }
        fail(err);
        break;
      }
    }
  });
  worker.on('error', fail);
  worker.on('exit', code => {
    if (!complete && code !== 0) {
      fail(new Error(`consumer worker ${id} exited with code ${code}`));
    }
  });

  await ready;

  const done = workerDone.finally(async () => {
    clearInterval(ackLagInterval);
    await server.close();
    await worker.terminate();
  });

  return {
    sub,
    done,
    stop: () => {
      sub.close();
      worker.postMessage({type: 'stop'});
    },
    stats: () => {
      observeAckLag();
      const serverStats = server.stats();
      const transportStats = mergeTransportStats(serverStats, {
        messages: 0,
        bytes: 0,
        acks: latestWorkerStats.transportAcks,
        ackBytes: latestWorkerStats.transportAckBytes,
      });
      return {
        ...latestWorkerStats,
        ackedWatermark: sub.acked,
        watermark: sub.watermark,
        pending: sub.numPending,
        transportMessages: transportStats.messages,
        transportBytes: transportStats.bytes,
        transportAcks: transportStats.acks,
        transportAckBytes: transportStats.ackBytes,
        maxAckLagMessages,
        totalAckLagMessages,
        samples: Math.max(latestWorkerStats.samples, ackLagSamples),
      };
    },
  };
}

function emptyConsumerStats(): LoadConsumerStats {
  return {
    processed: 0,
    ackedWatermark: '',
    watermark: '',
    pending: 0,
    transportMessages: 0,
    transportBytes: 0,
    transportAcks: 0,
    transportAckBytes: 0,
    maxAckLagMessages: 0,
    totalAckLagMessages: 0,
    totalParseMs: 0,
    totalApplyMs: 0,
    totalTxApplyMs: 0,
    maxTxApplyMs: 0,
    txApplySamples: 0,
    totalClientCpuMs: 0,
    samples: 0,
  };
}

function burnCpu(micros: number) {
  if (micros <= 0) {
    return;
  }

  const end = performance.now() + micros / 1000;
  let value = cpuSink;
  while (performance.now() < end) {
    value = (value + Math.imul(value + 1, 31)) % 1_000_003;
  }
  cpuSink = value;
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

function formatReconnectSummary(result: ScenarioSummary) {
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
