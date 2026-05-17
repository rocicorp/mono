/* oxlint-disable no-console */
import {once} from 'node:events';
import {rmSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import WebSocket, {WebSocketServer} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {StatementRunner} from '../../src/db/statements.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {
  type ChangeTag,
  type WatermarkedChange,
} from '../../src/services/change-streamer/change-streamer-service.ts';
import {
  downstreamSchema,
  type Downstream,
  PROTOCOL_VERSION,
} from '../../src/services/change-streamer/change-streamer.ts';
import {Forwarder} from '../../src/services/change-streamer/forwarder.ts';
import {
  ensureReplicationConfig,
  setupCDCTables,
} from '../../src/services/change-streamer/schema/tables.ts';
import {Storer} from '../../src/services/change-streamer/storer.ts';
import {Subscriber} from '../../src/services/change-streamer/subscriber.ts';
import {ChangeProcessor} from '../../src/services/replicator/change-processor.ts';
import {initReplicationState} from '../../src/services/replicator/schema/replication-state.ts';
import {ThreadWriteWorkerClient} from '../../src/services/replicator/write-worker-client.ts';
import {postgresTypeConfig, type PostgresDB} from '../../src/types/pg.ts';
import {cdcSchema, type ShardID} from '../../src/types/shards.ts';
import {streamIn, streamOutStringified} from '../../src/types/streams.ts';
import {Subscription} from '../../src/types/subscription.ts';
import {makeSchemaChanges, makeTransaction, watermarkFor} from './fixtures.ts';
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
import {describeScenarios, loadScenarios} from './scenarios.ts';
import type {
  ConsumerApplyMode,
  ConsumerConfig,
  ConsumerTransportAckMode,
  ConsumerTransportMode,
  LoadConsumer,
  Scenario,
  ScenarioSummary,
  Summary,
} from './types.ts';

// End-to-end load driver for reviewing storer/changeLog throughput changes.
//
// #5976/#5977: https://github.com/rocicorp/mono/pull/5976
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
const flushBytesThreshold = envInt('ZERO_RM_VS_FLUSH_BYTES', 16 * 1024);
const reconnectLagTx = envInt('ZERO_RM_VS_RECONNECT_LAG_TX', 64);
const applyMode = applyModeFromEnv();
const consumerConfig: ConsumerConfig = {
  count: envInt('ZERO_RM_VS_SUBSCRIBERS', full ? 16 : 4),
  ackDelayMs: envInt('ZERO_RM_VS_ACK_DELAY_MS', 0),
  applyMode,
  applyMessages: applyMode !== 'none',
  transportMode: transportModeFromEnv(),
  transportAckMode: transportAckModeFromEnv(),
  transportBatchMessages: envInt('ZERO_RM_VS_WS_BATCH_MESSAGES', 64),
  clientCpuMicros: envInt('ZERO_RM_VS_CLIENT_CPU_US', 0),
  slowAckDelayMs: envInt('ZERO_RM_VS_SLOW_ACK_DELAY_MS', full ? 2 : 1),
  slowEvery: envInt('ZERO_RM_VS_SLOW_EVERY', full ? 4 : 2),
};

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
        `${result.name}: ${formatRate(result.ingestTxPerSec)} tx/s | ` +
          `${formatRate(result.ingestRowsPerSec)} rows/s | ` +
          `${formatRate(result.fanoutMessagesPerSec)} fanout msg/s | ` +
          `p95 ${result.p95TxLatencyMs.toFixed(3)} ms | ` +
          `vs-tx ${result.avgSubscriberTxApplyMs.toFixed(3)} ms | ` +
          `drain ${result.storerDrainMs.toFixed(1)} ms | ` +
          `max lag ${result.maxAckLagMessages}`,
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

async function runScenario(
  changeDB: PostgresDB,
  scenario: Scenario,
): Promise<ScenarioSummary> {
  cleanupSQLite(sqlitePath);
  const replica = new Database(lc, sqlitePath);
  replica.pragma('journal_mode = WAL');
  const processor = initializeReplica(replica);
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
  let storerBytes = 0;
  let fanoutMessages = 0;
  let unflushedBytes = 0;
  let reconnect: LoadConsumer | undefined;
  let reconnectCatchupFrom: string | null = null;
  let summary: ScenarioSummary | undefined;
  const start = performance.now();
  let nextDue = start;

  try {
    while (performance.now() - start < durationMs) {
      tx++;
      const generated = makeTransaction(
        tx + 1,
        scenario.rowsPerTx,
        scenario.payload,
      );
      const txStart = performance.now();

      for (const change of generated.changes) {
        processor.processMessage(lc, change);
        const watermark = generated.watermark;
        const json = storer.store(watermark, change);
        const entry: WatermarkedChange = [
          watermark,
          change[1].tag as ChangeTag,
          json,
        ];

        unflushedBytes += Buffer.byteLength(json);
        if (unflushedBytes < flushBytesThreshold) {
          forwarder.forward(entry);
        } else {
          await forwarder.forwardWithFlowControl(entry);
          unflushedBytes = 0;
        }
        const readyForMore = storer.readyForMore();
        if (readyForMore !== undefined) {
          await readyForMore;
        }
        storerBytes += Buffer.byteLength(json);
      }

      latencies.push(performance.now() - txStart);
      rows += generated.rows;
      fanoutMessages += generated.changes.length * consumers.length;

      if (
        reconnect === undefined &&
        tx > reconnectLagTx + 2 &&
        performance.now() - start >= durationMs / 2
      ) {
        const catchupTx = Math.max(2, tx - reconnectLagTx);
        reconnectCatchupFrom = watermarkFor(catchupTx);
        reconnect = await makeConsumer(
          `reconnect-${scenario.name}`,
          reconnectCatchupFrom,
          consumerConfig.ackDelayMs,
          consumerConfig,
          false,
        );
        consumers.push(reconnect);
        storer.catchup(reconnect.sub, 'serving');
        forwarder.add(reconnect.sub);
      }

      nextDue += 1000 / scenario.targetTxPerSec;
      const waitMs = nextDue - performance.now();
      if (waitMs > 0) {
        await sleep(waitMs);
      }
    }

    const beforeDrain = performance.now();
    await storer.allProcessed();
    const storerDrainMs = performance.now() - beforeDrain;
    const settleMs = envInt('ZERO_RM_VS_SETTLE_MS', 100);
    if (settleMs > 0) {
      await sleep(settleMs);
    }
    const elapsedMs = performance.now() - start;
    const stats = consumers.map(consumer => consumer.stats());
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

    summary = {
      name: scenario.name,
      rowsPerTx: scenario.rowsPerTx,
      payload: scenario.payload.size,
      payloadBytes: scenario.payload.bytes,
      targetTxPerSec: scenario.targetTxPerSec,
      durationMs,
      tx,
      rows,
      storerBytes,
      elapsedMs,
      storerDrainMs,
      ingestTxPerSec: tx / (elapsedMs / 1000),
      ingestRowsPerSec: rows / (elapsedMs / 1000),
      fanoutMessages,
      fanoutMessagesPerSec: fanoutMessages / (elapsedMs / 1000),
      p50TxLatencyMs: percentile(latencies, 50),
      p95TxLatencyMs: percentile(latencies, 95),
      p99TxLatencyMs: percentile(latencies, 99),
      subscriberCount: consumerConfig.count,
      reconnectCatchup: reconnect !== undefined,
      reconnectCatchupFrom,
      reconnectMessages: reconnect?.stats().processed ?? 0,
      subscriberAckDelayMs: consumerConfig.ackDelayMs,
      subscriberApplyMode: consumerConfig.applyMode,
      subscriberApplyMessages: consumerConfig.applyMessages,
      subscriberTransportMode: consumerConfig.transportMode,
      subscriberTransportAckMode: consumerConfig.transportAckMode,
      subscriberTransportBatchMessages: consumerConfig.transportBatchMessages,
      subscriberClientCpuMicros: consumerConfig.clientCpuMicros,
      avgSubscriberParseMs,
      avgSubscriberApplyMs,
      avgSubscriberTxApplyMs,
      maxSubscriberTxApplyMs,
      avgSubscriberClientCpuMs,
      slowSubscriberAckDelayMs: consumerConfig.slowAckDelayMs,
      slowSubscriberEvery: consumerConfig.slowEvery,
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
    replica.close();
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

function initializeReplica(db: Database): ChangeProcessor {
  initReplicationState(db, ['zero-cache-rm-vs-load'], replicaVersion);
  const processor = new ChangeProcessor(
    new StatementRunner(db),
    'serving',
    (_, err) => {
      throw err;
    },
  );
  processor.processMessage(lc, [
    'begin',
    {tag: 'begin'},
    {commitWatermark: '000000000001'},
  ]);
  for (const change of makeSchemaChanges()) {
    processor.processMessage(lc, ['data', change]);
  }
  processor.processMessage(lc, [
    'commit',
    {tag: 'commit'},
    {watermark: '000000000001'},
  ]);
  return processor;
}

function createConsumers(config: ConsumerConfig): Promise<LoadConsumer[]> {
  return Promise.all(
    Array.from({length: config.count}, (_, i) => {
      const isSlow = config.slowEvery > 0 && (i + 1) % config.slowEvery === 0;
      return makeConsumer(
        `vs-${i}`,
        replicaVersion,
        isSlow ? config.slowAckDelayMs : config.ackDelayMs,
        config,
        true,
      );
    }),
  );
}

async function makeConsumer(
  id: string,
  watermark: string,
  ackDelayMs: number,
  config: ConsumerConfig,
  caughtUp: boolean,
): Promise<LoadConsumer> {
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
  let processor: ChangeProcessor | undefined;
  let worker: ThreadWriteWorkerClient | undefined;
  let consumerSQLitePath: string | undefined;
  if (config.applyMode !== 'none') {
    consumerSQLitePath = `${sqlitePath}-${id}`;
    cleanupSQLite(consumerSQLitePath);
    consumerReplica = new Database(lc, consumerSQLitePath);
    consumerReplica.pragma('journal_mode = WAL2');
    consumerReplica.pragma('synchronous = NORMAL');
    processor = initializeReplica(consumerReplica);
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
        },
        {level: 'error', format: 'text'},
      );
    }
  }

  const workerBatch: ChangeStreamData[] = [];
  const flushWorkerBatch = async () => {
    if (!worker || workerBatch.length === 0) {
      return;
    }
    await worker.processMessages(workerBatch.splice(0));
  };

  const applyChange = async (change: ChangeStreamData | undefined) => {
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
        await worker?.processMessage(change);
        return;
      case 'worker-batch':
        workerBatch.push(change);
        if (
          change[0] === 'commit' ||
          change[0] === 'rollback' ||
          workerBatch.length >= workerBatchMessages
        ) {
          await flushWorkerBatch();
        }
        return;
    }
  };

  const downstream = Subscription.create<string>();
  const sub = new Subscriber(
    PROTOCOL_VERSION,
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
  const transport = await createConsumerTransport(
    downstream,
    config.transportMode,
    config.transportAckMode,
    config.transportBatchMessages,
  );

  const done = (async () => {
    try {
      for await (const message of transport.messages) {
        if (!active) {
          break;
        }
        const parseStart = performance.now();
        const change =
          typeof message === 'string'
            ? parseChangeStreamData(message)
            : downstreamToChangeStreamData(message);
        totalParseMs += performance.now() - parseStart;
        if (change?.[0] === 'begin') {
          txApplyStart = performance.now();
        }
        const applyStart = performance.now();
        await applyChange(change);
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
    } finally {
      if (workerBatch.length > 0) {
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

async function createConsumerTransport(
  downstream: Subscription<string>,
  mode: ConsumerTransportMode,
  ackMode: ConsumerTransportAckMode,
  batchMessages: number,
): Promise<{
  messages: AsyncIterable<string | Downstream>;
  close: () => Promise<void>;
}> {
  switch (mode) {
    case 'in-process':
      return {messages: downstream, close: () => Promise.resolve()};
    case 'websocket': {
      const server = new WebSocketServer({host: '127.0.0.1', port: 0});
      server.on('connection', ws => {
        void streamOutStringified(
          lc,
          downstream,
          ws,
          batchMessages > 1 ? {batch: {maxMessages: batchMessages}} : undefined,
        );
      });
      await once(server, 'listening');
      const address = server.address();
      assert(
        typeof address === 'object' && address !== null,
        'expected websocket server address',
      );
      const ws = new WebSocket(`ws://127.0.0.1:${address.port}`);
      const messages = await streamIn(lc, ws, downstreamSchema, {
        ack: ackMode,
      });
      return {
        messages,
        close: async () => {
          messages.cancel();
          ws.close();
          await new Promise<void>((resolve, reject) => {
            server.close(err => (err ? reject(err) : resolve()));
          });
        },
      };
    }
  }
}

function parseChangeStreamData(message: string): ChangeStreamData | undefined {
  return downstreamToChangeStreamData(BigIntJSON.parse(message) as Downstream);
}

function downstreamToChangeStreamData(
  parsed: Downstream,
): ChangeStreamData | undefined {
  switch (parsed[0]) {
    case 'status':
      return undefined;
    case 'error':
      throw new Error(`subscription error: ${JSON.stringify(parsed[1])}`);
    case 'begin':
    case 'data':
    case 'commit':
    case 'rollback':
      return parsed;
  }
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

function cleanupSQLite(path: string) {
  rmSync(path, {force: true});
  rmSync(`${path}-shm`, {force: true});
  rmSync(`${path}-wal`, {force: true});
  rmSync(`${path}-wal2`, {force: true});
}
