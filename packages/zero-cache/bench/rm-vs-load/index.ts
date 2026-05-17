/* oxlint-disable no-console */
import {rmSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import {PostgreSqlContainer} from '@testcontainers/postgresql';
import postgres from 'postgres';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {StatementRunner} from '../../src/db/statements.ts';
import {
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
import {ChangeProcessor} from '../../src/services/replicator/change-processor.ts';
import {initReplicationState} from '../../src/services/replicator/schema/replication-state.ts';
import {postgresTypeConfig, type PostgresDB} from '../../src/types/pg.ts';
import {cdcSchema, type ShardID} from '../../src/types/shards.ts';
import {Subscription} from '../../src/types/subscription.ts';
import {makeSchemaChanges, makeTransaction, watermarkFor} from './fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  formatRate,
  percentile,
  sleep,
  sum,
  writeJsonSummary,
} from './perf-utils.ts';
import {describeScenarios, loadScenarios} from './scenarios.ts';
import type {
  ConsumerConfig,
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

const full = envFlag('ZERO_RM_VS_FULL');
const durationMs = envInt('ZERO_RM_VS_DURATION_MS', full ? 2_500 : 1_000);
const flushBytesThreshold = envInt('ZERO_RM_VS_FLUSH_BYTES', 16 * 1024);
const reconnectLagTx = envInt('ZERO_RM_VS_RECONNECT_LAG_TX', 64);
const consumerConfig: ConsumerConfig = {
  count: envInt('ZERO_RM_VS_SUBSCRIBERS', full ? 16 : 4),
  ackDelayMs: envInt('ZERO_RM_VS_ACK_DELAY_MS', 0),
  slowAckDelayMs: envInt('ZERO_RM_VS_SLOW_ACK_DELAY_MS', full ? 2 : 1),
  slowEvery: envInt('ZERO_RM_VS_SLOW_EVERY', full ? 4 : 2),
};

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
  const consumers = createConsumers(consumerConfig);
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
        reconnect = makeConsumer(
          `reconnect-${scenario.name}`,
          reconnectCatchupFrom,
          consumerConfig.ackDelayMs,
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

function createConsumers(config: ConsumerConfig): LoadConsumer[] {
  return Array.from({length: config.count}, (_, i) => {
    const isSlow = config.slowEvery > 0 && (i + 1) % config.slowEvery === 0;
    return makeConsumer(
      `vs-${i}`,
      replicaVersion,
      isSlow ? config.slowAckDelayMs : config.ackDelayMs,
      true,
    );
  });
}

function makeConsumer(
  id: string,
  watermark: string,
  ackDelayMs: number,
  caughtUp: boolean,
): LoadConsumer {
  let active = true;
  let processed = 0;
  let maxAckLagMessages = 0;
  let totalAckLagMessages = 0;
  let samples = 0;
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

  const done = (async () => {
    for await (const _message of downstream) {
      if (!active) {
        break;
      }
      if (ackDelayMs > 0) {
        await sleep(ackDelayMs);
      }
      processed++;
      const lag = sub.numPending;
      maxAckLagMessages = Math.max(maxAckLagMessages, lag);
      totalAckLagMessages += lag;
      samples++;
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
      samples,
    }),
  };
}

function cleanupSQLite(path: string) {
  rmSync(path, {force: true});
  rmSync(`${path}-shm`, {force: true});
  rmSync(`${path}-wal`, {force: true});
  rmSync(`${path}-wal2`, {force: true});
}
