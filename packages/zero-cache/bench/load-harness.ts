/* oxlint-disable no-console */
import {rmSync} from 'node:fs';
import {performance} from 'node:perf_hooks';
import {fileURLToPath} from 'node:url';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import {Database} from '../../zqlite/src/db.ts';
import {StatementRunner} from '../src/db/statements.ts';
import {PROTOCOL_VERSION} from '../src/services/change-streamer/change-streamer.ts';
import {Forwarder} from '../src/services/change-streamer/forwarder.ts';
import {Subscriber} from '../src/services/change-streamer/subscriber.ts';
import {ChangeProcessor} from '../src/services/replicator/change-processor.ts';
import {initReplicationState} from '../src/services/replicator/schema/replication-state.ts';
import {Subscription} from '../src/types/subscription.ts';
import {
  makeSchemaChanges,
  makeTransaction,
  smokePayloadProfiles,
  loadPayloadProfiles,
  type PayloadProfile,
} from './load-fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  envNumber,
  formatBytes,
  formatRate,
  percentile,
  sleep,
  sum,
  writeJsonSummary,
} from './perf-utils.ts';

type ConsumerConfig = {
  readonly count: number;
  readonly ackDelayMs: number;
  readonly slowAckDelayMs: number;
  readonly slowEvery: number;
};

type ScenarioSummary = {
  readonly rowsPerTx: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly targetTxPerSec: number;
  readonly durationMs: number;
  readonly tx: number;
  readonly rows: number;
  readonly inputBytes: number;
  readonly ingestTxPerSec: number;
  readonly ingestRowsPerSec: number;
  readonly fanoutMessages: number;
  readonly fanoutMessagesPerSec: number;
  readonly p50IngestLatencyMs: number;
  readonly p95IngestLatencyMs: number;
  readonly p99IngestLatencyMs: number;
  readonly subscriberCount: number;
  readonly subscriberAckDelayMs: number;
  readonly slowSubscriberAckDelayMs: number;
  readonly slowSubscriberEvery: number;
  readonly maxAckLagMessages: number;
  readonly avgAckLagMessages: number;
  readonly catchupMessages: number;
};

type Summary = {
  readonly name: 'zero-cache-load-harness';
  readonly mode: 'smoke' | 'full';
  readonly generatedAt: string;
  readonly scenarios: readonly ScenarioSummary[];
};

type LoadConsumer = {
  readonly sub: Subscriber;
  readonly stop: () => void;
  readonly done: Promise<void>;
  readonly stats: () => {
    readonly processed: number;
    readonly maxAckLagMessages: number;
    readonly totalAckLagMessages: number;
    readonly samples: number;
  };
};

const lc = createSilentLogContext();

function makeConsumer(
  id: string,
  watermark: string,
  ackDelayMs: number,
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
  sub.setCaughtUp();

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

function createConsumers(config: ConsumerConfig): LoadConsumer[] {
  return Array.from({length: config.count}, (_, i) => {
    const isSlow = config.slowEvery > 0 && (i + 1) % config.slowEvery === 0;
    return makeConsumer(
      `sub-${i}`,
      '000000000000',
      isSlow ? config.slowAckDelayMs : config.ackDelayMs,
    );
  });
}

function initializeReplica(db: Database): ChangeProcessor {
  initReplicationState(db, ['zero-cache-load-harness'], '000000000000');
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

async function runScenario(
  rowsPerTx: number,
  payload: PayloadProfile,
  durationMs: number,
  targetTxPerSec: number,
  consumerConfig: ConsumerConfig,
  dbFile: string,
): Promise<ScenarioSummary> {
  rmSync(dbFile, {force: true});
  rmSync(`${dbFile}-shm`, {force: true});
  rmSync(`${dbFile}-wal`, {force: true});
  rmSync(`${dbFile}-wal2`, {force: true});

  const db = new Database(lc, dbFile);
  db.pragma('journal_mode = WAL');
  const processor = initializeReplica(db);
  const forwarder = new Forwarder(lc, {
    flowControlConsensusPaddingSeconds: 0.001,
  });
  const consumers = createConsumers(consumerConfig);
  for (const {sub} of consumers) {
    forwarder.add(sub);
  }

  const latencies: number[] = [];
  let tx = 0;
  let rows = 0;
  let inputBytes = 0;
  let fanoutMessages = 0;
  let catchupMessages = 0;
  let unflushedBytes = 0;
  const flushBytesThreshold = envInt('ZERO_LOAD_FLUSH_BYTES', 16 * 1024);
  const start = performance.now();
  let nextDue = start;

  while (performance.now() - start < durationMs) {
    tx++;
    const generated = makeTransaction(tx + 1, rowsPerTx, payload);
    const txStart = performance.now();
    for (const change of generated.changes) {
      processor.processMessage(lc, change);
    }
    for (const entry of generated.watermarked) {
      unflushedBytes += Buffer.byteLength(entry[2]);
      if (unflushedBytes < flushBytesThreshold) {
        forwarder.forward(entry);
      } else {
        await forwarder.forwardWithFlowControl(entry);
        unflushedBytes = 0;
      }
    }
    latencies.push(performance.now() - txStart);
    rows += generated.rows;
    inputBytes += generated.bytes;
    fanoutMessages += generated.watermarked.length * consumers.length;

    nextDue += 1000 / targetTxPerSec;
    const waitMs = nextDue - performance.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }

  const catchup = makeConsumer('reconnect-catchup', '000000000000', 0);
  for (let i = Math.max(1, tx - 4); i <= tx; i++) {
    const generated = makeTransaction(i + 1, rowsPerTx, payload);
    for (const entry of generated.watermarked) {
      await catchup.sub.catchup(entry);
      catchupMessages++;
    }
  }
  catchup.stop();
  await catchup.done;

  for (const consumer of consumers) {
    consumer.stop();
  }
  await Promise.all(consumers.map(({done}) => done));
  db.close();

  const elapsedMs = performance.now() - start;
  const stats = consumers.map(consumer => consumer.stats());
  const maxAckLagMessages = Math.max(
    0,
    ...stats.map(({maxAckLagMessages}) => maxAckLagMessages),
  );
  const totalLag = sum(
    stats.map(({totalAckLagMessages}) => totalAckLagMessages),
  );
  const samples = sum(stats.map(({samples}) => samples));

  return {
    rowsPerTx,
    payload: payload.size,
    payloadBytes: payload.bytes,
    targetTxPerSec,
    durationMs: elapsedMs,
    tx,
    rows,
    inputBytes,
    ingestTxPerSec: (tx * 1000) / elapsedMs,
    ingestRowsPerSec: (rows * 1000) / elapsedMs,
    fanoutMessages,
    fanoutMessagesPerSec: (fanoutMessages * 1000) / elapsedMs,
    p50IngestLatencyMs: percentile(latencies, 50),
    p95IngestLatencyMs: percentile(latencies, 95),
    p99IngestLatencyMs: percentile(latencies, 99),
    subscriberCount: consumers.length,
    subscriberAckDelayMs: consumerConfig.ackDelayMs,
    slowSubscriberAckDelayMs: consumerConfig.slowAckDelayMs,
    slowSubscriberEvery: consumerConfig.slowEvery,
    maxAckLagMessages,
    avgAckLagMessages: samples === 0 ? 0 : totalLag / samples,
    catchupMessages,
  };
}

function printScenario(summary: ScenarioSummary) {
  console.log(
    [
      `${summary.rowsPerTx} rows/tx`,
      `${summary.payload} (${formatBytes(summary.payloadBytes)})`,
      `${formatRate(summary.ingestTxPerSec)} tx/s`,
      `${formatRate(summary.ingestRowsPerSec)} rows/s`,
      `${formatRate(summary.fanoutMessagesPerSec)} fanout msg/s`,
      `p95 ${summary.p95IngestLatencyMs.toFixed(3)} ms`,
      `max lag ${summary.maxAckLagMessages}`,
    ].join(' | '),
  );
}

export async function main() {
  const full = envFlag('ZERO_LOAD_FULL');
  const durationMs = envInt('ZERO_LOAD_DURATION_MS', full ? 5000 : 1000);
  const targetTxPerSec = envNumber('ZERO_LOAD_TARGET_TPS', 1000);
  const rowsPerTx = full ? [1, 10, 100] : [1];
  const payloads: readonly PayloadProfile[] = full
    ? loadPayloadProfiles
    : smokePayloadProfiles;
  const consumerConfig: ConsumerConfig = {
    count: envInt('ZERO_LOAD_SUBSCRIBERS', full ? 4 : 2),
    ackDelayMs: envNumber('ZERO_LOAD_ACK_DELAY_MS', 0),
    slowAckDelayMs: envNumber('ZERO_LOAD_SLOW_ACK_DELAY_MS', full ? 2 : 1),
    slowEvery: envInt('ZERO_LOAD_SLOW_EVERY', full ? 4 : 2),
  };
  const output = argValue('out') ?? process.env.ZERO_BENCH_OUT;
  const dbFile = process.env.ZERO_LOAD_DB ?? '/tmp/zero-cache-load-harness.db';
  const scenarios: ScenarioSummary[] = [];

  for (const rows of rowsPerTx) {
    for (const payload of payloads) {
      const summary = await runScenario(
        rows,
        payload,
        durationMs,
        targetTxPerSec,
        consumerConfig,
        dbFile,
      );
      scenarios.push(summary);
      printScenario(summary);
    }
  }

  const summary: Summary = {
    name: 'zero-cache-load-harness',
    mode: full ? 'full' : 'smoke',
    generatedAt: new Date().toISOString(),
    scenarios,
  };
  await writeJsonSummary(summary, output);
  console.log(JSON.stringify(summary));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
