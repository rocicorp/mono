import {performance} from 'node:perf_hooks';
import {parentPort, workerData} from 'node:worker_threads';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {WorkerMessageBatcher} from '../../src/services/replicator/worker-message-batcher.ts';
import {ThreadWriteWorkerClient} from '../../src/services/replicator/write-worker-client.ts';
import {sleep} from './perf-utils.ts';
import {
  connectConsumerWebSocket,
  parseTransportBatch,
  type TransportBatch,
} from './protocol.ts';
import {cleanupSQLite, initializeReplica} from './replica.ts';
import type {ConsumerWorkerData, LoadConsumerStats} from './types.ts';

if (parentPort === null) {
  throw new Error('consumer-worker must run inside a worker_thread');
}

const port = parentPort;
const config = workerData as ConsumerWorkerData;
const lc = createSilentLogContext();
let active = true;
let cpuSink = 0;

let processed = 0;
let totalParseMs = 0;
let totalApplyMs = 0;
let totalTxApplyMs = 0;
let maxTxApplyMs = 0;
let txApplySamples = 0;
let txApplyStart: number | undefined;
let totalClientCpuMs = 0;
let samples = 0;

function stats(): LoadConsumerStats {
  const transportStats = transport?.stats() ?? {
    messages: 0,
    bytes: 0,
    acks: 0,
    ackBytes: 0,
  };
  return {
    processed,
    ackedWatermark: '',
    watermark: '',
    pending: 0,
    transportMessages: transportStats.messages,
    transportBytes: transportStats.bytes,
    transportAcks: transportStats.acks,
    transportAckBytes: transportStats.ackBytes,
    maxAckLagMessages: 0,
    totalAckLagMessages: 0,
    totalParseMs,
    totalApplyMs,
    totalTxApplyMs,
    maxTxApplyMs,
    txApplySamples,
    totalClientCpuMs,
    samples,
  };
}

let transport: Awaited<ReturnType<typeof connectConsumerWebSocket>> | undefined;

port.on('message', msg => {
  if (typeof msg === 'object' && msg !== null && 'type' in msg) {
    switch (msg.type) {
      case 'stop':
        active = false;
        void transport?.close();
        break;
      case 'stats':
        port.postMessage({type: 'stats', stats: stats()});
        break;
    }
  }
});

function postStats() {
  port.postMessage({type: 'stats', stats: stats()});
}

try {
  await run();
  port.postMessage({type: 'done', stats: stats()});
  port.close();
} catch (e) {
  const error = e instanceof Error ? e : new Error(String(e));
  port.postMessage({
    type: 'error',
    error: {message: error.message, stack: error.stack},
  });
  port.close();
}

async function run() {
  let consumerReplica: Database | undefined;
  let processor: ReturnType<typeof initializeReplica> | undefined;
  let writeWorker: ThreadWriteWorkerClient | undefined;

  if (config.sqlitePath !== undefined && config.applyMode !== 'none') {
    cleanupSQLite(config.sqlitePath);
    consumerReplica = new Database(lc, config.sqlitePath);
    consumerReplica.pragma('journal_mode = WAL2');
    consumerReplica.pragma(`synchronous = ${config.synchronous ?? 'NORMAL'}`);
    processor = initializeReplica(lc, consumerReplica, config.replicaVersion);
    if (config.applyMode !== 'direct') {
      consumerReplica.close();
      consumerReplica = undefined;
      processor = undefined;
      writeWorker = new ThreadWriteWorkerClient();
      await writeWorker.init(
        config.sqlitePath,
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

  const workerBatcher = writeWorker
    ? new WorkerMessageBatcher(writeWorker, config.workerBatchMessages)
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
        return writeWorker?.processMessage(change);
      case 'worker-batch':
        return workerBatcher?.push(change);
    }
  };

  const interval = setInterval(postStats, 25);
  interval.unref();

  try {
    transport = await connectConsumerWebSocket(
      lc,
      config.url,
      config.transportAckMode,
      config.protocolMode,
    );
    port.postMessage({type: 'ready'});

    for await (const batch of transport.messages) {
      if (!active) {
        break;
      }
      await consumeBatch(batch, applyChange);
    }
  } finally {
    clearInterval(interval);
    if (workerBatcher !== undefined && workerBatcher.size > 0) {
      workerBatcher.clear();
      writeWorker?.abort();
    }
    await transport?.close();
    await writeWorker?.stop();
    consumerReplica?.close();
    if (config.sqlitePath !== undefined) {
      cleanupSQLite(config.sqlitePath);
    }
  }
}

async function consumeBatch(
  batch: TransportBatch,
  applyChange: (
    change: ChangeStreamData | undefined,
  ) => Promise<unknown> | undefined,
) {
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
    if (config.ackDelayMs > 0) {
      await sleep(config.ackDelayMs);
    }
    processed++;
    samples++;
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
