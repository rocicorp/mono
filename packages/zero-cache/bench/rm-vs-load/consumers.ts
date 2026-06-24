import {performance} from 'node:perf_hooks';
import {Worker} from 'node:worker_threads';
import type {LogContext} from '@rocicorp/logger';
import {Database} from '../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {Subscriber} from '../../src/services/change-streamer/subscriber.ts';
import {ThreadWriteWorkerClient} from '../../src/services/replicator/write-worker-client.ts';
import {Subscription} from '../../src/types/subscription.ts';
import {protocolVersionForMode} from './config.ts';
import {sleep} from './perf-utils.ts';
import {
  createConsumerTransport,
  createConsumerWebSocketServer,
  mergeTransportStats,
  parseTransportBatch,
} from './protocol.ts';
import {cleanupSQLite, initializeReplica} from './replica.ts';
import type {
  ConsumerConfig,
  ConsumerWorkerData,
  LoadConsumer,
  LoadConsumerStats,
} from './types.ts';

export type ConsumerFactoryOptions = {
  readonly lc: LogContext;
  readonly sqlitePath: string;
  readonly replicaVersion: string;
  readonly consumerWorkerUrl?: URL | undefined;
};

export class ConsumerFactory {
  readonly #lc: LogContext;
  readonly #sqlitePath: string;
  readonly #replicaVersion: string;
  readonly #consumerWorkerUrl: URL;
  #cpuSink = 0;

  constructor({
    lc,
    sqlitePath,
    replicaVersion,
    consumerWorkerUrl = new URL('./consumer-worker.ts', import.meta.url),
  }: ConsumerFactoryOptions) {
    this.#lc = lc;
    this.#sqlitePath = sqlitePath;
    this.#replicaVersion = replicaVersion;
    this.#consumerWorkerUrl = consumerWorkerUrl;
  }

  createGroup(config: ConsumerConfig): Promise<LoadConsumer[]> {
    return Promise.all(
      Array.from({length: config.count}, (_, i) => {
        const isSlow = config.slowEvery > 0 && (i + 1) % config.slowEvery === 0;
        const consumerConfig = ConsumerFactory.configForApplySlot(config, i);
        return this.create(
          `vs-${i}`,
          this.#replicaVersion,
          isSlow ? consumerConfig.slowAckDelayMs : consumerConfig.ackDelayMs,
          consumerConfig,
          true,
        );
      }),
    );
  }

  static configForApplySlot(
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

  create(
    id: string,
    watermark: string,
    ackDelayMs: number,
    config: ConsumerConfig,
    caughtUp: boolean,
  ): Promise<LoadConsumer> {
    const downstream = Subscription.create<string>();
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
      return this.#createWorkerRuntimeConsumer(
        id,
        sub,
        downstream,
        ackDelayMs,
        config,
      );
    }
    return this.#createInlineConsumer(id, sub, downstream, ackDelayMs, config);
  }

  async #createInlineConsumer(
    id: string,
    sub: Subscriber,
    downstream: Subscription<string>,
    ackDelayMs: number,
    config: ConsumerConfig,
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
    let processor: ReturnType<typeof initializeReplica> | undefined;
    let worker: ThreadWriteWorkerClient | undefined;
    let consumerSQLitePath: string | undefined;

    if (config.applyMode !== 'none') {
      consumerSQLitePath = `${this.#sqlitePath}-${id}`;
      cleanupSQLite(consumerSQLitePath);
      consumerReplica = new Database(this.#lc, consumerSQLitePath);
      consumerReplica.pragma('journal_mode = WAL2');
      consumerReplica.pragma(`synchronous = ${config.synchronous ?? 'NORMAL'}`);
      processor = initializeReplica(
        this.#lc,
        consumerReplica,
        this.#replicaVersion,
      );
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
            walAutocheckpoint: config.walAutocheckpoint,
          },
          {level: 'error', format: 'text'},
        );
      }
    }

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
          processor?.processMessage(this.#lc, change);
          return;
        case 'worker-message':
          return worker?.processMessage(change);
      }
    };

    const transport = await createConsumerTransport(
      this.#lc,
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
            this.#burnCpu(config.clientCpuMicros);
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
        }
      } finally {
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
      sqlitePath: consumerSQLitePath,
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

  async #createWorkerRuntimeConsumer(
    id: string,
    sub: Subscriber,
    downstream: Subscription<string>,
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
      this.#lc,
      downstream,
      config.transportBatchMessages,
      config.protocolMode,
    );
    const consumerSQLitePath =
      config.applyMode === 'none' ? undefined : `${this.#sqlitePath}-${id}`;
    const worker = new Worker(this.#consumerWorkerUrl, {
      workerData: {
        id,
        url: server.url,
        sqlitePath: consumerSQLitePath,
        replicaVersion: this.#replicaVersion,
        protocolMode: config.protocolMode,
        transportAckMode: config.transportAckMode,
        applyMode: config.applyMode,
        synchronous: config.synchronous,
        walAutocheckpoint: config.walAutocheckpoint,
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
      const delta = Math.max(
        0,
        latestWorkerStats.samples - lastObservedSamples,
      );
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
      sqlitePath: consumerSQLitePath,
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

  #burnCpu(micros: number) {
    if (micros <= 0) {
      return;
    }

    const end = performance.now() + micros / 1000;
    let value = this.#cpuSink;
    while (performance.now() < end) {
      value = (value + Math.imul(value + 1, 31)) % 1_000_003;
    }
    this.#cpuSink = value;
  }
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
