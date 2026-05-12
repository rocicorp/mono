import {once} from 'node:events';
import type {AddressInfo} from 'node:net';
import {monitorEventLoopDelay, performance} from 'node:perf_hooks';
import {fileURLToPath} from 'node:url';
import WebSocket, {WebSocketServer, type RawData} from 'ws';
import {BigIntJSON, type JSONValue} from '../../shared/src/bigint-json.ts';
import {createSilentLogContext} from '../../shared/src/logging-test-utils.ts';
import * as v from '../../shared/src/valita.ts';
import {
  downstreamSchema,
  type Downstream,
} from '../src/services/change-streamer/change-streamer.ts';
import {
  streamIn,
  streamOutStringified,
  type Source,
} from '../src/types/streams.ts';
import {Subscription, type Result} from '../src/types/subscription.ts';
import {expectPingsForLiveness, sendPingsForLiveness} from '../src/types/ws.ts';
import {loadPayloadProfiles, type PayloadProfile} from './load-fixtures.ts';
import {
  argValue,
  envFlag,
  envInt,
  formatBytes,
  formatRate,
  percentile,
  sleep,
  writeJsonSummary,
} from './perf-utils.ts';

const pingIntervalMs = 30_000;
const threshold =
  'Ship production cumulative ack only if every scenario has no p95 latency regression greater than 1 ms or 5%, and cumulative ack reduces ack messages, elapsed time, or p95 event-loop delay by >10% (or ack traffic by >=50%).';

type Strategy = 'current-per-message-ack' | 'simulated-cumulative-ack';
type Mode = 'smoke' | 'full';

type Scenario = {
  readonly name: string;
  readonly burstMessages: number;
  readonly intervalMs: number;
};

type AckBatchConfig = {
  readonly every: number;
  readonly intervalMs: number;
};

type ProduceSummary = {
  readonly sent: number;
  readonly nonConsumed: number;
};

type ScenarioResult = {
  readonly strategy: Strategy;
  readonly scenario: string;
  readonly burstMessages: number;
  readonly intervalMs: number;
  readonly targetMessagesPerSec: number;
  readonly payload: string;
  readonly payloadBytes: number;
  readonly messages: number;
  readonly receivedMessages: number;
  readonly acknowledgedMessages: number;
  readonly ackMessages: number;
  readonly ackBytes: number;
  readonly elapsedMs: number;
  readonly messageRate: number;
  readonly ackMessagesPerSec: number;
  readonly p95LatencyMs: number;
  readonly p99LatencyMs: number;
  readonly eventLoopDelayP95Ms: number;
  readonly eventLoopDelayMaxMs: number;
};

type ScenarioComparison = {
  readonly scenario: string;
  readonly payload: string;
  readonly messages: number;
  readonly ackReductionPct: number;
  readonly elapsedChangePct: number;
  readonly p95LatencyChangePct: number;
  readonly p95LatencyDeltaMs: number;
  readonly eventLoopDelayP95ChangePct: number;
  readonly noLatencyRegression: boolean;
  readonly go: boolean;
  readonly decision: 'go' | 'no-go';
};

type BenchmarkDecision = {
  readonly go: boolean;
  readonly recommendation: string;
  readonly threshold: string;
};

type Summary = {
  readonly name: 'zero-cache-ack-overhead';
  readonly mode: Mode;
  readonly generatedAt: string;
  readonly ackBatch: AckBatchConfig;
  readonly results: readonly ScenarioResult[];
  readonly comparisons: readonly ScenarioComparison[];
  readonly decision: BenchmarkDecision;
};

type Streamed<T> = {
  readonly msg: T;
  readonly id: number;
};

type WebSocketPair = {
  readonly server: WebSocketServer;
  readonly serverSocket: WebSocket;
  readonly clientSocket: WebSocket;
};

const scenarios: readonly Scenario[] = [
  {name: 'steady-1-every-1ms', burstMessages: 1, intervalMs: 1},
  {name: 'burst-10-every-10ms', burstMessages: 10, intervalMs: 10},
  {name: 'burst-100-every-100ms', burstMessages: 100, intervalMs: 100},
];

const payloadCache = new Map<number, string>();

function payloadFor(bytes: number) {
  let payload = payloadCache.get(bytes);
  if (payload === undefined) {
    payload = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'
      .repeat(Math.ceil(bytes / 62))
      .slice(0, bytes);
    payloadCache.set(bytes, payload);
  }
  return payload;
}

function makeDownstream(seq: number, payloadBytes: number, sentAt: number) {
  return BigIntJSON.stringify([
    'data',
    {
      tag: 'insert',
      relation: {
        schema: 'public',
        name: 'ack_bench_rows',
        rowKey: {columns: ['id']},
      },
      new: {
        id: `row-${seq}`,
        seq,
        sentAt,
        payload: payloadFor(payloadBytes),
      },
    },
  ] satisfies Downstream);
}

function sentAtOf(msg: Downstream) {
  if (msg[0] !== 'data' || msg[1].tag !== 'insert') {
    throw new Error(
      `Unexpected benchmark message ${BigIntJSON.stringify(msg)}`,
    );
  }
  const {sentAt} = msg[1].new;
  if (typeof sentAt !== 'number') {
    throw new Error(`Missing sentAt in benchmark message`);
  }
  return sentAt;
}

function rawDataBytes(data: RawData) {
  if (Array.isArray(data)) {
    return data.reduce((sum, buffer) => sum + buffer.byteLength, 0);
  }
  return typeof data === 'string' ? Buffer.byteLength(data) : data.byteLength;
}

function parseAck(data: RawData | string) {
  const parsed = JSON.parse(data.toString()) as {ack?: unknown};
  if (typeof parsed.ack !== 'number') {
    throw new Error(`Invalid ack ${data.toString()}`);
  }
  return parsed.ack;
}

function waitForOpen(ws: WebSocket) {
  switch (ws.readyState) {
    case WebSocket.OPEN:
      return Promise.resolve();
    case WebSocket.CONNECTING:
      return new Promise<void>((resolve, reject) => {
        ws.once('open', () => resolve());
        ws.once('error', reject);
        ws.once('close', () =>
          reject(new Error('WebSocket closed before open')),
        );
      });
    default:
      return Promise.reject(new Error(`WebSocket in state ${ws.readyState}`));
  }
}

function webSocketClosed(ws: WebSocket) {
  return (
    ws.readyState === WebSocket.CLOSED || ws.readyState === WebSocket.CLOSING
  );
}

async function createWebSocketPair(): Promise<WebSocketPair> {
  const server = new WebSocketServer({host: '127.0.0.1', port: 0});
  await once(server, 'listening');
  const address = server.address();
  if (address === null || typeof address === 'string') {
    throw new Error(`Unexpected WebSocket server address ${String(address)}`);
  }

  const connection = once(server, 'connection') as Promise<[WebSocket]>;
  const clientSocket = new WebSocket(
    `ws://127.0.0.1:${(address as AddressInfo).port}`,
  );
  const [serverSocket] = await connection;
  await waitForOpen(clientSocket);
  return {server, serverSocket, clientSocket};
}

async function closeWebSocketPair(pair: WebSocketPair) {
  for (const ws of [pair.clientSocket, pair.serverSocket]) {
    if (!webSocketClosed(ws)) {
      ws.terminate();
    }
  }
  await new Promise<void>((resolve, reject) => {
    pair.server.close(err => (err ? reject(err) : resolve()));
  });
}

function createCumulativeAcker(ws: WebSocket, config: AckBatchConfig) {
  let maxConsumed = 0;
  let lastSent = 0;
  let pending = 0;
  let timer: NodeJS.Timeout | undefined;

  const clear = () => {
    if (timer !== undefined) {
      clearTimeout(timer);
      timer = undefined;
    }
  };

  const flush = () => {
    clear();
    if (maxConsumed <= lastSent || ws.readyState !== WebSocket.OPEN) {
      return;
    }
    ws.send(JSON.stringify({ack: maxConsumed}));
    lastSent = maxConsumed;
    pending = 0;
  };

  return {
    consumed: (id: number) => {
      maxConsumed = Math.max(maxConsumed, id);
      pending++;
      if (pending >= config.every) {
        flush();
      } else if (timer === undefined) {
        timer = setTimeout(flush, config.intervalMs);
      }
    },
    close: () => {
      flush();
      clear();
    },
  };
}

async function streamInCumulative<T extends JSONValue>(
  lc: ReturnType<typeof createSilentLogContext>,
  source: WebSocket,
  schema: v.Type<T>,
  ackBatch: AckBatchConfig,
): Promise<Source<T>> {
  expectPingsForLiveness(lc, source, pingIntervalMs);

  const streamedSchema = v.object({
    msg: schema,
    id: v.number(),
  });
  const acker = createCumulativeAcker(source, ackBatch);
  const sink: Subscription<T, Streamed<T>> = new Subscription<T, Streamed<T>>(
    {
      consumed: ({id}) => acker.consumed(id),
      cleanup: () => cleanup(),
    },
    ({msg}) => msg,
  );

  function cleanup() {
    acker.close();
    source.removeEventListener('message', handleMessage);
    source.removeEventListener('close', handleClose);
    source.removeEventListener('error', handleError);
    if (!webSocketClosed(source)) {
      source.close();
    }
  }

  function handleMessage(event: WebSocket.MessageEvent) {
    const data = event.data.toString();
    if (!sink.active) {
      return;
    }
    try {
      const value = BigIntJSON.parse(data);
      const msg = v.parse(value, streamedSchema, 'passthrough');
      sink.push(msg);
    } catch (err) {
      sink.fail(ensureError(err));
    }
  }

  function handleClose() {
    sink.end();
  }

  function handleError(event: WebSocket.ErrorEvent) {
    sink.fail(ensureError(event.error));
  }

  source.addEventListener('message', handleMessage);
  source.addEventListener('close', handleClose);
  source.addEventListener('error', handleError);

  await waitForOpen(source);
  return sink;
}

async function streamOutStringifiedCumulative(
  lc: ReturnType<typeof createSilentLogContext>,
  source: Source<string>,
  sink: WebSocket,
): Promise<void> {
  sendPingsForLiveness(lc, sink, pingIntervalMs);

  let nextID = 0;
  let lastAck = 0;
  const pending = new Map<number, () => void>();

  sink.addEventListener('message', ({data}) => {
    try {
      if (typeof data !== 'string') {
        throw new Error('Expected string ack message');
      }
      const ack = parseAck(data);
      if (ack < lastAck) {
        throw new Error(`Ack moved backwards from ${lastAck} to ${ack}`);
      }
      lastAck = ack;
      for (const [id, consumed] of pending) {
        if (id > ack) {
          continue;
        }
        consumed();
        pending.delete(id);
      }
    } catch (err) {
      if (!webSocketClosed(sink)) {
        sink.close(1002, String(err).slice(0, 123));
      }
    }
  });

  try {
    const {pipeline} = source;
    if (pipeline === undefined) {
      throw new Error(
        'Benchmark cumulative stream requires a pipelined source',
      );
    }
    for await (const {value: msg, consumed} of pipeline) {
      const id = ++nextID;
      pending.set(id, consumed);
      sink.send(`{"id":${id},"msg":${msg}}`);
    }
  } finally {
    if (!webSocketClosed(sink)) {
      sink.close();
    }
  }
}

function ensureError(err: unknown) {
  return err instanceof Error ? err : new Error(String(err));
}

async function consumeMessages(
  source: Source<Downstream>,
  latencies: number[],
) {
  let received = 0;
  for await (const msg of source) {
    latencies.push(performance.now() - sentAtOf(msg));
    received++;
  }
  return received;
}

async function produceMessages(
  source: Subscription<string>,
  scenario: Scenario,
  payload: PayloadProfile,
  messages: number,
): Promise<ProduceSummary> {
  const results: Promise<Result>[] = [];
  let sent = 0;
  let nextDue = performance.now();

  while (sent < messages) {
    const burst = Math.min(scenario.burstMessages, messages - sent);
    for (let i = 0; i < burst; i++) {
      results.push(
        source.push(makeDownstream(sent, payload.bytes, performance.now()))
          .result,
      );
      sent++;
    }

    nextDue += scenario.intervalMs;
    const waitMs = nextDue - performance.now();
    if (waitMs > 0) {
      await sleep(waitMs);
    }
  }

  const outcomes = await Promise.all(results);
  return {
    sent,
    nonConsumed: outcomes.filter(result => result !== 'consumed').length,
  };
}

async function withTimeout<T>(promise: Promise<T>, ms: number, label: string) {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${ms} ms`)),
          ms,
        );
      }),
    ]);
  } finally {
    if (timer !== undefined) {
      clearTimeout(timer);
    }
  }
}

function nsToMs(ns: number) {
  return Number.isFinite(ns) ? ns / 1_000_000 : 0;
}

async function runScenario(
  strategy: Strategy,
  scenario: Scenario,
  payload: PayloadProfile,
  messages: number,
  ackBatch: AckBatchConfig,
): Promise<ScenarioResult> {
  const lc = createSilentLogContext();
  const pair = await createWebSocketPair();
  const source = Subscription.create<string>();
  const latencies: number[] = [];
  let ackMessages = 0;
  let ackBytes = 0;
  let acknowledgedMessages = 0;

  pair.serverSocket.on('message', data => {
    ackMessages++;
    ackBytes += rawDataBytes(data);
    acknowledgedMessages = Math.max(acknowledgedMessages, parseAck(data));
  });

  try {
    const inbound =
      strategy === 'current-per-message-ack'
        ? await streamIn(lc, pair.clientSocket, downstreamSchema)
        : await streamInCumulative(
            lc,
            pair.clientSocket,
            downstreamSchema,
            ackBatch,
          );
    const consumer = consumeMessages(inbound, latencies);
    const outbound =
      strategy === 'current-per-message-ack'
        ? streamOutStringified(lc, source, pair.serverSocket)
        : streamOutStringifiedCumulative(lc, source, pair.serverSocket);
    const eventLoopDelay = monitorEventLoopDelay({resolution: 10});
    const targetMessagesPerSec =
      (scenario.burstMessages * 1000) / scenario.intervalMs;
    const timeoutMs =
      Math.ceil((messages * 1000) / targetMessagesPerSec) + 15_000;

    eventLoopDelay.enable();
    const start = performance.now();
    const produced = await withTimeout(
      produceMessages(source, scenario, payload, messages),
      timeoutMs,
      `${strategy} ${scenario.name} ${payload.size}`,
    );
    const elapsedMs = performance.now() - start;
    eventLoopDelay.disable();

    if (produced.sent !== messages || produced.nonConsumed !== 0) {
      throw new Error(
        `Produced ${produced.sent}/${messages}; ${produced.nonConsumed} were not consumed`,
      );
    }

    source.cancel();
    await withTimeout(
      Promise.all([outbound, consumer]),
      5_000,
      `${strategy} cleanup`,
    );

    const receivedMessages = await consumer;
    if (receivedMessages !== messages) {
      throw new Error(`Received ${receivedMessages}/${messages} messages`);
    }
    if (acknowledgedMessages !== messages) {
      throw new Error(
        `Acknowledged ${acknowledgedMessages}/${messages} messages`,
      );
    }

    return {
      strategy,
      scenario: scenario.name,
      burstMessages: scenario.burstMessages,
      intervalMs: scenario.intervalMs,
      targetMessagesPerSec,
      payload: payload.size,
      payloadBytes: payload.bytes,
      messages,
      receivedMessages,
      acknowledgedMessages,
      ackMessages,
      ackBytes,
      elapsedMs,
      messageRate: (messages * 1000) / elapsedMs,
      ackMessagesPerSec: (ackMessages * 1000) / elapsedMs,
      p95LatencyMs: percentile(latencies, 95),
      p99LatencyMs: percentile(latencies, 99),
      eventLoopDelayP95Ms: nsToMs(eventLoopDelay.percentile(95)),
      eventLoopDelayMaxMs: nsToMs(eventLoopDelay.max),
    };
  } finally {
    await closeWebSocketPair(pair);
  }
}

function percentChange(next: number, baseline: number) {
  return baseline === 0 ? 0 : ((next - baseline) * 100) / baseline;
}

function percentReduction(next: number, baseline: number) {
  return baseline === 0 ? 0 : ((baseline - next) * 100) / baseline;
}

function compareResults(
  baseline: ScenarioResult,
  cumulative: ScenarioResult,
): ScenarioComparison {
  const ackReductionPct = percentReduction(
    cumulative.ackMessages,
    baseline.ackMessages,
  );
  const elapsedChangePct = percentChange(
    cumulative.elapsedMs,
    baseline.elapsedMs,
  );
  const p95LatencyChangePct = percentChange(
    cumulative.p95LatencyMs,
    baseline.p95LatencyMs,
  );
  const p95LatencyDeltaMs = cumulative.p95LatencyMs - baseline.p95LatencyMs;
  const eventLoopDelayP95ChangePct = percentChange(
    cumulative.eventLoopDelayP95Ms,
    baseline.eventLoopDelayP95Ms,
  );
  const noLatencyRegression =
    p95LatencyDeltaMs <= 1 || p95LatencyChangePct <= 5;
  const improvesOverhead =
    ackReductionPct > 10 ||
    elapsedChangePct <= -10 ||
    eventLoopDelayP95ChangePct <= -10;
  const materiallyReducesTraffic = ackReductionPct >= 50;
  const go =
    noLatencyRegression && (improvesOverhead || materiallyReducesTraffic);

  return {
    scenario: baseline.scenario,
    payload: baseline.payload,
    messages: baseline.messages,
    ackReductionPct,
    elapsedChangePct,
    p95LatencyChangePct,
    p95LatencyDeltaMs,
    eventLoopDelayP95ChangePct,
    noLatencyRegression,
    go,
    decision: go ? 'go' : 'no-go',
  };
}

function makeDecision(
  comparisons: readonly ScenarioComparison[],
): BenchmarkDecision {
  const go = comparisons.every(comparison => comparison.go);
  return {
    go,
    recommendation: go
      ? 'GO for a separate production cumulative-ack PR; this benchmark intentionally leaves production protocol unchanged.'
      : 'NO-GO for production cumulative ack; keep production per-message ack unchanged.',
    threshold,
  };
}

function printResult(result: ScenarioResult) {
  console.log(
    [
      result.strategy,
      result.scenario,
      `${result.payload} (${formatBytes(result.payloadBytes)})`,
      `${result.messages} messages`,
      `${formatRate(result.messageRate)} msg/s`,
      `${result.ackMessages} ack messages`,
      `${formatBytes(result.ackBytes)} ack bytes`,
      `${result.elapsedMs.toFixed(1)} ms elapsed`,
      `p95=${result.p95LatencyMs.toFixed(3)} ms`,
      `eld-p95=${result.eventLoopDelayP95Ms.toFixed(3)} ms`,
    ].join(' | '),
  );
}

function printComparison(comparison: ScenarioComparison) {
  console.log(
    [
      comparison.decision.toUpperCase(),
      comparison.scenario,
      comparison.payload,
      `ack reduction=${comparison.ackReductionPct.toFixed(1)}%`,
      `elapsed change=${comparison.elapsedChangePct.toFixed(1)}%`,
      `p95 delta=${comparison.p95LatencyDeltaMs.toFixed(3)} ms`,
      `eld-p95 change=${comparison.eventLoopDelayP95ChangePct.toFixed(1)}%`,
    ].join(' | '),
  );
}

export async function main() {
  const full = envFlag('ZERO_ACK_FULL');
  const mode: Mode = full ? 'full' : 'smoke';
  const messages = envInt('ZERO_ACK_MESSAGES', full ? 5000 : 1000);
  const ackBatch: AckBatchConfig = {
    every: envInt('ZERO_ACK_BATCH_EVERY', 32),
    intervalMs: envInt('ZERO_ACK_BATCH_INTERVAL_MS', 5),
  };
  const output = argValue('out') ?? process.env.ZERO_BENCH_OUT;
  const payloads = loadPayloadProfiles.filter(
    payload => payload.size === 'small' || payload.size === 'medium',
  );
  const results: ScenarioResult[] = [];
  const comparisons: ScenarioComparison[] = [];

  console.log(threshold);
  console.log(
    `ack batch: every=${ackBatch.every}, intervalMs=${ackBatch.intervalMs}`,
  );

  for (const scenario of scenarios) {
    for (const payload of payloads) {
      const baseline = await runScenario(
        'current-per-message-ack',
        scenario,
        payload,
        messages,
        ackBatch,
      );
      results.push(baseline);
      printResult(baseline);

      const cumulative = await runScenario(
        'simulated-cumulative-ack',
        scenario,
        payload,
        messages,
        ackBatch,
      );
      results.push(cumulative);
      printResult(cumulative);

      const comparison = compareResults(baseline, cumulative);
      comparisons.push(comparison);
      printComparison(comparison);
    }
  }

  const summary: Summary = {
    name: 'zero-cache-ack-overhead',
    mode,
    generatedAt: new Date().toISOString(),
    ackBatch,
    results,
    comparisons,
    decision: makeDecision(comparisons),
  };
  await writeJsonSummary(summary, output);
  console.log(summary.decision.recommendation);
  console.log(JSON.stringify(summary));
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await main();
}
