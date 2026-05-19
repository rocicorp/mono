import {once} from 'node:events';
import type {LogContext} from '@rocicorp/logger';
import WebSocket, {WebSocketServer} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {
  downstreamSchema,
  type Downstream,
} from '../../src/services/change-streamer/change-streamer.ts';
import {
  isStreamBatch,
  streamInBatches,
  streamOutStringified,
  type StreamInPayload,
  type StringifiedStreamPayload,
} from '../../src/types/streams.ts';
import type {Subscription} from '../../src/types/subscription.ts';
import type {
  ConsumerProtocolMode,
  ConsumerTransportAckMode,
  ConsumerTransportMode,
} from './types.ts';

export type TransportMessage = string | Downstream;

export type TransportBatch = {
  readonly kind: 'messages';
  readonly messages: readonly TransportMessage[];
};

export type ConsumerTransportStats = {
  readonly messages: number;
  readonly bytes: number;
  readonly acks: number;
  readonly ackBytes: number;
};

export async function createConsumerTransport(
  lc: LogContext,
  downstream: Subscription<StringifiedStreamPayload>,
  mode: ConsumerTransportMode,
  ackMode: ConsumerTransportAckMode,
  batchMessages: number,
  protocolMode: ConsumerProtocolMode,
): Promise<{
  messages: AsyncIterable<TransportBatch>;
  close: () => Promise<void>;
  stats: () => ConsumerTransportStats;
}> {
  const stats = {messages: 0, bytes: 0, acks: 0, ackBytes: 0};
  switch (mode) {
    case 'in-process':
      return {
        messages: batchSingletonMessages(
          flattenStringifiedMessages(downstream),
        ),
        close: () => Promise.resolve(),
        stats: () => stats,
      };
    case 'websocket': {
      const server = await createConsumerWebSocketServer(
        lc,
        downstream,
        batchMessages,
        protocolMode,
      );
      const client = await connectConsumerWebSocket(
        lc,
        server.url,
        ackMode,
        protocolMode,
      );
      return {
        messages: client.messages,
        close: async () => {
          await client.close();
          await server.close();
        },
        stats: () => mergeTransportStats(server.stats(), client.stats()),
      };
    }
  }
}

export async function createConsumerWebSocketServer(
  lc: LogContext,
  downstream: Subscription<StringifiedStreamPayload>,
  batchMessages: number,
  protocolMode: ConsumerProtocolMode,
): Promise<{
  url: string;
  close: () => Promise<void>;
  stats: () => ConsumerTransportStats;
}> {
  const stats = {messages: 0, bytes: 0, acks: 0, ackBytes: 0};
  const server = new WebSocketServer({host: '127.0.0.1', port: 0});
  server.on('connection', ws => {
    trackWebSocketSend(ws, data => {
      stats.messages++;
      stats.bytes += byteLength(data);
    });
    assert(protocolMode === 'v6', 'rm-vs-load only models v6 stream payloads');
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
    address !== null && typeof address !== 'string',
    'expected websocket server address',
  );
  return {
    url: `ws://127.0.0.1:${address.port}`,
    close: () =>
      new Promise<void>((resolve, reject) => {
        server.close(err => (err ? reject(err) : resolve()));
      }),
    stats: () => stats,
  };
}

export async function connectConsumerWebSocket(
  lc: LogContext,
  url: string,
  ackMode: ConsumerTransportAckMode,
  protocolMode: ConsumerProtocolMode,
): Promise<{
  messages: AsyncIterable<TransportBatch>;
  close: () => Promise<void>;
  stats: () => ConsumerTransportStats;
}> {
  const stats = {messages: 0, bytes: 0, acks: 0, ackBytes: 0};
  const ws = new WebSocket(url);
  trackWebSocketSend(ws, data => {
    stats.acks++;
    stats.ackBytes += byteLength(data);
  });
  let cancelMessages: () => void;
  let messages: AsyncIterable<TransportBatch>;
  assert(protocolMode === 'v6', 'rm-vs-load only models v6 stream payloads');
  const stream = await streamInBatches(lc, ws, downstreamSchema, {
    ack: ackMode,
  });
  cancelMessages = () => stream.cancel();
  messages = productionTransportBatches(stream);
  return {
    messages,
    close: () => {
      cancelMessages();
      ws.close();
      return Promise.resolve();
    },
    stats: () => stats,
  };
}

export function mergeTransportStats(
  a: ConsumerTransportStats,
  b: ConsumerTransportStats,
): ConsumerTransportStats {
  return {
    messages: a.messages + b.messages,
    bytes: a.bytes + b.bytes,
    acks: a.acks + b.acks,
    ackBytes: a.ackBytes + b.ackBytes,
  };
}

function trackWebSocketSend(ws: WebSocket, onSend: (data: unknown) => void) {
  const send = ws.send.bind(ws) as (...args: unknown[]) => void;
  ws.send = ((data: unknown, ...args: unknown[]) => {
    onSend(data);
    return send(data, ...args);
  }) as WebSocket['send'];
}

function byteLength(data: unknown): number {
  if (typeof data === 'string') {
    return Buffer.byteLength(data);
  }
  if (Buffer.isBuffer(data)) {
    return data.byteLength;
  }
  if (data instanceof ArrayBuffer) {
    return data.byteLength;
  }
  if (ArrayBuffer.isView(data)) {
    return data.byteLength;
  }
  return Buffer.byteLength(String(data));
}

async function* batchSingletonMessages(
  source: AsyncIterable<TransportMessage>,
): AsyncIterable<TransportBatch> {
  for await (const message of source) {
    yield {kind: 'messages', messages: [message]};
  }
}

async function* productionTransportBatches(
  source: AsyncIterable<StreamInPayload<Downstream>>,
): AsyncIterable<TransportBatch> {
  for await (const payload of source) {
    if (isStreamBatch(payload)) {
      yield {kind: 'messages', messages: payload.messages};
    } else {
      yield {kind: 'messages', messages: [payload]};
    }
  }
}

async function* flattenStringifiedMessages(
  source: AsyncIterable<StringifiedStreamPayload>,
): AsyncIterable<string> {
  for await (const payload of source) {
    if (typeof payload === 'string') {
      yield payload;
    } else {
      yield* payload;
    }
  }
}

export function parseTransportBatch(
  batch: TransportBatch,
): (ChangeStreamData | undefined)[] {
  const changes: (ChangeStreamData | undefined)[] = [];
  for (const message of batch.messages) {
    changes.push(
      ...(typeof message === 'string'
        ? parseChangeStreamData(message)
        : downstreamToChangeStreamData(message)),
    );
  }
  return changes;
}

function parseChangeStreamData(
  message: string,
): readonly (ChangeStreamData | undefined)[] {
  return downstreamToChangeStreamData(BigIntJSON.parse(message) as Downstream);
}

function downstreamToChangeStreamData(
  parsed: Downstream,
): readonly (ChangeStreamData | undefined)[] {
  switch (parsed[0]) {
    case 'status':
      return [undefined];
    case 'error':
      throw new Error(`subscription error: ${JSON.stringify(parsed[1])}`);
    case 'begin':
    case 'data':
    case 'commit':
    case 'rollback':
      return [parsed];
    default:
      return [undefined];
  }
}
