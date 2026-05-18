import {once} from 'node:events';
import type {LogContext} from '@rocicorp/logger';
import WebSocket, {WebSocketServer} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import {BigIntJSON} from '../../../shared/src/bigint-json.ts';
import * as v from '../../../shared/src/valita.ts';
import type {ChangeStreamData} from '../../src/services/change-source/protocol/current/downstream.ts';
import {CHANGE_STREAMER_V7_PROTOCOL_VERSION} from '../../src/services/change-streamer/change-streamer-protocol.ts';
import {
  downstreamSchemaForProtocolVersion,
  downstreamSchema,
  type ChangeStreamerDownstream,
  type Downstream,
} from '../../src/services/change-streamer/change-streamer.ts';
import {
  streamIn,
  streamOutStringified,
  type StringifiedStreamPayload,
} from '../../src/types/streams.ts';
import {Subscription} from '../../src/types/subscription.ts';
import {benchRelation} from './fixtures.ts';
import type {
  ConsumerProtocolMode,
  ConsumerTransportAckMode,
  ConsumerTransportMode,
} from './types.ts';

export type TransportMessage = string | ChangeStreamerDownstream;

export type TransportBatch =
  | {readonly kind: 'messages'; readonly messages: readonly TransportMessage[]}
  | {readonly kind: 'downstreams'; readonly messages: readonly Downstream[]};

export type ConsumerTransportStats = {
  readonly messages: number;
  readonly bytes: number;
  readonly acks: number;
  readonly ackBytes: number;
};

type QueuedStringifiedMessage = {
  readonly json: string;
  readonly consumed: () => void;
};

type CompactOp =
  | readonly ['b', string | null]
  | readonly ['c', string | null]
  | readonly ['i', string, number, number, number, string];

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
      const server = new WebSocketServer({host: '127.0.0.1', port: 0});
      server.on('connection', ws => {
        trackWebSocketSend(ws, data => {
          stats.messages++;
          stats.bytes += byteLength(data);
        });
        if (usesProductionStream(protocolMode)) {
          void streamOutStringified(
            lc,
            downstream,
            ws,
            batchMessages > 1
              ? {batch: {maxMessages: batchMessages}}
              : undefined,
          );
        } else {
          void streamOutBatchFrames(
            downstream,
            ws,
            batchMessages,
            protocolMode,
          );
        }
      });
      await once(server, 'listening');
      const address = server.address();
      assert(
        address !== null && typeof address !== 'string',
        'expected websocket server address',
      );
      const ws = new WebSocket(`ws://127.0.0.1:${address.port}`);
      trackWebSocketSend(ws, data => {
        stats.acks++;
        stats.ackBytes += byteLength(data);
      });
      let cancelMessages: () => void;
      let messages: AsyncIterable<TransportBatch>;
      if (usesProductionStream(protocolMode)) {
        const stream = await streamIn(
          lc,
          ws,
          protocolMode === 'v7'
            ? downstreamSchemaForProtocolVersion(
                CHANGE_STREAMER_V7_PROTOCOL_VERSION,
              )
            : downstreamSchema,
          {ack: ackMode},
        );
        cancelMessages = () => stream.cancel();
        messages = batchSingletonMessages(stream);
      } else {
        const stream = await streamInBatchFrames(ws, protocolMode);
        cancelMessages = () => stream.cancel();
        messages = stream;
      }
      return {
        messages,
        close: async () => {
          cancelMessages();
          ws.close();
          await new Promise<void>((resolve, reject) => {
            server.close(err => (err ? reject(err) : resolve()));
          });
        },
        stats: () => stats,
      };
    }
  }
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

function usesProductionStream(protocolMode: ConsumerProtocolMode) {
  return (
    protocolMode === 'v6' ||
    protocolMode === 'v7' ||
    protocolMode === 'message-json'
  );
}

async function* batchSingletonMessages(
  source: AsyncIterable<TransportMessage>,
): AsyncIterable<TransportBatch> {
  for await (const message of source) {
    yield {kind: 'messages', messages: [message]};
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

async function streamOutBatchFrames(
  source: Subscription<StringifiedStreamPayload>,
  sink: WebSocket,
  maxFrameMessages: number,
  protocolMode: Exclude<ConsumerProtocolMode, 'message-json' | 'v6' | 'v7'>,
) {
  const pipeline = source.pipeline;
  assert(pipeline, 'batch frame transport requires a pipelined source');

  let nextID = 0;
  let lastAck = 0;
  const pending = new Map<number, readonly (() => void)[]>();

  sink.on('message', data => {
    try {
      const {ack} = parseTransportAck(data.toString());
      if (ack < lastAck) {
        throw new Error(`Ack moved backwards from ${lastAck} to ${ack}`);
      }
      lastAck = ack;
      for (const [id, callbacks] of pending) {
        if (id > ack) {
          break;
        }
        for (const consumed of callbacks) {
          consumed();
        }
        pending.delete(id);
      }
    } catch (e) {
      source.cancel(e instanceof Error ? e : new Error(String(e)));
      sink.close();
    }
  });

  const sendFrame = (messages: readonly QueuedStringifiedMessage[]) => {
    if (messages.length === 0 || sink.readyState !== WebSocket.OPEN) {
      return;
    }
    const id = ++nextID;
    pending.set(
      id,
      messages.map(({consumed}) => consumed),
    );
    sink.send(stringifyBatchFrame(id, messages, protocolMode));
  };

  try {
    const iterator = pipeline[Symbol.asyncIterator]();
    const maxMessages = Math.max(1, maxFrameMessages);
    for (;;) {
      const first = await iterator.next();
      if (first.done) {
        break;
      }

      const entries = [first.value];
      let messageCount = countStringifiedMessages(first.value.value);
      while (messageCount < maxMessages && source.queued > 0) {
        const next = await iterator.next();
        if (next.done) {
          break;
        }
        entries.push(next.value);
        messageCount += countStringifiedMessages(next.value.value);
      }

      const frameMessages: QueuedStringifiedMessage[] = [];
      for (const {value, consumed} of entries) {
        const messages = stringifiedMessages(value);
        if (messages.length === 0) {
          consumed();
          continue;
        }
        let remaining = messages.length;
        const consumeOne = () => {
          remaining--;
          if (remaining === 0) {
            consumed();
          }
        };

        for (const json of messages) {
          frameMessages.push({json, consumed: consumeOne});
        }
      }
      sendFrame(frameMessages);
    }
    sink.close();
  } catch (e) {
    source.cancel(e instanceof Error ? e : new Error(String(e)));
    sink.close();
  }
}

function stringifiedMessages(
  value: StringifiedStreamPayload,
): readonly string[] {
  return typeof value === 'string' ? [value] : value;
}

function countStringifiedMessages(value: StringifiedStreamPayload) {
  return typeof value === 'string' ? 1 : value.length;
}

function stringifyBatchFrame(
  id: number,
  messages: readonly QueuedStringifiedMessage[],
  protocolMode: Exclude<ConsumerProtocolMode, 'message-json' | 'v6' | 'v7'>,
) {
  if (protocolMode === 'batch-compact') {
    const compact = compactBatchFrame(id, messages);
    if (compact !== undefined) {
      return compact;
    }
  }
  return `{"id":${id},"messages":[${messages.map(({json}) => json).join(',')}]}`;
}

function compactBatchFrame(
  id: number,
  messages: readonly QueuedStringifiedMessage[],
): string | undefined {
  const parsed = messages.map(({json}) =>
    BigIntJSON.parse(json),
  ) as Downstream[];
  const ops: CompactOp[] = [];
  for (const msg of parsed) {
    switch (msg[0]) {
      case 'begin': {
        const metadata = msg[2] as {readonly commitWatermark?: unknown};
        const watermark = metadata.commitWatermark;
        if (watermark !== undefined && typeof watermark !== 'string') {
          return undefined;
        }
        ops.push(['b', watermark ?? null]);
        break;
      }
      case 'commit': {
        const metadata = msg[2] as {readonly watermark?: unknown};
        const watermark = metadata.watermark;
        if (watermark !== undefined && typeof watermark !== 'string') {
          return undefined;
        }
        ops.push(['c', watermark ?? null]);
        break;
      }
      case 'data': {
        if (msg[1].tag !== 'insert') {
          return undefined;
        }
        const row = msg[1].new;
        if (
          typeof row.id !== 'string' ||
          typeof row.tx !== 'number' ||
          typeof row.seq !== 'number' ||
          typeof row.bucket !== 'number' ||
          typeof row.payload !== 'string'
        ) {
          return undefined;
        }
        ops.push(['i', row.id, row.tx, row.seq, row.bucket, row.payload]);
        break;
      }
      default:
        return undefined;
    }
  }

  return JSON.stringify({id, c: 'ops-v1', o: ops});
}

async function streamInBatchFrames(
  source: WebSocket,
  protocolMode: Exclude<ConsumerProtocolMode, 'message-json' | 'v6' | 'v7'>,
): Promise<Subscription<TransportBatch, {id: number; batch: TransportBatch}>> {
  if (source.readyState === WebSocket.CONNECTING) {
    await once(source, 'open');
  }

  const sink = new Subscription<
    TransportBatch,
    {id: number; batch: TransportBatch}
  >(
    {
      consumed: ({id}) => {
        if (source.readyState === WebSocket.OPEN) {
          source.send(JSON.stringify({ack: id}));
        }
      },
      cleanup: () => source.close(),
    },
    ({batch}) => batch,
  );

  source.on('message', data => {
    if (!sink.active) {
      return;
    }
    try {
      const {id, messages} = parseBatchFrame(data.toString(), protocolMode);
      sink.push({id, batch: {kind: 'downstreams', messages}});
    } catch (e) {
      sink.fail(e instanceof Error ? e : new Error(String(e)));
    }
  });
  source.on('error', err => sink.fail(err));
  source.on('close', () => sink.cancel());
  return sink;
}

function parseTransportAck(data: string): {ack: number} {
  const ack = JSON.parse(data) as {ack?: unknown};
  const value = ack.ack;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`Invalid ack ${String(ack.ack)}`);
  }
  return {ack: value};
}

function parseBatchFrame(
  data: string,
  protocolMode: Exclude<ConsumerProtocolMode, 'message-json' | 'v6' | 'v7'>,
): {id: number; messages: readonly Downstream[]} {
  const frame = BigIntJSON.parse(data) as {
    readonly id?: unknown;
    readonly messages?: unknown;
    readonly c?: unknown;
    readonly o?: unknown;
  };
  const id = frame.id;
  if (typeof id !== 'number' || !Number.isSafeInteger(id) || id <= 0) {
    throw new Error(`Invalid batch frame id ${String(frame.id)}`);
  }
  if (
    protocolMode === 'batch-compact' &&
    frame.c === 'ops-v1' &&
    Array.isArray(frame.o)
  ) {
    return {
      id,
      messages: expandCompactOps(frame.o),
    };
  }
  if (!Array.isArray(frame.messages)) {
    throw new Error('Invalid batch frame messages');
  }
  return {
    id,
    messages: frame.messages.map(msg =>
      v.parse(msg, downstreamSchema, 'passthrough'),
    ),
  };
}

function expandCompactOps(ops: readonly unknown[]): Downstream[] {
  return ops.map(op => {
    if (!Array.isArray(op) || op.length === 0) {
      throw new Error('Invalid compact op');
    }
    switch (op[0]) {
      case 'b': {
        const watermark = op[1];
        if (watermark !== null && typeof watermark !== 'string') {
          throw new Error('Invalid compact begin op');
        }
        return [
          'begin',
          {tag: 'begin'},
          watermark === null ? {} : {commitWatermark: watermark},
        ] as Downstream;
      }
      case 'c': {
        const watermark = op[1];
        if (watermark !== null && typeof watermark !== 'string') {
          throw new Error('Invalid compact commit op');
        }
        return [
          'commit',
          {tag: 'commit'},
          watermark === null ? {} : {watermark},
        ] as Downstream;
      }
      case 'i': {
        if (op.length !== 6) {
          throw new Error('Invalid compact insert op');
        }
        const [, id, tx, seq, bucket, payload] = op;
        if (
          typeof id !== 'string' ||
          typeof tx !== 'number' ||
          typeof seq !== 'number' ||
          typeof bucket !== 'number' ||
          typeof payload !== 'string'
        ) {
          throw new Error('Invalid compact insert op values');
        }
        return [
          'data',
          {
            tag: 'insert',
            relation: benchRelation,
            new: {id, tx, seq, bucket, payload},
          },
        ] as Downstream;
      }
      default:
        throw new Error(`Invalid compact op ${String(op[0])}`);
    }
  });
}

export function parseTransportBatch(
  batch: TransportBatch,
): (ChangeStreamData | undefined)[] {
  switch (batch.kind) {
    case 'messages': {
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
    case 'downstreams': {
      const changes: (ChangeStreamData | undefined)[] = [];
      for (const message of batch.messages) {
        changes.push(...downstreamToChangeStreamData(message));
      }
      return changes;
    }
  }
}

function parseChangeStreamData(
  message: string,
): readonly (ChangeStreamData | undefined)[] {
  return downstreamToChangeStreamData(
    BigIntJSON.parse(message) as ChangeStreamerDownstream,
  );
}

function downstreamToChangeStreamData(
  parsed: ChangeStreamerDownstream,
): readonly (ChangeStreamData | undefined)[] {
  switch (parsed[0]) {
    case 'status':
      return [undefined];
    case 'error':
      throw new Error(`subscription error: ${JSON.stringify(parsed[1])}`);
    case 'change-batch':
      return parsed[1].changes;
    case 'begin':
    case 'data':
    case 'commit':
    case 'rollback':
      return [parsed];
    default:
      return [undefined];
  }
}
