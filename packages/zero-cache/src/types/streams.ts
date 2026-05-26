import {
  pipeline,
  Readable,
  Transform,
  Writable,
  type DuplexOptions,
} from 'node:stream';
import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {
  createWebSocketStream,
  type CloseEvent,
  type ErrorEvent,
  type MessageEvent,
  type WebSocket,
} from 'ws';
import {assert} from '../../../shared/src/asserts.ts';
import {BigIntJSON, type JSONValue} from '../../../shared/src/bigint-json.ts';
import {HeadIndexedQueue} from '../../../shared/src/head-indexed-queue.ts';
import {Queue} from '../../../shared/src/queue.ts';
import * as v from '../../../shared/src/valita.ts';
import {Subscription, type Options} from './subscription.ts';
import {
  closeWithError,
  expectPingsForLiveness,
  sendPingsForLiveness,
} from './ws.ts';

// Consistent with Postgres keepalives, and shorter than the
// commonly used default idle timeout of 1 minute.
const PING_INTERVAL_MS = 30_000;

export type Source<T> = AsyncIterable<T> & {
  /**
   * Immediately terminates all current iterations (i.e. {@link AsyncIterator.next next()})
   * will return `{value: undefined, done: true}`), and prevents any subsequent iterations
   * from yielding any values.
   *
   * @param err Terminate the iteration by throwing the `err` instead.
   */
  cancel: (err?: Error) => void;

  /**
   * The presence of a `pipeline` iterable allows the usual "consumed-on-iterate" semantics
   * to be overridden.
   *
   * This is suitable for transport layers that serialize messages across processes, such
   * as the {@link streamOut()} method; pipelining allows the transport to send messages
   * as they arrive without waiting for the previous message to be acked, streaming
   * them to the receiving process where they are presumably queued and processed without
   * a per-message ack delay. The receiving end of the transport then responds with acks
   * asynchronously as the receiving end processes the messages.
   */
  pipeline?: AsyncIterable<{value: T; consumed: () => void}> | undefined;
};

export type Sink<T> = {
  push(message: T): void;
};

export type StreamBatch<T> = {
  readonly tag: 'stream-batch';
  readonly messages: readonly T[];
};

export type StreamInPayload<T> = T | StreamBatch<T>;

export function isStreamBatch<T>(
  payload: StreamInPayload<T>,
): payload is StreamBatch<T> {
  return (
    typeof payload === 'object' &&
    payload !== null &&
    !Array.isArray(payload) &&
    (payload as {readonly tag?: unknown}).tag === 'stream-batch'
  );
}

/**
 * Back-pressure-aware transformation of a WebSocket into
 * upstream and downstream {@link Subscription} objects.
 */
// TODO: Change {@link streamIn} and {@link streamOut} to use this
//       under the covers so that internal communication is also
//       responsive to backpressure.
export function stream<In extends JSONValue, Out extends JSONValue>(
  lc: LogContext,
  ws: WebSocket,
  inSchema: v.Type<In>,
  outOptions: Options<Out> = {},
  inOptions: Options<In> = {},
  streamOptions: DuplexOptions = {},
): {outstream: Sink<Out>; instream: Source<In>} {
  const endpoint = ws.url ?? 'client';
  function close(err?: unknown) {
    if (ws.readyState !== ws.CLOSED && ws.readyState !== ws.CLOSING) {
      if (err) {
        closeWithError(lc, ws, err);
      } else {
        lc.info?.(`closing connection to ${endpoint}`);
        ws.close();
      }
    }
  }

  const instream = Subscription.create<In>({
    ...inOptions,
    cleanup: (unconsumed, err) => {
      inOptions.cleanup?.(unconsumed, err);
      close(err);
    },
  });
  const outstream = Subscription.create<Out>({
    ...outOptions,
    cleanup: (unconsumed, err) => {
      outOptions.cleanup?.(unconsumed, err);
      close(err);
    },
  });

  const duplex = createWebSocketStream(ws, {
    ...streamOptions,
    decodeStrings: false,
  });

  // Outgoing transform.
  function streamOut() {
    // Mainly used for verifying that back-pressure kicks in tests.
    duplex.on('drain', () => lc.debug?.(`drained messages to ${endpoint}`));

    pipeline(
      Readable.from(outstream),
      new Transform({
        objectMode: true,
        transform: (msg, _encoding, callback) =>
          callback(null, BigIntJSON.stringify(msg)),
      }),
      duplex,
      err => (err ? outstream.fail(err) : outstream.cancel()),
    );
  }

  if (ws.readyState === ws.CONNECTING) {
    ws.on('open', () => {
      lc.info?.(`connected to ${endpoint}`);
      streamOut();
    });
  } else {
    streamOut();
  }

  // Incoming transform.
  pipe({
    source: duplex,
    sink: instream,
    parse: chunk => {
      const json = BigIntJSON.parse(chunk.toString());
      return v.parse(json, inSchema, 'passthrough');
    },
  });

  sendPingsForLiveness(lc, ws, PING_INTERVAL_MS);

  return {outstream, instream};
}

type PipeOptions<T> = {
  source: Readable;
  sink: Subscription<T>;
  parse: (buffer: Buffer) => T | null;
  bufferMessages?: number;
};

export function pipe<T>({source, sink, parse, bufferMessages}: PipeOptions<T>) {
  bufferMessages ??= 0;
  assert(bufferMessages >= 0, 'bufferMessages must be non-negative');
  const pending = new HeadIndexedQueue<Promise<unknown>>();

  pipeline(
    source,
    new Writable({
      decodeStrings: false,
      write: (chunk, _encoding, callback) => {
        let msg: T | null;
        try {
          if ((msg = parse(chunk)) === null) {
            callback();
            return;
          }
        } catch (err) {
          callback(ensureError(err));
          return;
        }
        // Inbound backpressure is exerted by unconsumed messages in the
        // subscription. A buffer can be used to allow messages to queue up in
        // in the Subscription object, which allows the consumer to "peek" at
        // whether there are more messages immediately available
        // (via {@link Subscription.queued}.
        const {result} = sink.push(msg);
        pending.push(result);
        void result.then(() => pending.shift());

        if (pending.size <= bufferMessages) {
          // immediately allow more messages
          callback();
        } else {
          // wait for the oldest result in the pending queue
          const oldest = pending.peek();
          assert(oldest !== undefined, 'pending result expected');
          oldest.then(
            () => callback(),
            err => callback(ensureError(err)),
          );
        }
      },
      destroy: (err, callback) => {
        if (err) {
          sink.fail(ensureError(err));
        }
        // Otherwise, final will handle the cancel.
        callback();
      },
      final: callback => {
        sink.cancel();
        callback();
      },
    }),
    err => (err ? sink.fail(err) : sink.cancel()),
  );
}

function ensureError(err: unknown) {
  return err instanceof Error ? err : new Error(String(err));
}

type Ack = {ack: number};

function parseAck(data: unknown): Ack {
  if (typeof data !== 'string') {
    throw new Error('Expected string message');
  }
  const ack = JSON.parse(data) as {ack?: unknown};
  if (
    typeof ack.ack !== 'number' ||
    !Number.isSafeInteger(ack.ack) ||
    ack.ack < 0
  ) {
    throw new Error(`Invalid ack ${String(ack.ack)}`);
  }
  return {ack: ack.ack};
}

type Streamed<T> = {
  /** Application-level message. */
  msg: T;

  /** ID used for the Ack message. */
  id: number;
};

export type StringifiedStreamPayload = string | readonly string[];

export function streamOut<T extends JSONValue>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
  options: StreamOutOptions = {},
): Promise<void> {
  return streamOutInternal<T, T>(
    lc,
    source,
    sink,
    BigIntJSON.stringify,
    payload => [payload],
    options,
  );
}

/**
 * Streams out a `Source` for which messages are already stringified JSON.
 */
export function streamOutStringified(
  lc: LogContext,
  source: Source<StringifiedStreamPayload>,
  sink: WebSocket,
  options: StreamOutOptions = {},
): Promise<void> {
  return streamOutInternal<StringifiedStreamPayload, string>(
    lc,
    source,
    sink,
    json => json,
    payload => (typeof payload === 'string' ? [payload] : payload),
    options,
  );
}

type StreamOutOptions = {
  batch?: {maxMessages?: number | undefined} | undefined;
};

async function streamOutInternal<TPayload, TMessage extends JSONValue>(
  lc: LogContext,
  source: Source<TPayload>,
  sink: WebSocket,
  stringify: (payload: TMessage) => string,
  expandPayload: (payload: TPayload) => readonly TMessage[],
  options: StreamOutOptions,
): Promise<void> {
  sendPingsForLiveness(lc, sink, PING_INTERVAL_MS);

  const closer = WebSocketCloser.forSource(lc, sink, source);
  const acks = new Queue<Ack>();
  sink.addEventListener('message', ({data}) => {
    try {
      acks.enqueue(parseAck(data));
    } catch (e) {
      lc.error?.(`error parsing ack`, e);
      closer.close(e);
    }
  });

  try {
    let nextID = 0;
    const {pipeline} = source;
    if (pipeline) {
      const iterator = pipeline[Symbol.asyncIterator]();
      const maxBatchMessages = Math.max(1, options.batch?.maxMessages ?? 1);

      lc.debug?.(`started pipelined outbound stream`);
      for (;;) {
        const next = await iterator.next();
        if (next.done) {
          break;
        }
        const batch = [next.value];
        while (
          batch.length < maxBatchMessages &&
          source instanceof Subscription &&
          source.queued > 0
        ) {
          // Only drain work that is already queued. Waiting for future messages
          // just to fill a websocket frame would add latency to quiet streams
          // and status/error delivery.
          const queued = await iterator.next();
          if (queued.done) {
            break;
          }
          batch.push(queued.value);
        }
        const outbound = [];
        for (const {value, consumed} of batch) {
          const messages = expandPayload(value);
          if (messages.length === 0) {
            consumed();
            continue;
          }
          let remaining = messages.length;
          const consumeOne = () => {
            remaining--;
            if (remaining === 0) {
              // A single source payload can expand to several websocket IDs.
              // The source is consumed only after all IDs are ACKed.
              consumed();
            }
          };
          for (const msg of messages) {
            const id = ++nextID;
            outbound.push({id, msg});
            void (async () => {
              const {ack} = await acks.dequeue();
              // lc.debug?.(`received ack`, ack);
              if (ack !== id) {
                throw new Error(`Unexpected ack for ${id}: ${ack}`);
              }
              consumeOne();
            })().catch(e => {
              lc.error?.(`error handling ack`, e);
              closer.close(e);
            });
          }
        }
        if (outbound.length === 0) {
          continue;
        }
        const data = stringifyOutboundBatch(outbound, stringify);
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`pipelining`, data);
        sink.send(data);
      }
    } else {
      lc.debug?.(`started synchronous outbound stream`);
      for await (const payload of source) {
        const messages = expandPayload(payload);
        if (messages.length === 0) {
          continue;
        }
        const outbound = messages.map(msg => ({
          id: ++nextID,
          msg,
        }));
        const data = stringifyOutboundBatch(outbound, stringify);
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`sending`, data);
        sink.send(data);

        for (const {id} of outbound) {
          const {ack} = await acks.dequeue();
          if (ack !== id) {
            throw new Error(`Unexpected ack for ${id}: ${ack}`);
          }
        }
      }
    }
    closer.close();
  } catch (e) {
    closer.close(e);
  }
}

function stringifyOutboundBatch<T extends JSONValue>(
  batch: readonly {readonly id: number; readonly msg: T}[],
  stringify: (payload: T) => string,
) {
  if (batch.length === 1) {
    const [{id, msg}] = batch;
    return `{"id":${id},"msg":${stringify(msg)}}`;
  }
  // RM -> VS catchup emits large websocket batches. Building the frame in one
  // loop avoids the extra strings and array that `map().join()` creates in the
  // hottest fanout path.
  let data = '{"batch":[';
  for (let i = 0; i < batch.length; i++) {
    if (i > 0) {
      data += ',';
    }
    const {id, msg} = batch[i];
    data += `{"id":${id},"msg":${stringify(msg)}}`;
  }
  return `${data}]}`;
}

export function streamIn<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Promise<Source<T>> {
  return streamInInternal(lc, source, schema, false) as Promise<Source<T>>;
}

export function streamInBatches<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Promise<Source<StreamInPayload<T>>> {
  return streamInInternal(lc, source, schema, true);
}

async function streamInInternal<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
  preserveBatches: boolean,
): Promise<Source<StreamInPayload<T>>> {
  expectPingsForLiveness(lc, source, PING_INTERVAL_MS);

  const streamedSchema = v.object({
    msg: schema,
    id: v.number(),
  });

  const sink: Subscription<
    StreamInPayload<T>,
    Streamed<T> | readonly Streamed<T>[]
  > = new Subscription(
    {
      consumed: streamOrBatch => {
        if (isStreamedBatch(streamOrBatch)) {
          for (const {id} of streamOrBatch) {
            source.send(JSON.stringify({ack: id} satisfies Ack));
          }
        } else {
          source.send(JSON.stringify({ack: streamOrBatch.id} satisfies Ack));
        }
      },
      cleanup: () => closer.close(),
    },
    streamOrBatch =>
      isStreamedBatch(streamOrBatch)
        ? {
            tag: 'stream-batch',
            messages: streamOrBatch.map(({msg}) => msg),
          }
        : streamOrBatch.msg,
  );

  const closer = WebSocketCloser.forSink(lc, source, sink, handleMessage);

  function handleMessage(event: MessageEvent) {
    const data = webSocketMessageDataToString(event.data);
    if (!sink.active) {
      lc.warn?.('dropping ws message received after close', data);
      return;
    }
    try {
      const value = BigIntJSON.parse(data);
      const messages = parseStreamedMessages(value, streamedSchema);
      if (preserveBatches && messages.length > 1) {
        // The RM already grouped these messages into one websocket frame. Keep
        // that boundary visible so the VS can batch one write-worker apply.
        sink.push(messages);
        return;
      }
      for (const msg of messages) {
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`received`, data);
        sink.push(msg);
      }
    } catch (e) {
      closer.close(e);
    }
  }

  await closer.connected;
  return sink;
}

function webSocketMessageDataToString(data: MessageEvent['data']): string {
  if (typeof data === 'string') {
    return data;
  }
  if (Buffer.isBuffer(data)) {
    return data.toString();
  }
  if (data instanceof ArrayBuffer) {
    return Buffer.from(data).toString();
  }
  return Buffer.concat(data).toString();
}

function isStreamedBatch<T>(
  streamOrBatch: Streamed<T> | readonly Streamed<T>[],
): streamOrBatch is readonly Streamed<T>[] {
  return Array.isArray(streamOrBatch);
}

function parseStreamedMessages<T extends JSONValue>(
  value: JSONValue,
  streamedSchema: v.Type<Streamed<T>>,
): Streamed<T>[] {
  if (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    Object.hasOwn(value, 'batch')
  ) {
    // A batch frame is a transport optimization only. Validate each child
    // message against the normal streamed schema before handing it to callers.
    const batch = (value as {readonly batch?: unknown}).batch;
    if (!Array.isArray(batch)) {
      throw new Error('Invalid stream batch');
    }
    const messages: Streamed<T>[] = [];
    messages.length = batch.length;
    for (let i = 0; i < batch.length; i++) {
      messages[i] = v.parse(batch[i], streamedSchema, 'passthrough');
    }
    return messages;
  }
  return [v.parse(value, streamedSchema, 'passthrough')];
}

class WebSocketCloser {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #closeStream: () => void;
  readonly #messageHandler: ((e: MessageEvent) => void | undefined) | null;
  readonly #connected = resolver();

  get connected(): Promise<void> {
    return this.#connected.promise;
  }

  static forSource<T>(lc: LogContext, ws: WebSocket, stream: Source<T>) {
    // If the websocket is closed, call cancel() to notify the Source of
    // any unconsumed messages.
    return new WebSocketCloser(lc, ws, () => stream.cancel());
  }

  static forSink<T, M>(
    lc: LogContext,
    ws: WebSocket,
    stream: Subscription<T, M>,
    messageHandler: (e: MessageEvent) => void | undefined,
  ) {
    // If the websocket is closed, call end() to allow the downstream Sink
    // to process any pending messages before closing the stream.
    return new WebSocketCloser(lc, ws, () => stream.end(), messageHandler);
  }

  private constructor(
    lc: LogContext,
    ws: WebSocket,
    closeStream: () => void,
    messageHandler?: (e: MessageEvent) => void | undefined,
  ) {
    this.#lc = lc;
    this.#ws = ws;
    this.#closeStream = closeStream;
    this.#messageHandler = messageHandler ?? null;

    ws.addEventListener('open', this.#handleOpen);
    ws.addEventListener('close', this.#handleClose);
    ws.addEventListener('error', this.#handleError);
    if (this.#messageHandler) {
      ws.addEventListener('message', this.#messageHandler);
    }

    switch (ws.readyState) {
      case ws.CONNECTING:
        break; // expected for new connections. resolve or reject in handlers.
      case ws.OPEN:
        this.#connected.resolve();
        break;
      default:
        this.#connected.reject(
          new Error(`websocket already in state ${ws.readyState}`),
        );
        break;
    }
  }

  get #conn(): string {
    return 'connection' + (this.#ws.url ? ` to ${this.#ws.url}` : '');
  }

  #handleOpen = () => {
    this.#lc.info?.(`${this.#conn} established`);
    this.#connected.resolve();
  };

  #handleClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    this.#lc.info?.(`${this.#conn} closed`, {
      code,
      reason,
      wasClean,
    });
    this.close();
    this.#connected.reject(`${this.#conn} closed with code ${code}`);
  };

  #handleError = ({message, error}: ErrorEvent) => {
    if (this.#ws.readyState === this.#ws.OPEN) {
      this.#lc.error?.(`error in ${this.#conn}`, message, error);
    }
    this.#connected.reject(error);
  };

  close(err?: unknown) {
    if (err) {
      this.#lc.error?.(`closing stream with error`, err);
    }
    this.#closeStream();
    if (!this.closed()) {
      this.#ws.close();
    }
  }

  closed() {
    return (
      this.#ws.readyState === this.#ws.CLOSED ||
      this.#ws.readyState === this.#ws.CLOSING
    );
  }
}
