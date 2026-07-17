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
  const pending: Promise<unknown>[] = [];

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

        if (pending.length <= bufferMessages) {
          // immediately allow more messages
          callback();
        } else {
          // wait for the oldest result in the pending queue
          pending[0].then(
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

const ackSchema = v.object({ack: v.number()});

type Ack = v.Infer<typeof ackSchema>;

type Streamed<T> = {
  /** Application-level message. */
  msg: T;

  /** ID used for the Ack message. */
  id: number;
};

export function streamOut<T extends JSONValue>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
): Promise<void> {
  return streamOutInternal(lc, source, sink, BigIntJSON.stringify);
}

/**
 * Streams out a `Source` for which messages are already stringified JSON.
 */
export function streamOutStringified(
  lc: LogContext,
  source: Source<string>,
  sink: WebSocket,
): Promise<void> {
  return streamOutInternal(lc, source, sink, json => json);
}

export type BatchOptions<T> = {
  /** Maximum number of logical messages in one WebSocket frame. */
  maxMessages: number;
  /** Approximate maximum serialized payload size of one WebSocket frame. */
  maxBytes: number;
  /** Maximum time to wait for more messages after receiving the first one. */
  flushIntervalMs: number;
  /** Flushes the current batch immediately after matching messages. */
  flushAfter?: ((value: T) => boolean) | undefined;
};

type BatchEnvelope<T> = {batch: T[]};

/**
 * Streams already-stringified JSON messages in bounded batches. Each batch is
 * transported and ACKed as one WebSocket message. The individual source
 * entries are marked consumed only after that batch ACK is received.
 */
export function streamOutStringifiedBatched(
  lc: LogContext,
  source: Source<string>,
  sink: WebSocket,
  options: BatchOptions<string>,
): Promise<void> {
  return streamOutInternal(lc, source, sink, json => json, options);
}

async function streamOutInternal<T extends JSONValue>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
  stringify: (payload: T) => string,
  batchOptions?: BatchOptions<T>,
): Promise<void> {
  sendPingsForLiveness(lc, sink, PING_INTERVAL_MS);

  const closer = WebSocketCloser.forSource(lc, sink, source);

  const acks = new Queue<Ack>();
  sink.addEventListener('message', ({data}) => {
    try {
      if (typeof data !== 'string') {
        throw new Error('Expected string message');
      }
      acks.enqueue(v.parse(JSON.parse(data), ackSchema));
    } catch (e) {
      lc.error?.(`error parsing ack`, e);
      closer.close(e);
    }
  });

  try {
    let nextID = 0;
    const {pipeline} = source;
    if (pipeline) {
      lc.debug?.(`started pipelined outbound stream`);
      const outbound = batchOptions
        ? batchPipeline(pipeline, stringify, batchOptions)
        : stringifyPipeline(pipeline, stringify);
      for await (const {json, consumed} of outbound) {
        const id = ++nextID;
        const data = `{"id":${id},"msg":${json}}`;
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`pipelining`, data);
        sink.send(data);

        void (async () => {
          const {ack} = await acks.dequeue();
          // lc.debug?.(`received ack`, ack);
          if (ack !== id) {
            throw new Error(`Unexpected ack for ${id}: ${ack}`);
          }
          consumed();
        })().catch(e => closer.close(e));
      }
    } else {
      lc.debug?.(`started synchronous outbound stream`);
      for await (const msg of source) {
        const id = ++nextID;
        const data = `{"id":${id},"msg":${stringify(msg)}}`;
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`sending`, data);
        sink.send(data);

        const {ack} = await acks.dequeue();
        if (ack !== id) {
          throw new Error(`Unexpected ack for ${id}: ${ack}`);
        }
      }
    }
    closer.close();
  } catch (e) {
    closer.close(e);
  }
}

type PipelineEntry<T> = {value: T; consumed: () => void};
type SerializedPipelineEntry = {json: string; consumed: () => void};

async function* stringifyPipeline<T>(
  pipeline: AsyncIterable<PipelineEntry<T>>,
  stringify: (payload: T) => string,
): AsyncGenerator<SerializedPipelineEntry> {
  for await (const {value, consumed} of pipeline) {
    yield {json: stringify(value), consumed};
  }
}

async function* batchPipeline<T>(
  pipeline: AsyncIterable<PipelineEntry<T>>,
  stringify: (payload: T) => string,
  options: BatchOptions<T>,
): AsyncGenerator<SerializedPipelineEntry> {
  const {maxMessages, maxBytes, flushIntervalMs, flushAfter} = options;
  assert(maxMessages > 0, 'maxMessages must be positive');
  assert(maxBytes > 0, 'maxBytes must be positive');
  assert(flushIntervalMs >= 0, 'flushIntervalMs must be non-negative');

  const iterator = pipeline[Symbol.asyncIterator]();
  let pendingNext: Promise<IteratorResult<PipelineEntry<T>>> | undefined;
  let sourceDone = false;

  try {
    while (!sourceDone) {
      const first = await (pendingNext ?? iterator.next());
      pendingNext = undefined;
      if (first.done) {
        break;
      }

      const entries = [first.value];
      const json = [stringify(first.value.value)];
      let bytes = json[0].length;

      if (!flushAfter?.(first.value.value)) {
        let flushTimer: ReturnType<typeof setTimeout> | undefined;
        const flush = new Promise<'flush'>(resolve => {
          flushTimer = setTimeout(resolve, flushIntervalMs, 'flush');
        });

        try {
          while (entries.length < maxMessages && bytes < maxBytes) {
            pendingNext ??= iterator.next();
            const result = await Promise.race([
              pendingNext.then(value => ({kind: 'next' as const, value})),
              flush.then(kind => ({kind})),
            ]);
            if (result.kind === 'flush') {
              break;
            }

            pendingNext = undefined;
            if (result.value.done) {
              sourceDone = true;
              break;
            }

            const entry = result.value.value;
            const serialized = stringify(entry.value);
            entries.push(entry);
            json.push(serialized);
            bytes += serialized.length + 1; // comma separator
            if (flushAfter?.(entry.value)) {
              break;
            }
          }
        } finally {
          clearTimeout(flushTimer);
        }
      }

      yield {
        json: `{"batch":[${json.join(',')}]}`,
        consumed: () => entries.forEach(entry => entry.consumed()),
      };
    }
  } finally {
    await iterator.return?.();
  }
}

export async function streamIn<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Promise<Source<T>> {
  expectPingsForLiveness(lc, source, PING_INTERVAL_MS);

  const streamedSchema = v.object({
    msg: schema,
    id: v.number(),
  });

  const sink: Subscription<T, Streamed<T>> = new Subscription<T, Streamed<T>>(
    {
      consumed: ({id}) => source.send(JSON.stringify({ack: id} satisfies Ack)),
      cleanup: () => closer.close(),
    },
    ({msg}) => msg,
  );

  const closer = WebSocketCloser.forSink(lc, source, sink, handleMessage);

  function handleMessage(event: MessageEvent) {
    const data = event.data.toString();
    if (!sink.active) {
      lc.warn?.('dropping ws message received after close', data);
      return;
    }
    try {
      const value = BigIntJSON.parse(data);
      const msg = v.parse(value, streamedSchema, 'passthrough');
      // Enable for debugging. Otherwise too verbose.
      // lc.debug?.(`received`, data);
      sink.push(msg);
    } catch (e) {
      closer.close(e);
    }
  }

  await closer.connected;
  return sink;
}

/**
 * Receives either legacy single-message frames or capability-gated batch
 * envelopes and exposes both as the original logical message stream.
 */
export async function streamInBatched<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Promise<Source<T>> {
  const envelopeSchema = v.object({batch: v.array(schema)});
  const wireSource = await streamIn(
    lc,
    source,
    v.union(schema, envelopeSchema),
  );
  return flattenBatchEnvelopes(wireSource);
}

function flattenBatchEnvelopes<T extends JSONValue>(
  source: Source<T | BatchEnvelope<T>>,
): Source<T> {
  return {
    cancel: err => source.cancel(err),
    [Symbol.asyncIterator]() {
      const delegate = source[Symbol.asyncIterator]();
      let batch: T[] | undefined;
      let index = 0;

      return {
        async next(): Promise<IteratorResult<T>> {
          if (batch && index < batch.length) {
            return {value: batch[index++], done: false};
          }

          const next = await delegate.next();
          if (next.done) {
            return {value: next.value as T, done: true};
          }
          batch = isBatchEnvelope(next.value) ? next.value.batch : [next.value];
          index = 0;
          assert(batch.length > 0, 'batch envelope must not be empty');
          return {value: batch[index++], done: false};
        },

        return(value?: unknown): Promise<IteratorResult<T>> {
          if (batch && index < batch.length) {
            // Do not ACK a partially processed batch. Closing the stream makes
            // the producer treat every entry in the frame as unconsumed.
            source.cancel();
            return Promise.resolve({value: value as T, done: true});
          }
          if (delegate.return) {
            return delegate
              .return(value as T)
              .then(result => ({value: result.value as T, done: true}));
          }
          source.cancel();
          return Promise.resolve({value: value as T, done: true});
        },
      };
    },
  };
}

function isBatchEnvelope<T>(
  value: T | BatchEnvelope<T>,
): value is BatchEnvelope<T> {
  return (
    typeof value === 'object' &&
    value !== null &&
    !Array.isArray(value) &&
    'batch' in value
  );
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

  static forSink<T>(
    lc: LogContext,
    ws: WebSocket,
    stream: Subscription<T, Streamed<T>>,
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
