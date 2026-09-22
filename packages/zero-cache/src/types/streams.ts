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
import {closeWithError, sendPingsForLiveness} from './ws.ts';

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
   * An AbortSignal that can be used to listen for termination or
   * race a short-lived promise against the termination of the iterable
   * (via `promiseOrAbort()`)
   */
  readonly signal: AbortSignal;

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

  /**
   * Pipelined batching support: eagerly drains available queued messages up to `maxBatch`.
   */
  pipelineBatched?:
    | ((
        maxBatch?: number,
      ) => AsyncIterable<{values: T[]; consumed: () => void}> | undefined)
    | undefined;
};

export type Sink<T> = {
  push(message: T): void;
};

/**
 * Back-pressure-aware transformation of a WebSocket into
 * upstream and downstream {@link Subscription} objects.
 *
 * This is used for connections with external servers
 * (i.e. custom change sources). For inter-zero-cache communication,
 * see {@link streamInternal}.
 */
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

/** A parsed value paired with its approximate serialized transport size. */
export type Sized<T> = {
  data: T;
  size: number;
};

export type StreamOutOptions = {
  batched?: boolean | undefined;
  maxBatchSize?: number | undefined;
};

export type PreSerialized = {
  readonly payload: Buffer;
  readonly byteLength: number;
};

export function isPreSerialized(val: unknown): val is PreSerialized {
  return (
    typeof val === 'object' &&
    val !== null &&
    'payload' in val &&
    Buffer.isBuffer((val as PreSerialized).payload)
  );
}

function sendTextFrame(sink: WebSocket, data: Buffer | string) {
  if (typeof data === 'string') {
    sink.send(data);
  } else {
    (sink as unknown as {send: (data: unknown, opts?: unknown) => void}).send(
      data,
      {binary: false},
    );
  }
}

/**
 * Establishes a bidirectional stream over an internal zero-cache to zero-cache
 * WebSocket. Both peers call this: each may send an outbound {@link Source}
 * (`outSource`) and receives an inbound {@link Source} (the return value).
 * Transport-level `{id, msg}` / `{ack}` framing is multiplexed in both
 * directions over the single socket.
 *
 * `inSchema` validates inbound application frames; it may be omitted by a peer
 * that expects no inbound application messages (any that arrive are a protocol
 * error).
 */
export function streamInternal<
  In extends JSONValue,
  Out extends JSONValue = JSONValue,
>(
  lc: LogContext,
  ws: WebSocket,
  inSchema: v.Type<In> | undefined,
  outSource?: Source<Out>,
  options?: StreamOutOptions,
): Promise<Source<In>> {
  return streamInternalCore(
    lc,
    ws,
    inSchema,
    outSource,
    BigIntJSON.stringify,
    data => data,
    options,
  );
}

/**
 * {@link streamInternal} whose outbound `Source` carries messages that are
 * already stringified JSON or pre-serialized Buffers.
 */
export function streamInternalStringified<In extends JSONValue>(
  lc: LogContext,
  ws: WebSocket,
  inSchema: v.Type<In> | undefined,
  outSource?: Source<string | PreSerialized>,
  options?: StreamOutOptions,
): Promise<Source<In>> {
  return streamInternalCore(
    lc,
    ws,
    inSchema,
    outSource,
    msg => (typeof msg === 'string' ? msg : msg.payload.toString('utf8')),
    data => data,
    options,
  );
}

/**
 * {@link streamInternal} that retains only the transport-frame size of each
 * inbound message. The size bounds downstream batching without keeping or
 * copying the JSON.
 */
export function streamInternalWithSize<
  In extends JSONValue,
  Out extends JSONValue = JSONValue,
>(
  lc: LogContext,
  ws: WebSocket,
  inSchema: v.Type<In>,
  outSource?: Source<Out>,
  options?: StreamOutOptions,
): Promise<Source<Sized<In>>> {
  return streamInternalCore(
    lc,
    ws,
    inSchema,
    outSource,
    BigIntJSON.stringify,
    (data, _frame, _id, size) => ({data, size}),
    options,
  );
}

/**
 * Runs the outbound send loop of a {@link streamInternal} connection: reads
 * messages from `source`, frames them as `{id, msg}` / `{id, batch}` (or
 * pre-serialized Buffers), and awaits the corresponding `{ack}` frames — which
 * are demuxed off the socket by the shared message handler in
 * {@link streamInternalCore} and delivered via the `acks` queue.
 */
async function runOutbound<T extends JSONValue | PreSerialized>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
  stringify: (payload: T) => string,
  acks: Queue<Ack>,
  closer: WebSocketCloser,
  options?: StreamOutOptions | undefined,
): Promise<void> {
  try {
    let nextID = 0;
    const {pipeline} = source;
    const batched = options?.batched ?? false;
    const maxBatchSize = Math.max(1, Math.floor(options?.maxBatchSize ?? 64));

    if (batched && source.pipelineBatched) {
      const batchedIterable = source.pipelineBatched(maxBatchSize);
      if (batchedIterable) {
        lc.debug?.(
          `started batched outbound stream (maxBatchSize=${maxBatchSize})`,
        );
        for await (const {values, consumed} of batchedIterable) {
          if (values.length === 1 && isPreSerialized(values[0])) {
            const id = ++nextID;
            const prefix = Buffer.from(`{"id":${id}`);
            const data = Buffer.concat([prefix, values[0].payload]);
            sendTextFrame(sink, data);

            void (async () => {
              const {ack} = await acks.dequeue();
              if (ack !== id) {
                throw new Error(`Unexpected ack for ${id}: ${ack}`);
              }
              consumed();
            })().catch(e => closer.close(e));
          } else if (values.some(isPreSerialized)) {
            let remaining = values.length;
            const onConsumed = () => {
              if (--remaining === 0) {
                consumed();
              }
            };
            for (const val of values) {
              const id = ++nextID;
              if (isPreSerialized(val)) {
                const prefix = Buffer.from(`{"id":${id}`);
                const data = Buffer.concat([prefix, val.payload]);
                sendTextFrame(sink, data);
              } else {
                const data = `{"id":${id},"msg":${stringify(val)}}`;
                sink.send(data);
              }
              void (async () => {
                const {ack} = await acks.dequeue();
                if (ack !== id) {
                  throw new Error(`Unexpected ack for ${id}: ${ack}`);
                }
                onConsumed();
              })().catch(e => closer.close(e));
            }
          } else {
            const id = ++nextID;
            const data =
              values.length === 1
                ? `{"id":${id},"msg":${stringify(values[0])}}`
                : `{"id":${id},"batch":[${values.map(stringify).join(',')}]}`;
            sink.send(data);

            void (async () => {
              const {ack} = await acks.dequeue();
              if (ack !== id) {
                throw new Error(`Unexpected ack for ${id}: ${ack}`);
              }
              consumed();
            })().catch(e => closer.close(e));
          }
        }
        closer.close();
        return;
      }
    }

    if (pipeline) {
      lc.debug?.(`started pipelined outbound stream`);
      for await (const {value: msg, consumed} of pipeline) {
        const id = ++nextID;
        if (isPreSerialized(msg)) {
          const prefix = Buffer.from(`{"id":${id}`);
          const data = Buffer.concat([prefix, msg.payload]);
          sendTextFrame(sink, data);
        } else {
          const data = `{"id":${id},"msg":${stringify(msg)}}`;
          // Enable for debugging. Otherwise too verbose.
          // lc.debug?.(`pipelining`, data);
          sink.send(data);
        }

        // The ack is awaited off the send loop so that the next message can be
        // sent without waiting for it. A bad ack is a protocol error like in
        // the synchronous path below: close the socket (which cancels the
        // source) rather than leaving the rejection unhandled.
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
        if (isPreSerialized(msg)) {
          const prefix = Buffer.from(`{"id":${id}`);
          const data = Buffer.concat([prefix, msg.payload]);
          sendTextFrame(sink, data);
        } else {
          const data = `{"id":${id},"msg":${stringify(msg)}}`;
          // Enable for debugging. Otherwise too verbose.
          // lc.debug?.(`sending`, data);
          sink.send(data);
        }

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

async function streamInternalCore<
  In extends JSONValue,
  Out extends JSONValue | PreSerialized,
  TIn,
>(
  lc: LogContext,
  ws: WebSocket,
  inSchema: v.Type<In> | undefined,
  outSource: Source<Out> | undefined,
  stringify: (payload: Out) => string,
  transform: (data: In, frame: string, id: number, size: number) => TIn,
  options: StreamOutOptions | undefined,
): Promise<Source<TIn>> {
  sendPingsForLiveness(lc, ws, PING_INTERVAL_MS);

  // Acks received from the peer for the messages we send on `outSource`.
  const acks = new Queue<Ack>();

  const streamedSchema = inSchema
    ? v.object({
        id: v.number(),
        msg: inSchema.optional(),
        batch: v.array(inSchema).optional(),
      })
    : undefined;

  type SinkEntry = {
    consumed: () => void;
    data: TIn;
  };

  const sink: Subscription<TIn, SinkEntry> = new Subscription<TIn, SinkEntry>(
    {
      consumed: ({consumed}) => consumed(),
      cleanup: () => closer.close(),
    },
    ({data}) => data,
  );

  const closer = WebSocketCloser.forDuplex(
    lc,
    ws,
    outSource,
    sink,
    handleMessage,
  );

  function handleMessage(event: MessageEvent) {
    const data = event.data.toString();
    if (!sink.active) {
      lc.warn?.('dropping ws message received after close');
      return;
    }
    try {
      const value = BigIntJSON.parse(data);

      // Demux: an `{ack}` frame acknowledges a message we sent on `outSource`.
      const parsedAck = v.test(value, ackSchema);
      if (parsedAck.ok) {
        acks.enqueue(parsedAck.value);
        return;
      }

      // Otherwise it is an inbound application frame.
      if (!streamedSchema) {
        closer.close(new Error(`unexpected inbound message: ${data}`));
        return;
      }
      const parsed = v.parse(value, streamedSchema, 'passthrough');
      const {id, msg, batch} = parsed;

      const sendAck = () => {
        if (ws.readyState === ws.OPEN) {
          ws.send(JSON.stringify({ack: id} satisfies Ack));
        }
      };

      if (batch !== undefined && msg !== undefined) {
        throw new Error(`Message ${id} has both "msg" and "batch"`);
      }

      if (batch !== undefined) {
        let remaining = batch.length;
        if (remaining === 0) {
          sendAck();
          return;
        }
        const onConsumed = () => {
          if (--remaining === 0) {
            sendAck();
          }
        };
        const itemSize = Math.max(1, Math.round(data.length / batch.length));
        for (const item of batch) {
          sink.push({
            consumed: onConsumed,
            data: transform(item, data, id, itemSize),
          });
        }
      } else if (msg !== undefined) {
        sink.push({
          consumed: sendAck,
          data: transform(msg, data, id, data.length),
        });
      } else {
        throw new Error(`Message ${id} has neither "msg" nor "batch"`);
      }
    } catch (e) {
      closer.close(e);
    }
  }

  await closer.connected;

  // Start the outbound send loop only after the connection is open, so that a
  // client whose socket begins in CONNECTING can push before it connects.
  if (outSource) {
    void runOutbound(lc, outSource, ws, stringify, acks, closer, options);
  }

  return sink;
}

class WebSocketCloser {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #closeStream: (err?: unknown) => void;
  readonly #messageHandler: ((e: MessageEvent) => void | undefined) | null;
  readonly #connected = resolver();

  get connected(): Promise<void> {
    return this.#connected.promise;
  }

  /**
   * Closer for a {@link streamInternalCore} duplex. On socket close it cancels
   * the outbound `Source` (notifying it of unconsumed messages, as the old
   * `forSource` did) and fails/ends the inbound `Sink` (as the old `forSink`
   * did): fail() on error so consumers throw, end() otherwise so pending
   * messages finish. Either stream may be absent for a one-directional peer.
   */
  static forDuplex<In, InInput, Out>(
    lc: LogContext,
    ws: WebSocket,
    outSource: Source<Out> | undefined,
    inSink: Subscription<In, InInput>,
    messageHandler: (e: MessageEvent) => void | undefined,
  ) {
    return new WebSocketCloser(
      lc,
      ws,
      (err?: unknown) => {
        outSource?.cancel(err instanceof Error ? err : undefined);
        if (err) {
          inSink.fail(err instanceof Error ? err : new Error(String(err)));
        } else {
          inSink.end();
        }
      },
      messageHandler,
    );
  }

  private constructor(
    lc: LogContext,
    ws: WebSocket,
    closeStream: (err?: unknown) => void,
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
    this.#closeStream(err);
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
