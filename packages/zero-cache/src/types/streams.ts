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
// #6001: https://github.com/rocicorp/mono/pull/6001
// Batch stream ACKs so row-heavy RM -> serving-replica traffic spends CPU on
// apply work instead of ACK writes. Quiet streams still flush on the short timer.
const CUMULATIVE_ACK_EVERY = 128;
const CUMULATIVE_ACK_INTERVAL_MS = 5;

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

  try {
    let nextID = 0;
    const {pipeline} = source;
    if (pipeline) {
      const iterator = pipeline[Symbol.asyncIterator]();
      const maxBatchMessages = Math.max(1, options.batch?.maxMessages ?? 1);
      let lastAck = 0;
      const pending = new Map<number, () => void>();
      sink.addEventListener('message', ({data}) => {
        try {
          const {ack} = parseAck(data);
          if (ack < lastAck) {
            throw new Error(`Ack moved backwards from ${lastAck} to ${ack}`);
          }
          if (ack > nextID) {
            if (nextID === 0 && pending.size === 0) {
              return;
            }
            throw new Error(
              `Unexpected ack ${ack}; only sent through ${nextID}`,
            );
          }
          if (ack === lastAck) {
            return;
          }
          lastAck = ack;
          // #6001: https://github.com/rocicorp/mono/pull/6001
          // RM -> VS catchup can keep thousands of future stream IDs pending;
          // ACK cleanup stops at the first future ID so each ACK only pays for
          // frames that can actually be released.
          for (const [id, consumed] of pending) {
            if (id > ack) {
              break;
            }
            consumed();
            pending.delete(id);
          }
        } catch (e) {
          lc.error?.(`error handling ack`, e);
          closer.close(e);
        }
      });

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
              consumed();
            }
          };
          for (const msg of messages) {
            const id = ++nextID;
            pending.set(id, consumeOne);
            outbound.push({id, msg});
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
      const acks = new Queue<Ack>();
      sink.addEventListener('message', ({data}) => {
        try {
          acks.enqueue(parseAck(data));
        } catch (e) {
          lc.error?.(`error parsing ack`, e);
          closer.close(e);
        }
      });

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
  // #6001: https://github.com/rocicorp/mono/pull/6001
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
  options: StreamInOptions = {},
): Promise<Source<T>> {
  return streamInInternal(lc, source, schema, options, false) as Promise<
    Source<T>
  >;
}

export function streamInBatches<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
  options: StreamInOptions = {},
): Promise<Source<StreamInPayload<T>>> {
  return streamInInternal(lc, source, schema, options, true);
}

async function streamInInternal<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
  options: StreamInOptions,
  preserveBatches: boolean,
): Promise<Source<StreamInPayload<T>>> {
  expectPingsForLiveness(lc, source, PING_INTERVAL_MS);

  const streamedSchema = v.object({
    msg: schema,
    id: v.number(),
  });
  const acker = new CumulativeAcker(source, options.ack === 'cumulative');

  const sink: Subscription<
    StreamInPayload<T>,
    Streamed<T> | readonly Streamed<T>[]
  > = new Subscription(
    {
      consumed: streamOrBatch => {
        if (isStreamedBatch(streamOrBatch)) {
          acker.consumedBatch(streamOrBatch.map(({id}) => id));
        } else {
          acker.consumed(streamOrBatch.id);
        }
      },
      cleanup: () => {
        acker.close();
        closer.close();
      },
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
    const data = event.data.toString();
    if (!sink.active) {
      lc.warn?.('dropping ws message received after close', data);
      return;
    }
    try {
      const value = BigIntJSON.parse(data);
      const messages = parseStreamedMessages(value, streamedSchema);
      for (const msg of messages) {
        acker.received(msg.id);
      }
      if (preserveBatches && messages.length > 1) {
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
    const batch = (value as {readonly batch?: unknown}).batch;
    if (!Array.isArray(batch)) {
      throw new Error('Invalid stream batch');
    }
    return batch.map(msg => v.parse(msg, streamedSchema, 'passthrough'));
  }
  return [v.parse(value, streamedSchema, 'passthrough')];
}

type StreamInOptions = {
  ack?: 'per-message' | 'cumulative' | undefined;
};

class CumulativeAcker {
  readonly #ws: WebSocket;
  readonly #batch: boolean;
  readonly #outOfOrder = new Set<number>();
  #highestReceived = 0;
  #highestAckable = 0;
  #lastSent = 0;
  #pendingSinceLastSend = 0;

  #timer: NodeJS.Timeout | undefined;

  constructor(ws: WebSocket, batch: boolean) {
    this.#ws = ws;
    this.#batch = batch;
  }

  received(id: number) {
    this.#validateID(id);
    this.#highestReceived = Math.max(this.#highestReceived, id);
  }

  consumed(id: number) {
    this.#validateID(id);
    if (id <= this.#highestAckable) {
      return;
    }
    const previous = this.#highestAckable;
    if (id === this.#highestAckable + 1) {
      this.#highestAckable = id;
      while (this.#outOfOrder.delete(this.#highestAckable + 1)) {
        this.#highestAckable++;
      }
    } else {
      this.#outOfOrder.add(id);
    }
    const advanced = this.#highestAckable - previous;
    if (advanced === 0) {
      return;
    }
    this.#pendingSinceLastSend += advanced;
    if (
      this.#batch &&
      this.#pendingSinceLastSend < CUMULATIVE_ACK_EVERY &&
      !(
        this.#pendingSinceLastSend > 1 &&
        this.#highestAckable === this.#highestReceived
      )
    ) {
      this.#schedule();
    } else {
      this.flush();
    }
  }

  consumedBatch(ids: readonly number[]) {
    if (ids.length === 0) {
      return;
    }
    let expected = this.#highestAckable + 1;
    for (const id of ids) {
      this.#validateID(id);
      if (id !== expected) {
        for (const fallbackID of ids) {
          this.consumed(fallbackID);
        }
        return;
      }
      expected++;
    }

    const previous = this.#highestAckable;
    const last = ids.at(-1);
    assert(last !== undefined, 'non-empty batch must have a last id');
    this.#highestAckable = last;
    while (this.#outOfOrder.delete(this.#highestAckable + 1)) {
      this.#highestAckable++;
    }
    const advanced = this.#highestAckable - previous;
    if (advanced === 0) {
      return;
    }
    this.#pendingSinceLastSend += advanced;
    this.flush();
  }

  flush() {
    this.#clearTimer();
    if (this.#highestAckable <= this.#lastSent) {
      return;
    }
    if (this.#ws.readyState !== this.#ws.OPEN) {
      return;
    }
    this.#ws.send(JSON.stringify({ack: this.#highestAckable} satisfies Ack));
    this.#lastSent = this.#highestAckable;
    this.#pendingSinceLastSend = 0;
  }

  close() {
    this.flush();
    this.#clearTimer();
  }

  #schedule() {
    this.#timer ??= setTimeout(() => this.flush(), CUMULATIVE_ACK_INTERVAL_MS);
  }

  #clearTimer() {
    if (this.#timer !== undefined) {
      clearTimeout(this.#timer);
      this.#timer = undefined;
    }
  }

  #validateID(id: number) {
    if (!Number.isSafeInteger(id) || id <= 0) {
      throw new Error(`Invalid stream message id ${id}`);
    }
  }
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
