import type {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {pipeline, Writable, type DuplexOptions} from 'node:stream';
import {
  createWebSocketStream,
  type CloseEvent,
  type ErrorEvent,
  type MessageEvent,
  type WebSocket,
} from 'ws';
import {Queue} from '../../../shared/src/queue.ts';
import * as v from '../../../shared/src/valita.ts';
import {BigIntJSON, type JSONValue} from './bigint-json.ts';
import {Subscription, type Options} from './subscription.ts';
import {closeWithError} from './ws.ts';

export type Source<T> = AsyncIterable<T> & {
  /**
   * Immediately terminates all current iterations (i.e. {@link AsyncIterator.next next()})
   * will return `{value: undefined, done: true}`), and prevents any subsequent iterations
   * from yielding any values.
   */
  cancel: () => void;

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
        lc.info?.(`closing conneciton to ${endpoint}`);
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
  async function sendOut() {
    for await (const msg of outstream) {
      // Upstream backpressure is signaled by the tcp socket buffer.
      if (!duplex.write(BigIntJSON.stringify(msg))) {
        const {promise: canWriteMore, resolve: drained} = resolver();

        const start = Date.now();
        duplex.once('drain', drained);
        await canWriteMore;
        lc.debug?.(
          `drained messages to ${endpoint} in ${Date.now() - start} ms`,
        );
      }
    }
  }

  if (ws.readyState === ws.CONNECTING) {
    ws.on('open', () => {
      lc.info?.(`connected to ${endpoint}`);
      void sendOut();
    });
  } else {
    void sendOut();
  }

  // Incoming transform.
  pipeline(
    duplex,
    new Writable({
      decodeStrings: false,
      write: (chunk, _encoding, callback) => {
        let msg: In;
        try {
          const json = BigIntJSON.parse(chunk.toString());
          msg = v.parse(json, inSchema, 'passthrough');
        } catch (err) {
          callback(ensureError(err));
          return;
        }
        const {result} = instream.push(msg);
        // Inbound backpressure is exerted by unconsumed
        // messages in the subscription.
        void result.then(
          () => callback(),
          err => callback(err),
        );
      },
      destroy: (err, callback) => {
        instream.fail(ensureError(err));
        callback();
      },
      final: callback => {
        instream.cancel();
        callback();
      },
    }),
    close,
  );

  return {outstream, instream};
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

export async function streamOut<T extends JSONValue>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
): Promise<void> {
  const closer = new WebSocketCloser(lc, sink, source);

  const acks = new Queue<Ack>();
  sink.addEventListener('message', ({data}) => {
    try {
      if (typeof data !== 'string') {
        throw new Error('Expected string message');
      }
      void acks.enqueue(v.parse(JSON.parse(data), ackSchema));
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
      for await (const {value: msg, consumed} of pipeline) {
        const id = ++nextID;
        const data = BigIntJSON.stringify({msg, id} satisfies Streamed<T>);
        // Enable for debugging. Otherwise too verbose.
        // lc.debug?.(`pipelining`, data);
        sink.send(data);

        void acks.dequeue().then(({ack}) => {
          // lc.debug?.(`received ack`, ack);
          if (ack !== id) {
            throw new Error(`Unexpected ack for ${id}: ${ack}`);
          }
          consumed();
        });
      }
    } else {
      lc.debug?.(`started synchronous outbound stream`);
      for await (const msg of source) {
        const id = ++nextID;
        const data = BigIntJSON.stringify({msg, id} satisfies Streamed<T>);
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

export async function streamIn<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Promise<Source<T>> {
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

  const closer = new WebSocketCloser(lc, source, sink, handleMessage);

  function handleMessage(event: MessageEvent) {
    const data = event.data.toString();
    if (closer.closed()) {
      lc.debug?.('Ignoring message received after closed', data);
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

class WebSocketCloser<T> {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #stream: Source<T>;
  readonly #messageHandler: ((e: MessageEvent) => void | undefined) | null;
  readonly #connected = resolver();

  get connected(): Promise<void> {
    return this.#connected.promise;
  }

  constructor(
    lc: LogContext,
    ws: WebSocket,
    stream: Source<T>,
    messageHandler?: (e: MessageEvent) => void | undefined,
  ) {
    this.#lc = lc;
    this.#ws = ws;
    this.#stream = stream;
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

  #handleOpen = () => {
    this.#lc.info?.('connected');
    this.#connected.resolve();
  };

  #handleClose = (e: CloseEvent) => {
    const {code, reason, wasClean} = e;
    this.#lc.info?.('connection closed', {code, reason, wasClean});
    this.close();
    this.#connected.reject(`connection closed with code ${code}`);
  };

  #handleError = ({message, error}: ErrorEvent) => {
    this.#lc.error?.('connection error', message, error);
    this.#connected.reject(error);
  };

  close(err?: unknown) {
    if (err) {
      this.#lc.error?.(`closing stream with error`, err);
    }
    this.#stream.cancel();
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
