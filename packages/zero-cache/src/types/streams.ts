import type {LogContext} from '@rocicorp/logger';
import {Queue} from 'shared/src/queue.js';
import * as v from 'shared/src/valita.js';
import type {CloseEvent, ErrorEvent, MessageEvent, WebSocket} from 'ws';
import {BigIntJSON, type JSONValue} from './bigint-json.js';
import {Subscription} from './subscription.js';

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

export function streamIn<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Source<T> {
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

  return sink;
}

class WebSocketCloser<T> {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #stream: Source<T>;
  readonly #closeHandler: (e: CloseEvent) => void;
  readonly #errorHandler: (e: ErrorEvent) => void;
  readonly #messageHandler: ((e: MessageEvent) => void | undefined) | null;
  #closed = false;

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

    this.#closeHandler = e => this.#handleClose(e);
    this.#errorHandler = e => this.#handleError(e);

    ws.addEventListener('close', this.#closeHandler);
    ws.addEventListener('error', this.#errorHandler);
    if (this.#messageHandler) {
      ws.addEventListener('message', this.#messageHandler);
    }
  }

  #handleClose(e: CloseEvent) {
    const {code, reason, wasClean} = e;
    this.#lc.info?.('WebSocket close event', {code, reason, wasClean});
    this.close();
  }

  #handleError(e: ErrorEvent) {
    this.#lc.error?.('WebSocket error event', e.message, e.error);
    // Should we close here?
  }

  close(err?: unknown) {
    if (this.#closed) {
      return;
    }
    if (err) {
      this.#lc.error?.(`closing stream with error`, err);
    }
    this.#closed = true;
    this.#ws.removeEventListener('close', this.#closeHandler);
    this.#ws.removeEventListener('error', this.#errorHandler);
    if (this.#messageHandler) {
      this.#ws.removeEventListener('message', this.#messageHandler);
    }
    this.#stream.cancel();
    if (this.#ws.readyState !== this.#ws.CLOSED) {
      this.#ws.close();
    }
  }

  closed() {
    return this.#closed;
  }
}
