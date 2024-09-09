import type {WebSocket} from '@fastify/websocket';
import type {LogContext} from '@rocicorp/logger';
import {Queue} from 'shared/src/queue.js';
import * as v from 'shared/src/valita.js';
import type {CloseEvent, ErrorEvent, MessageEvent} from 'ws';
import {BigIntJSON, type JSONObject} from './bigint-json.js';
import {Subscription} from './subscription.js';

export type Source<T> = AsyncIterable<T> & {
  /**
   * Immediately terminates all current iterations (i.e. {@link AsyncIterator.next next()})
   * will return `{value: undefined, done: true}`), and prevents any subsequent iterations
   * from yielding any values.
   */
  cancel: () => void;
};

export type Sink<T> = {
  push(message: T): void;
};

const ackSchema = v.object({consumedID: v.number()});

type Ack = v.Infer<typeof ackSchema>;

// eslint-disable-next-line @typescript-eslint/naming-convention
type Streamed<T> = T & {_streamID: number};

export async function streamOut<T extends JSONObject>(
  lc: LogContext,
  source: Source<T>,
  sink: WebSocket,
): Promise<void> {
  const closer = new WebSocketCloser(lc, sink, source);

  const acks = new Queue<Ack>();
  sink.addEventListener('message', (e: {data: string}) => {
    try {
      if (typeof e.data !== 'string') {
        throw new Error('Expected string message');
      }
      void acks.enqueue(v.parse(JSON.parse(e.data), ackSchema));
    } catch (e) {
      lc.error?.(`error parsing ack`, e);
      closer.close(e);
    }
  });

  lc.info?.('started outbound stream');
  try {
    let nextID = 0;
    for await (const payload of source) {
      const streamID = ++nextID;
      // eslint-disable-next-line @typescript-eslint/naming-convention
      const msg = BigIntJSON.stringify({_streamID: streamID, ...payload});
      lc.debug?.(`sending`, msg);
      sink.send(msg);

      const ack = await acks.dequeue();
      if (ack.consumedID !== streamID) {
        throw new Error(`Unexpected ack for ${streamID}: ${ack.consumedID}`);
      }
      lc.debug?.(`received ack`, ack);
    }
    closer.close();
  } catch (e) {
    closer.close(e);
  }
}

export function streamIn<T extends JSONObject>(
  lc: LogContext,
  source: WebSocket,
  schema: v.Type<T>,
): Source<T> {
  const sink: Subscription<T, Streamed<T>> = new Subscription<T, Streamed<T>>(
    {
      consumed: msg => {
        const ack: Ack = {consumedID: msg._streamID};
        source.send(JSON.stringify(ack));
      },
      cleanup: () => closer.close(),
    },
    s => {
      // Exclude the internal wire-protocol `_streamID` field from the message exposed in the Iterable.
      const {_streamID, ...m} = s;
      m satisfies Omit<Streamed<T>, '_streamID'>; // Documents that `m` is of type `T`,
      return m as unknown as T; // which TypeScript can't yet figure out.
    },
  );

  const closer = new WebSocketCloser(lc, source, sink, handleMessage);

  lc.info?.('started inbound stream');
  function handleMessage(event: MessageEvent) {
    const data = event.data.toString();
    if (closer.closed()) {
      lc.debug?.('Ignoring message received after closed', data);
      return;
    }
    try {
      const value = BigIntJSON.parse(data);
      // TODO: Make this work with schema.extend({_streamID: v.number()})
      const msg = v.parse(value, schema, 'passthrough');
      if (typeof msg['_streamID'] !== 'number') {
        throw new Error(`No _streamID found in ${BigIntJSON.stringify(msg)}`);
      }
      lc.debug?.(`received`, data);
      sink.push(msg as Streamed<T>);
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
