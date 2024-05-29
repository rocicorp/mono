import type {LogContext} from '@rocicorp/logger';
import * as v from 'shared/src/valita.js';
import {BigIntJSON, type JSONValue} from './bigint-json.js';
import type {Subscription} from './subscription.js';

export type CancelableAsyncIterable<T> = AsyncIterable<T> & {
  /**
   * Immediately terminates all current iterations (i.e. {@link AsyncIterator.next next()})
   * will return `{value: undefined, done: true}`), and prevents any subsequent iterations
   * from yielding any values.
   */
  cancel: () => void;
};

export async function streamOut<T extends JSONValue>(
  lc: LogContext,
  source: CancelableAsyncIterable<T>,
  sink: WebSocket,
): Promise<void> {
  const closer = new WebSocketCloser(lc, sink, source);

  lc.info?.('started outbound stream');
  try {
    for await (const payload of source) {
      const msg = BigIntJSON.stringify(payload);
      lc.debug?.(`sending`, msg);
      sink.send(msg);
    }
    closer.close();
  } catch (e) {
    closer.close(e);
  }
}

export function streamIn<T extends JSONValue>(
  lc: LogContext,
  source: WebSocket,
  sink: Subscription<T>,
  schema: v.Type<T>,
): {close: () => void} {
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
      const msg = v.parse(value, schema);
      lc.debug?.(`received`, data);
      sink.push(msg);
    } catch (e) {
      closer.close(e);
    }
  }

  return closer;
}

class WebSocketCloser<T> {
  readonly #lc: LogContext;
  readonly #ws: WebSocket;
  readonly #stream: CancelableAsyncIterable<T>;
  readonly #closeHandler: EventListenerOrEventListenerObject<CloseEvent>;
  readonly #errorHandler: EventListenerOrEventListenerObject<ErrorEvent>;
  readonly #messageHandler: EventListenerOrEventListenerObject<MessageEvent> | null;
  #closed = false;

  constructor(
    lc: LogContext,
    ws: WebSocket,
    stream: CancelableAsyncIterable<T>,
    messageHandler?: EventListenerOrEventListenerObject<MessageEvent>,
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
    if (this.#ws.readyState !== WebSocket.READY_STATE_CLOSED) {
      this.#ws.close();
    }
  }

  closed() {
    return this.#closed;
  }
}
