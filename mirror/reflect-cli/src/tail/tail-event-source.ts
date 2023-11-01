import type {BaseRequest} from 'mirror-protocol/src/base.js';
import {
  TailMessage,
  tailMessageSchema,
} from 'mirror-protocol/src/tail-message.js';
import {createTailEventSourceURL} from 'mirror-protocol/src/tail.js';
import assert from 'node:assert';
import * as valita from 'shared/src/valita.js';
import {ErrorWithSeverity} from '../error.js';
import {EventSourceEntry, eventSourceStream} from './event-source-stream.js';
import {lineByLineStream} from './line-by-line-stream.js';

export type TailEventSource = AsyncIterable<TailMessage>;

export function createTailEventSource<R extends BaseRequest>(
  functionName: string,
  appID: string,
  apiToken: string,
  request: R,
  url = createTailEventSourceURL(functionName, appID),
): TailEventSource {
  return new TailEventSourceImpl(url, apiToken, request);
}

class TailEventSourceImpl<R extends BaseRequest>
  implements AsyncIterable<TailMessage>
{
  readonly #url: string;
  readonly #apiToken: string;
  readonly #request: R;

  constructor(url: string, apiToken: string, request: R) {
    this.#url = url;
    this.#apiToken = apiToken;
    this.#request = request;
  }

  [Symbol.asyncIterator](): AsyncIterator<TailMessage, void, unknown> {
    return createIter(this.#url, this.#apiToken, this.#request);
  }
}

async function* createIter<R extends BaseRequest>(
  url: string,
  apiToken: string,
  request: R,
): AsyncGenerator<TailMessage, void> {
  const headers = {
    ['Authorization']: `Bearer ${apiToken}`,
    ['Content-Type']: 'text/event-stream',
  };
  const abortController = new AbortController();
  const response = await fetch(url, {
    method: 'POST',
    headers,
    body: JSON.stringify(request),
    signal: abortController.signal,
  });

  if (!response.ok) {
    const message = await response.text();
    throw new ErrorWithSeverity(
      `${message || response.statusText} (HTTP ${response.status})`,
      response.status < 500 ? 'WARNING' : 'ERROR',
    );
  }

  assert(response.body);
  const reader = response.body
    .pipeThrough(new TextDecoderStream())
    .pipeThrough(lineByLineStream())
    .pipeThrough(eventSourceStream());
  try {
    for await (const entry of reader as unknown as AsyncIterable<EventSourceEntry>) {
      if (entry.event === 'message') {
        const v = JSON.parse(entry.data);
        yield valita.parse(v, tailMessageSchema, 'passthrough');
      } else if (entry.event === 'error') {
        throw new Error(entry.data);
      }
    }
  } finally {
    abortController.abort();
  }
}
