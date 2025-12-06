import type * as vitest from 'vitest';

type StatusResponse = {body?: unknown; status: number};
type ThrowResponse = {throws: Error};
type HandlerResult<T> = T | StatusResponse | ThrowResponse;
type HandlerFn<T> = (
  url: string,
  body: unknown,
) => HandlerResult<T> | Promise<HandlerResult<T>>;

type Handler = {
  urlSubstring: string | undefined; // undefined = match all
  handler: HandlerFn<unknown>;
  once: boolean;
};

function getUrl(input: string | Request | URL): string {
  return input instanceof Request ? input.url : input.toString();
}

function successResponse<T>(result: T): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(result),
  } as unknown as Response;
}

function errorResponse(code: number, message = ''): Response {
  return {
    ok: false,
    status: code,
    statusText: message,
    text: () => Promise.resolve(message),
  } as unknown as Response;
}

function toResponse(result: HandlerResult<unknown>): Response {
  if (typeof result === 'object' && result !== null) {
    if ('throws' in result) throw (result as ThrowResponse).throws;
    if (
      'status' in result &&
      typeof (result as StatusResponse).status === 'number'
    ) {
      const r = result as StatusResponse;
      return r.status === 200
        ? successResponse(r.body)
        : errorResponse(r.status, String(r.body ?? ''));
    }
  }
  return successResponse(result);
}

type FetchSpy = vitest.MockInstance<
  (
    ...args: [input: string | Request | URL, init?: RequestInit | undefined]
  ) => Promise<Response>
>;

export class FetchMocker {
  readonly spy: FetchSpy;
  readonly #handlers: Handler[] = [];
  readonly #bodies: unknown[] = [];

  constructor(vi: {spyOn: typeof vitest.vi.spyOn}) {
    this.spy = vi
      .spyOn(globalThis, 'fetch')
      .mockImplementation((input, init) => this.#handle(input, init));
  }

  async #handle(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const url = getUrl(input);

    // Parse and store body
    let body: unknown = null;
    if (init?.body) {
      body = JSON.parse(
        typeof init.body === 'string' ? init.body : String(init.body),
      );
    } else if (input instanceof Request) {
      body = await input
        .clone()
        .json()
        .catch(() => null);
    }
    this.#bodies.push(body);

    // Find matching handler (search backwards, newer handlers take precedence)
    for (let i = this.#handlers.length - 1; i >= 0; i--) {
      const h = this.#handlers[i];
      const matches =
        h.urlSubstring === undefined ||
        (h.urlSubstring.length > 0 && url.includes(h.urlSubstring));
      if (matches) {
        if (h.once) this.#handlers.splice(i, 1);
        const result = await h.handler(url, body);
        return toResponse(result);
      }
    }
    return errorResponse(404, 'not found');
  }

  post<T>(
    urlSubstring: string | undefined,
    response: T | HandlerFn<T> | StatusResponse,
  ): this {
    const handler: HandlerFn<unknown> =
      typeof response === 'function'
        ? (response as HandlerFn<T>)
        : () => response;
    this.#handlers.push({urlSubstring, handler, once: false});
    return this;
  }

  postOnce<T>(
    urlSubstring: string | undefined,
    response: T | HandlerFn<T> | StatusResponse,
  ): this {
    this.post(urlSubstring, response);
    this.#handlers[this.#handlers.length - 1].once = true;
    return this;
  }

  reset(): void {
    this.#handlers.length = 0;
    this.#bodies.length = 0;
    this.spy.mockClear();
  }

  /** Get parsed JSON bodies of requests matching the URL substring. */
  bodies(urlSubstring: string): unknown[] {
    return this.spy.mock.calls
      .map(([input], i) => ({url: getUrl(input), body: this.#bodies[i]}))
      .filter(({url}) => url.includes(urlSubstring))
      .map(({body}) => body);
  }

  /** Get the parsed JSON body of the last request. */
  lastBody(): unknown {
    return this.#bodies[this.#bodies.length - 1];
  }
}
