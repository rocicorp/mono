import type * as vitest from 'vitest';

type HandlerFn = (
  url: string,
  init: RequestInit | undefined,
  request: Request,
) => Response | Promise<Response>;

type Handler = {
  urlSubstring: string | undefined; // undefined means match all URLs
  response: Response | HandlerFn;
  once?: boolean;
};

type StatusResponse = {body?: unknown; status: number};
type ThrowResponse = {throws: Error};
type PostHandler<T> = (
  url: string,
  init: RequestInit | undefined,
  request: Request,
) =>
  | T
  | StatusResponse
  | ThrowResponse
  | Promise<T | StatusResponse | ThrowResponse>;

function isStatusResponse(value: unknown): value is StatusResponse {
  return (
    typeof value === 'object' &&
    value !== null &&
    'status' in value &&
    typeof (value as StatusResponse).status === 'number'
  );
}

function isThrowResponse(value: unknown): value is ThrowResponse {
  return typeof value === 'object' && value !== null && 'throws' in value;
}

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

function errorResponse(code: number, message: string = ''): Response {
  return {
    ok: false,
    status: code,
    statusText: message,
    text: () => Promise.resolve(message),
  } as unknown as Response;
}

interface SpyOn {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  spyOn(obj: object, methodName: string): any;
}

type FetchSpy = vitest.MockInstance<
  (
    ...args: [input: string | Request | URL, init?: RequestInit | undefined]
  ) => Promise<Response>
>;

export class FetchMocker {
  readonly spy: FetchSpy;

  // Store request bodies separately since Request.body is a ReadableStream
  readonly #requestBodies: (string | null)[] = [];

  constructor(spyOn: SpyOn) {
    this.spy = spyOn
      .spyOn(globalThis, 'fetch')
      .mockImplementation(
        (input: RequestInfo | URL, init: RequestInit | undefined) =>
          this.#handle(input, init),
      );
  }

  readonly handlers: Handler[] = [];
  #defaultResponse: Response = errorResponse(404, 'not found');
  #catchHandler: (() => Response | Promise<Response>) | undefined;
  // Track which calls were matched (by index into spy.mock.calls)
  readonly #matchedIndices = new Set<number>();

  async #handle(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const url = getUrl(input);

    // Store the body for later retrieval via calls()/lastBody()
    let body: string | null = null;
    if (init?.body) {
      body = typeof init.body === 'string' ? init.body : String(init.body);
    } else if (input instanceof Request) {
      // Clone the request to read its body without consuming the original
      const cloned = input.clone();
      body = await cloned.text();
    }
    this.#requestBodies.push(body);
    const callIndex = this.#requestBodies.length - 1;

    // Create a Request object for handlers that need it
    const request =
      input instanceof Request
        ? input.clone()
        : new Request(url, {
            ...init,
            body: body ?? null,
          });

    // Search backwards so newer handlers take precedence (like overwriteRoutes)
    for (let i = this.handlers.length - 1; i >= 0; i--) {
      const handler = this.handlers[i];
      // undefined = match all; empty string = match nothing; other = substring match
      const urlMatches =
        handler.urlSubstring === undefined ||
        (handler.urlSubstring !== '' && url.includes(handler.urlSubstring));
      if (urlMatches) {
        if (handler.once) {
          this.handlers.splice(i, 1);
        }
        this.#matchedIndices.add(callIndex);
        const response = handler.response;
        if (typeof response === 'function') {
          return Promise.resolve(response(url, init, request));
        }
        return Promise.resolve(response);
      }
    }
    // Unmatched - call catch handler if set
    if (this.#catchHandler) {
      return Promise.resolve(this.#catchHandler());
    }
    return Promise.resolve(this.#defaultResponse);
  }

  /**
   * Sets a handler for unmatched requests.
   */
  catch(handler: () => Response | Promise<Response>): this {
    this.#catchHandler = handler;
    return this;
  }

  default<T extends Record<string, unknown>>(result: T): this;
  default(errorCode: number, message?: string): this;
  default(
    errorCodeOrResult: number | Record<string, unknown>,
    message?: string,
  ): this {
    this.#defaultResponse =
      typeof errorCodeOrResult === 'number'
        ? errorResponse(errorCodeOrResult, message)
        : successResponse(errorCodeOrResult);
    return this;
  }

  /**
   * Register a POST handler. Accepts:
   * - A JSON response value
   * - A handler function that returns JSON, {status, body}, or {throws: Error}
   * - A {status, body} object for error responses
   */
  post<T>(
    urlSubstring: string | undefined,
    json: T | PostHandler<T> | StatusResponse,
  ): this {
    if (typeof json === 'function') {
      this.handlers.push({
        urlSubstring,
        response: async (url, init, request) => {
          const result = await (json as PostHandler<T>)(url, init, request);
          return this.#handleResult(result);
        },
      });
    } else {
      this.handlers.push({
        urlSubstring,
        response: this.#handleResult(json),
      });
    }
    return this;
  }

  #handleResult<T>(result: T | StatusResponse | ThrowResponse): Response {
    if (isThrowResponse(result)) {
      throw result.throws;
    }
    if (isStatusResponse(result)) {
      return result.status === 200
        ? successResponse(result.body)
        : errorResponse(result.status, String(result.body ?? ''));
    }
    return successResponse(result);
  }

  /**
   * Like post() but handler is removed after first match.
   */
  postOnce<T>(
    urlSubstring: string | undefined,
    json: T | PostHandler<T> | StatusResponse,
  ): this {
    this.post(urlSubstring, json);
    this.handlers[this.handlers.length - 1].once = true;
    return this;
  }

  /**
   * Match any POST request.
   */
  postAny<T>(json: T): this {
    return this.post(undefined, json);
  }

  /**
   * Reset all handlers and clear recorded calls.
   */
  reset(): void {
    this.handlers.length = 0;
    this.#requestBodies.length = 0;
    this.#matchedIndices.clear();
    this.#catchHandler = undefined;
    this.spy.mockClear();
  }

  /**
   * Returns calls filtered by URL substring.
   * Each call has a `body` with the parsed JSON body.
   * Pass 'unmatched' to get calls that didn't match any handler.
   */
  calls(urlSubstring: string): {body: unknown}[] {
    const result: {body: unknown}[] = [];
    this.spy.mock.calls.forEach(([input], index) => {
      const matchesUrl =
        urlSubstring === 'unmatched'
          ? !this.#matchedIndices.has(index)
          : getUrl(input).includes(urlSubstring);

      if (matchesUrl) {
        const body = this.#requestBodies[index];
        result.push({body: body ? JSON.parse(body) : null});
      }
    });
    return result;
  }

  /**
   * Returns the JSON body of the last request.
   */
  lastBody(): unknown {
    const body = this.#requestBodies[this.#requestBodies.length - 1];
    return body ? JSON.parse(body) : undefined;
  }
}
