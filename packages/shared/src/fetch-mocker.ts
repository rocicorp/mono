import type * as vitest from 'vitest';

type Method = 'GET' | 'PUT' | 'PATCH' | 'POST' | 'DELETE';

type HandlerFn = (
  url: string,
  init: RequestInit | undefined,
  request: Request,
) => Response | Promise<Response>;

type Handler = {
  method: Method;
  urlSubstring: string;
  response: Response | HandlerFn;
  once?: boolean;
};

function defaultSuccessResponse<T>(result: T): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(result),
  } as unknown as Response;
}

function defaultErrorResponse(code: number, message: string = ''): Response {
  return {
    ok: false,
    status: code,
    statusText: message,
    text: () => Promise.resolve(message),
  } as unknown as Response;
}

export interface SpyOn {
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  spyOn(obj: object, methodName: string): any;
}

type FetchSpy = vitest.MockInstance<
  (
    ...args: [input: string | Request | URL, init?: RequestInit | undefined]
  ) => Promise<Response>
>;

export class FetchMocker {
  #success: (result: unknown) => Response;
  #error: (code: number, message?: string) => Response;

  readonly spy: FetchSpy;

  // Store request bodies separately since Request.body is a ReadableStream
  readonly #requestBodies: (string | null)[] = [];

  constructor(
    spyOn: SpyOn,
    success: (result: unknown) => Response = defaultSuccessResponse,
    error: (code: number, message?: string) => Response = defaultErrorResponse,
  ) {
    this.spy = spyOn
      .spyOn(globalThis, 'fetch')
      .mockImplementation(
        (input: RequestInfo | URL, init: RequestInit | undefined) =>
          this.#handle(input, init),
      );
    this.#success = success;
    this.#error = error;
  }

  readonly handlers: Handler[] = [];
  #defaultResponse: Response = {
    ok: false,
    status: 404,
    text: () => Promise.resolve('not found'),
  } as unknown as Response;
  #catchHandler: (() => Response | Promise<Response>) | undefined;
  // Track which calls were matched (by index into spy.mock.calls)
  readonly #matchedIndices = new Set<number>();

  async #handle(
    input: RequestInfo | URL,
    init?: RequestInit,
  ): Promise<Response> {
    const url = input instanceof Request ? input.url : input.toString();
    const method =
      init?.method ?? (input instanceof Request ? input.method : 'GET');

    // Store the body for later retrieval via calls()/lastCall()
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
      // Empty urlSubstring matches nothing (unlike String.includes which returns true)
      if (
        handler.method === method &&
        handler.urlSubstring !== '' &&
        url.includes(handler.urlSubstring)
      ) {
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
        ? this.#error(errorCodeOrResult, message)
        : this.#success(errorCodeOrResult);
    return this;
  }

  result<T>(method: Method, urlSubstring: string, json: T | (() => T)): this {
    this.handlers.push({
      method,
      urlSubstring,
      response:
        typeof json === 'function'
          ? () => this.#success((json as () => T)())
          : this.#success(json),
    });
    return this;
  }

  /**
   * Shorthand for result('POST', ...). Also supports function responses for compatibility.
   * Handler functions receive (url, init, request) arguments.
   */
  post<T>(
    urlSubstring: string,
    json:
      | T
      | ((
          url: string,
          init: RequestInit | undefined,
          request: Request,
        ) => T | Promise<T>)
      | {body: string; status: number},
  ): this {
    this.handlers.push({
      method: 'POST',
      urlSubstring,
      response:
        typeof json === 'function'
          ? async (
              url: string,
              init: RequestInit | undefined,
              request: Request,
            ) => {
              const result = await (
                json as (
                  url: string,
                  init: RequestInit | undefined,
                  request: Request,
                ) => T | Promise<T>
              )(url, init, request);
              // Check if the function returned an error-like object with explicit status
              if (
                typeof result === 'object' &&
                result !== null &&
                'status' in result &&
                typeof (result as {status: unknown}).status === 'number'
              ) {
                const r = result as {status: number; body?: unknown};
                // Non-200 status codes should use the error response format
                if (r.status !== 200) {
                  return this.#error(r.status, String(r.body ?? ''));
                }
                // Status 200 with body - return success with the body
                return this.#success(r.body);
              }
              // Check if the function requested to throw
              if (
                typeof result === 'object' &&
                result !== null &&
                'throws' in result
              ) {
                throw (result as {throws: Error}).throws;
              }
              return this.#success(result);
            }
          : typeof json === 'object' &&
              json !== null &&
              'body' in json &&
              'status' in json
            ? (json as {body: unknown; status: number}).status === 200
              ? this.#success((json as {body: unknown}).body)
              : this.#error(
                  (json as {status: number}).status,
                  String((json as {body: unknown}).body),
                )
            : this.#success(json),
    });
    return this;
  }

  /**
   * Shorthand for post() with once: true.
   */
  postOnce<T>(
    urlSubstring: string,
    json:
      | T
      | ((
          url: string,
          init: RequestInit | undefined,
          request: Request,
        ) => T | Promise<T>)
      | {body: string; status: number},
  ): this {
    this.post(urlSubstring, json);
    this.handlers[this.handlers.length - 1].once = true;
    return this;
  }

  /**
   * Match any POST request (uses empty string as substring, which matches all URLs).
   */
  postAny<T>(json: T): this {
    return this.post('', json);
  }

  /**
   * Reset all handlers (clear the handlers array) and clear recorded calls.
   */
  reset(): void {
    this.handlers.length = 0;
    this.#requestBodies.length = 0;
    this.#matchedIndices.clear();
    this.#catchHandler = undefined;
    this.spy.mockClear();
  }

  error(
    method: Method,
    urlSubstring: string,
    code: number,
    message?: string,
  ): this {
    this.handlers.push({
      method,
      urlSubstring,
      response: this.#error(code, message),
    });
    return this;
  }

  /**
   * Configures the last specified handler (via result() or error()) to only be applied once.
   */
  once(): this {
    this.handlers[this.handlers.length - 1].once = true;
    return this;
  }

  requests(): [method: string, url: string][] {
    return this.spy.mock.calls.map(([input, init]) => {
      const url = input instanceof Request ? input.url : input.toString();
      const method =
        init?.method ?? (input instanceof Request ? input.method : 'GET');
      return [method, url];
    });
  }

  /**
   * Returns calls filtered by URL substring.
   * Each call has a `body` with the parsed JSON body.
   * Pass 'unmatched' to get calls that didn't match any handler.
   */
  calls(urlSubstring: string): {body: unknown}[] {
    const result: {body: unknown}[] = [];
    this.spy.mock.calls.forEach(([input], index) => {
      const url = input instanceof Request ? input.url : input.toString();
      const matchesUrl =
        urlSubstring === 'unmatched'
          ? !this.#matchedIndices.has(index)
          : url.includes(urlSubstring);

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

  /**
   * Returns all request bodies as strings.
   */
  bodys(): (string | null)[] {
    return [...this.#requestBodies];
  }

  headers(): (HeadersInit | undefined)[] {
    return this.spy.mock.calls.map(([_, init]) => init?.headers);
  }

  /**
   * Returns all request bodies parsed as JSON.
   */
  jsonBodies(): unknown[] {
    return this.#requestBodies.map(body => (body ? JSON.parse(body) : null));
  }
}
