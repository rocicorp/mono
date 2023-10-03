import {jest} from '@jest/globals';

type Method = 'GET' | 'PUT' | 'PATCH' | 'POST' | 'DELETE';

type Handler = {
  method: Method;
  urlSubstring: string;
  response: Response;
  once?: boolean;
};

function defaultSuccessResponse<T>(result: T): Response {
  return {
    ok: true,
    status: 200,
    json: () => Promise.resolve(result),
  } as unknown as Response;
}

function defaultErrorResponse(code: number, message?: string): Response {
  return {
    ok: false,
    status: code,
    statusText: message ?? '',
  } as unknown as Response;
}

export class FetchMocker {
  #success: (result: unknown) => Response;
  #error: (code: number, message?: string) => Response;
  constructor(
    success: (result: unknown) => Response = defaultSuccessResponse,
    error: (code: number, message?: string) => Response = defaultErrorResponse,
  ) {
    this.#success = success;
    this.#error = error;
  }
  readonly spy = jest
    .spyOn(globalThis, 'fetch')
    .mockImplementation((input, init) => this.#handle(input, init));

  readonly #handlers: Handler[] = [];
  #defaultResponse: Response = {
    ok: false,
    status: 404,
  } as unknown as Response;

  #handle(input: RequestInfo | URL, init?: RequestInit): Promise<Response> {
    for (let i = 0; i < this.#handlers.length; i++) {
      const handler = this.#handlers[i];
      if (
        handler.method === (init?.method ?? 'GET') &&
        input.toString().includes(handler.urlSubstring)
      ) {
        if (handler.once) {
          this.#handlers.splice(i, 1);
        }
        return Promise.resolve(handler.response);
      }
    }
    return Promise.resolve(this.#defaultResponse);
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

  result<T>(method: Method, urlSubstring: string, json: T): this {
    this.#handlers.push({
      method,
      urlSubstring,
      response: this.#success(json),
    });
    return this;
  }

  error(
    method: Method,
    urlSubstring: string,
    code: number,
    message?: string,
  ): this {
    this.#handlers.push({
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
    this.#handlers[this.#handlers.length - 1].once = true;
    return this;
  }

  requests(): [method: string, url: string][] {
    return this.spy.mock.calls.map(([input, init]) => [
      init?.method ?? 'GET',
      input.toString(),
    ]);
  }

  bodys(): (BodyInit | null | undefined)[] {
    return this.spy.mock.calls.map(([_, init]) => init?.body);
  }

  headers(): (HeadersInit | undefined)[] {
    return this.spy.mock.calls.map(([_, init]) => init?.headers);
  }

  jsonPayloads(): unknown[] {
    return this.spy.mock.calls.map(([_, init]) =>
      JSON.parse(String(init?.body)),
    );
  }
}
