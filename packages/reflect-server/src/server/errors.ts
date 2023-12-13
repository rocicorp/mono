export abstract class ErrorWithResponse extends Error {
  constructor(message?: string, options?: ErrorOptions) {
    super(message, options);
  }

  abstract response(): Response;
}

export class HttpError extends ErrorWithResponse {
  readonly #status: number;

  constructor(status: number, message: string, options?: ErrorOptions) {
    super(message, options);
    this.#status = status;
  }

  response(): Response {
    return new Response(this.message, {status: this.#status});
  }
}

export function makeErrorResponse(e: unknown): Response {
  return e instanceof ErrorWithResponse
    ? e.response()
    : new Response(e instanceof Error ? e.message : String(e), {status: 500});
}
