import type {Response} from 'express';
import {logger} from 'firebase-functions';
import {HttpsError, type Request} from 'firebase-functions/v2/https';
import type {RequestContextValidator} from './types.js';
import type {MaybePromise} from 'shared/src/types.js';

type OnRequest = (request: Request, response: Response) => MaybePromise<void>;

export type OnRequestHandler<Request, Context> = (
  req: Request,
  ctx: Context,
) => MaybePromise<void>;

export type HttpsRequestContext = {
  request: Request;
};

export type HttpsResponseContext = {
  response: Response;
};

export type OnRequestContext = HttpsRequestContext & HttpsResponseContext;

export class OnRequestBuilder<Request, Context> {
  readonly #requestValidator: RequestContextValidator<
    Request,
    OnRequestContext,
    Context
  >;

  constructor(
    requestValidator: RequestContextValidator<
      Request,
      OnRequestContext,
      Context
    >,
  ) {
    this.#requestValidator = requestValidator;
  }

  /**
   * Used to chain RequestContextValidators that convert / augment
   * the final context passed to the handler.
   */
  validate<NewContext>(
    nextValidator: RequestContextValidator<Request, Context, NewContext>,
  ): OnRequestBuilder<Request, NewContext> {
    return new OnRequestBuilder(async (request, ctx) => {
      const context = await this.#requestValidator(request, ctx);
      return nextValidator(request, context);
    });
  }

  handle(handler: OnRequestHandler<Request, Context>): OnRequest {
    return async (request, response) => {
      const ctx: OnRequestContext = {request, response};
      try {
        // If the body is a Buffer, it's likely a JSON payload.
        const {body} = request;
        const payload =
          body instanceof Buffer ? JSON.parse(body.toString('utf-8')) : body;
        const context = await this.#requestValidator(payload, ctx);
        await handler(payload, context);
      } catch (e) {
        const err =
          e instanceof HttpsError
            ? e
            : new HttpsError('internal', String(e), e);
        const {status} = err.httpErrorCode;
        if (status >= 500) {
          logger.error(e);
        } else {
          logger.warn(e);
        }
        response.status(err.httpErrorCode.status).send(err.message);
      }
    };
  }
}

/**
 * A variant of the onRequestBuilder used by the API gateway to
 * reuse the auth validation logic used by the other functions.
 */
class ContextValidator<Request, InputContext, OutputContext> {
  readonly #request: Request;
  readonly #input: InputContext;
  readonly #requestValidator: RequestContextValidator<
    Request,
    InputContext,
    OutputContext
  >;

  constructor(
    request: Request,
    input: InputContext,
    requestValidator: RequestContextValidator<
      Request,
      InputContext,
      OutputContext
    >,
  ) {
    this.#request = request;
    this.#input = input;
    this.#requestValidator = requestValidator;
  }

  validate<NewContext>(
    nextValidator: RequestContextValidator<Request, OutputContext, NewContext>,
  ): ContextValidator<Request, InputContext, NewContext> {
    return new ContextValidator(
      this.#request,
      this.#input,
      async (request, ctx) => {
        const context = await this.#requestValidator(request, ctx);
        return nextValidator(request, context);
      },
    );
  }

  process(): MaybePromise<OutputContext> {
    return this.#requestValidator(this.#request, this.#input);
  }
}

export function contextValidator<Request, Context>(req: Request, ctx: Context) {
  return new ContextValidator(req, ctx, (_, ctx) => ctx);
}
