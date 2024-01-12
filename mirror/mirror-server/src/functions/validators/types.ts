import {logger} from 'firebase-functions';
import {HttpsError, type CallableRequest} from 'firebase-functions/v2/https';
import {WARMUP_RESPONSE, type WarmupRequest} from 'mirror-protocol/src/call.js';
import type {App} from 'mirror-schema/src/app.js';

export type UserAuthorization = {
  userID: string;
};

export type UserOrKeyAuthorization = {
  userID: string;
  isKeyAuth: boolean;
};

export type AppAuthorization = {
  app: App;
};

export type MaybePromise<T> = T | Promise<T>;

export type RequestContextValidator<Request, InputContext, OutputContext> = (
  req: Request,
  ctx: InputContext,
) => MaybePromise<OutputContext>;

export type ResponseValidator<Response> = (
  res: Response,
) => MaybePromise<Response>;

export type Handler<Request, Context, Response> = (
  req: Request,
  ctx: Context,
) => MaybePromise<Response>;

export type Callable<Request, Response> = (
  request: CallableRequest<Request>,
) => Promise<Response>;

export class ValidatorChainer<Request, Context, Response> {
  readonly #requestValidator: RequestContextValidator<
    Request,
    CallableRequest<Request>,
    Context
  >;
  readonly #responseValidator: ResponseValidator<Response>;

  constructor(
    requestValidator: RequestContextValidator<
      Request,
      CallableRequest<Request>,
      Context
    >,
    responseValidator: ResponseValidator<Response>,
  ) {
    this.#requestValidator = requestValidator;
    this.#responseValidator = responseValidator;
  }

  /**
   * Used to chain RequestContextValidators that convert / augment
   * the final context passed to the handler.
   */
  validate<NewContext>(
    nextValidator: RequestContextValidator<Request, Context, NewContext>,
  ): ValidatorChainer<Request, NewContext, Response> {
    return new ValidatorChainer(async (request, ctx) => {
      const context = await this.#requestValidator(request, ctx);
      return nextValidator(request, context);
    }, this.#responseValidator);
  }

  handle(
    handler: Handler<Request, Context, Response>,
  ): Callable<Request, Response> {
    return async originalContext => {
      const request = originalContext.data;
      if ((request as WarmupRequest)._warm_) {
        logger.debug('Serviced warmup request');
        return WARMUP_RESPONSE as Response;
      }
      try {
        const context = await this.#requestValidator(request, originalContext);
        const response = await handler(request, context);
        return this.#responseValidator(response);
      } catch (e) {
        const err =
          e instanceof HttpsError
            ? e
            : new HttpsError('internal', String(e), e);
        const {status} = err.httpErrorCode;
        if (status >= 500) {
          logger.error(e, request);
        } else {
          logger.warn(e, request);
        }
        throw e;
      }
    };
  }
}
