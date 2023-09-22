import {logger} from 'firebase-functions';
import {HttpsError, type CallableRequest} from 'firebase-functions/v2/https';
import type {App} from 'mirror-schema/src/app.js';
import type {Role} from 'mirror-schema/src/membership.js';
import type {User} from 'mirror-schema/src/user.js';

export type UserAuthorization = {
  userID: string;
};

export type AppAuthorization = {
  app: App;
  user: User;
  role: Role;
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
      try {
        const context = await this.#requestValidator(request, originalContext);
        const response = await handler(request, context);
        return this.#responseValidator(response);
      } catch (e) {
        if (!(e instanceof HttpsError)) {
          e = new HttpsError('internal', String(e), e);
        }
        const status = (e as HttpsError).httpErrorCode.status;
        if (status >= 500) {
          logger.error(e);
        } else {
          logger.warn(e);
        }
        throw e;
      }
    };
  }
}
