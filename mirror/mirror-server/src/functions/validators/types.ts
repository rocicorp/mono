import type {CallableRequest} from 'firebase-functions/v2/https';
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
  private readonly _requestValidator: RequestContextValidator<
    Request,
    CallableRequest<Request>,
    Context
  >;
  private readonly _responseValidator: ResponseValidator<Response>;

  constructor(
    requestValidator: RequestContextValidator<
      Request,
      CallableRequest<Request>,
      Context
    >,
    responseValidator: ResponseValidator<Response>,
  ) {
    this._requestValidator = requestValidator;
    this._responseValidator = responseValidator;
  }

  /**
   * Used to chain RequestContextValidators that convert / augment
   * the final context passed to the handler.
   */
  validate<NewContext>(
    nextValidator: RequestContextValidator<Request, Context, NewContext>,
  ): ValidatorChainer<Request, NewContext, Response> {
    return new ValidatorChainer(async (request, ctx) => {
      const context = await this._requestValidator(request, ctx);
      return nextValidator(request, context);
    }, this._responseValidator);
  }

  handle(
    handler: Handler<Request, Context, Response>,
  ): Callable<Request, Response> {
    return async originalContext => {
      const request = originalContext.data;
      const context = await this._requestValidator(request, originalContext);
      const response = await handler(request, context);
      return this._responseValidator(response);
    };
  }
}
