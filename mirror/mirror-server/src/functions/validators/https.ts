import type {Request} from 'firebase-functions/v2/https';
import type {Response} from 'express';
import type {RequestContextValidator, MaybePromise} from './types.js';

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
  private readonly _requestValidator: RequestContextValidator<
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
    this._requestValidator = requestValidator;
  }

  /**
   * Used to chain RequestContextValidators that convert / augment
   * the final context passed to the handler.
   */
  validate<NewContext>(
    nextValidator: RequestContextValidator<Request, Context, NewContext>,
  ): OnRequestBuilder<Request, NewContext> {
    return new OnRequestBuilder(async (request, ctx) => {
      const context = await this._requestValidator(request, ctx);
      return nextValidator(request, context);
    });
  }

  handle(handler: OnRequestHandler<Request, Context>): OnRequest {
    return async (request, response) => {
      const ctx: OnRequestContext = {request, response};
      const payload: Request = request.body as Request;

      const context = await this._requestValidator(payload, ctx);
      await handler(payload, context);
    };
  }
}
