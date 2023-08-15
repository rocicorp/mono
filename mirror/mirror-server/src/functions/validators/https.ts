import type {Request} from 'firebase-functions/v2/https';
import type {AuthData} from 'firebase-functions/v2/tasks';
import type {Response} from 'express';
import type {RequestContextValidator, MaybePromise} from './types.js';

type OnRequest = (request: Request, response: Response) => MaybePromise<void>;

export type OnRequestHandler<Request, Context> = (
  req: Request,
  ctx: Context,
) => MaybePromise<void>;

export type OnRequestContext = {
  request: Request;
  response: Response;
};

export class HttpsValidatorChainer<Request, Context> {
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
  ): HttpsValidatorChainer<Request, NewContext> {
    return new HttpsValidatorChainer(async (request, ctx) => {
      const context = await this._requestValidator(request, ctx);
      return nextValidator(request, context);
    });
  }

  handle(handler: OnRequestHandler<Request, Context>): OnRequest {
    return async (req, res) => {
      const ctx: OnRequestContext = {res} as unknown as OnRequestContext;
      const payload: Request = req.body as unknown as Request;

      const context = await this._requestValidator(payload, ctx);
      await handler(payload, context);
    };
  }
}
