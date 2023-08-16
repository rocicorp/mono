import {HttpsError, type CallableRequest} from 'firebase-functions/v2/https';
import type * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
import {RequestContextValidator, ValidatorChainer} from './types.js';
import {logger} from 'firebase-functions';
import {OnRequestBuilder} from './https.js';

export function validateSchema<Request, Response>(
  reqSchema: v.Type<Request>,
  resSchema: v.Type<Response>,
): ValidatorChainer<Request, CallableRequest<Request>, Response> {
  return new ValidatorChainer(requestSchema(reqSchema), res =>
    parse(res, resSchema),
  );
}

export function validateRequest<Request>(reqSchema: v.Type<Request>) {
  return new OnRequestBuilder(requestSchema(reqSchema));
}

function requestSchema<Request, Context>(
  schema: v.Type<Request>,
): RequestContextValidator<Request, Context, Context> {
  return (request, context) => {
    try {
      parse(request, schema);
    } catch (e) {
      logger.warn(String(e));
      throw new HttpsError('invalid-argument', String(e));
    }
    return context;
  };
}
