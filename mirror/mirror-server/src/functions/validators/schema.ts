import {HttpsError, type CallableRequest} from 'firebase-functions/v2/https';
import type * as v from 'shared/src/valita.js';
import {parse} from 'shared/src/valita.js';
import {ValidatorChainer} from './types.js';
import {logger} from 'firebase-functions';

export function validateSchema<Request, Response>(
  requestSchema: v.Type<Request>,
  responseSchema: v.Type<Response>,
): ValidatorChainer<Request, CallableRequest<Request>, Response> {
  return new ValidatorChainer(
    (request, context) => {
      try {
        parse(request, requestSchema);
      } catch (e) {
        logger.warn(String(e));
        throw new HttpsError('invalid-argument', String(e));
      }
      return context;
    },
    res => parse(res, responseSchema),
  );
}
