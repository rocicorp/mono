import {HttpsError, type CallableRequest} from 'firebase-functions/v2/https';
import type * as v from 'shared/src/valita.js';
import {parse, is} from 'shared/src/valita.js';
import {ValidatorChainer} from './types.js';

export function validateSchema<Request, Response>(
  requestSchema: v.Type<Request>,
  responseSchema: v.Type<Response>,
): ValidatorChainer<Request, CallableRequest<Request>, Response> {
  return new ValidatorChainer(
    (request: Request, context: CallableRequest<Request>) => {
      if (!is(request, requestSchema, 'passthrough')) {
        throw new HttpsError(
          'invalid-argument',
          'Invalid request payload format',
        );
      }
      return Promise.resolve(context);
    },
    // eslint-disable-next-line require-await
    async (res: Response) => parse(res, responseSchema),
  );
}
