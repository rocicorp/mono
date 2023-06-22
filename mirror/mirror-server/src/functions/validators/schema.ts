import type {AsyncCallable, AsyncHandler} from './types.js';
import {type CallableRequest, HttpsError} from 'firebase-functions/v2/https';
import type * as v from 'shared/valita.js';
import {parse} from 'shared/valita.js';

export function withSchema<Request, Response>(
  requestSchema: v.Type<Request>,
  responseSchema: v.Type<Response>,
  handler: AsyncHandler<Request, Response>,
): AsyncCallable<Request, Response> {
  return async (req: CallableRequest<Request>) => {
    let payload: Request;
    try {
      payload = parse(req.data, requestSchema);
    } catch (e) {
      console.log(String(e));
      throw new HttpsError('invalid-argument', String(e));
    }
    const res = await handler(payload, req);
    return parse(res, responseSchema);
  };
}
