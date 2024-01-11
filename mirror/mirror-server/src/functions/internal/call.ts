import * as v from 'shared/src/valita.js';
import {cloudFunctionURL} from '../../config/index.js';
import {INTERNAL_FUNCTION_HEADER, INTERNAL_FUNCTION_SECRET} from './auth.js';

/**
 * Creates a call to invoke another cloud function. In order to invoke the
 * call, the calling function must be configured with the INTERNAL_FUNCTION_SECRET,
 * which is how it authenticates itself to the callee.
 */
export function createCall<Req extends v.ObjectType, Res extends v.ObjectType>(
  functionName: string,
  reqSchema: Req,
  resSchema: Res,
) {
  return async (req: v.Infer<typeof reqSchema>) => {
    const url = cloudFunctionURL(functionName);
    const data = v.parse(req, reqSchema);

    const res = await fetch(url, {
      method: 'POST',
      headers: {
        [INTERNAL_FUNCTION_HEADER]: INTERNAL_FUNCTION_SECRET.value(),
        ['Content-Type']: 'application/json',
      },
      body: JSON.stringify({data}),
    });

    if (!res.ok) {
      throw new Error(`${res.status}: ${await res.text()}`);
    }
    return resSchema.parse(await res.json(), {mode: 'passthrough'});
  };
}
