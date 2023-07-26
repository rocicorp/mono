import type * as v from 'shared/src/valita.js';
import {getFunctions, httpsCallable} from 'firebase/functions';

export function createCall<Req extends v.ObjectType, Res extends v.ObjectType>(
  functionName: string,
  reqSchema: Req,
  resSchema: Res,
) {
  return async (req: v.Infer<typeof reqSchema>) => {
    const callable = httpsCallable(getFunctions(), functionName);
    const result = await callable(req);

    // Make forwards-compatible by ignoring unknown (i.e. new) fields.
    return resSchema.parse(result.data, {mode: 'passthrough'});
  };
}
