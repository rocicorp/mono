import type * as v from 'shared/src/valita.js';
import {
  connectFunctionsEmulator,
  getFunctions,
  httpsCallable,
} from 'firebase/functions';

export function createCall<Req extends v.ObjectType, Res extends v.ObjectType>(
  functionName: string,
  reqSchema: Req,
  resSchema: Res,
) {
  return async (req: v.Infer<typeof reqSchema>) => {
    const functions = getFunctions();
    // TODO(darick): Make this a parameter/config
    connectFunctionsEmulator(functions, '127.0.0.1', 5001);

    const callable = httpsCallable(functions, functionName);
    const result = await callable(req);
    return resSchema.parse(result.data);
  };
}
