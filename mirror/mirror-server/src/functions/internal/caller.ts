import type {ReadonlyJSONObject} from 'shared/src/json.js';
import * as v from 'shared/src/valita.js';
import {cloudFunctionURL} from '../../config/index.js';
import {INTERNAL_FUNCTION_HEADER, INTERNAL_FUNCTION_SECRET} from './auth.js';

export interface FunctionCaller<
  Req extends ReadonlyJSONObject,
  Res extends ReadonlyJSONObject,
> {
  call(request: Req): Promise<Res>;
}

export interface FunctionCallerFactory {
  createCaller<Req extends ReadonlyJSONObject, Res extends ReadonlyJSONObject>(
    functionName: string,
    reqSchema: v.Type<Req>,
    resSchema: v.Type<Res>,
  ): FunctionCaller<Req, Res>;
}

export class InternalFunctionCallerFactory implements FunctionCallerFactory {
  createCaller<Req extends ReadonlyJSONObject, Res extends ReadonlyJSONObject>(
    functionName: string,
    reqSchema: v.Type<Req>,
    resSchema: v.Type<Res>,
  ): FunctionCaller<Req, Res> {
    const url = cloudFunctionURL(functionName);
    return {
      call: async (req: Req) => {
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
        return v.parse(await res.json(), resSchema, 'passthrough');
      },
    };
  }
}
