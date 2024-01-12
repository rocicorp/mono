import {getFunctions, httpsCallable} from 'firebase/functions';
import * as v from 'shared/src/valita.js';

export const warmupRequestSchema = v.object({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _warm_: v.literal(true),
});

export const warmupResponseSchema = v.object({
  // eslint-disable-next-line @typescript-eslint/naming-convention
  _warmed_: v.literal(true),
});

export type WarmupRequest = v.Infer<typeof warmupRequestSchema>;
export type WarmupResponse = v.Infer<typeof warmupResponseSchema>;

// eslint-disable-next-line @typescript-eslint/naming-convention
const WARMUP_REQUEST: WarmupRequest = {_warm_: true} as const;

// eslint-disable-next-line @typescript-eslint/naming-convention
export const WARMUP_RESPONSE: WarmupResponse = {_warmed_: true} as const;

export type WarmupCaller = {
  warm: () => void;
};

export type FunctionCaller<Req, Res> = WarmupCaller & {
  call: (req: Req) => Promise<Res>;
};

export function createCaller<
  Req extends v.ObjectType,
  Res extends v.ObjectType,
>(
  functionName: string,
  reqSchema: Req,
  resSchema: Res,
): FunctionCaller<v.Infer<typeof reqSchema>, v.Infer<typeof resSchema>> {
  return {
    call: async req => {
      const callable = httpsCallable(getFunctions(), functionName);
      const result = await callable(req);

      // Make forwards-compatible by ignoring unknown (i.e. new) fields.
      return resSchema.parse(result.data, {mode: 'passthrough'});
    },
    warm: () => {
      const callable = httpsCallable(getFunctions(), functionName);
      void callable(WARMUP_REQUEST).catch(_ => {
        /* ignored */
      });
    },
  };
}
