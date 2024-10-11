import * as v from '../../shared/src/valita.js';

export const warmBodySchema = v.object({
  payload: v.string(),
});

export const warmMessageSchema = v.tuple([v.literal('warm'), warmBodySchema]);

export type WarmBody = v.Infer<typeof warmBodySchema>;
export type WarmMessage = v.Infer<typeof warmMessageSchema>;
