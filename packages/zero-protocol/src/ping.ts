import * as v from '../../shared/src/valita.ts';

const pingBodySchema = v.object({});
export const pingMessageSchema = v.tuple([v.literal('ping'), pingBodySchema]);

export type PingMessage = v.Infer<typeof pingMessageSchema>;
