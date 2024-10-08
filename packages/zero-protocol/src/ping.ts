import * as v from '../../shared/src/valita.js';

export const pingBodySchema = v.object({});
export const pingMessageSchema = v.tuple([v.literal('ping'), pingBodySchema]);

export type PingBody = v.Infer<typeof pingBodySchema>;
export type PingMessage = v.Infer<typeof pingMessageSchema>;
