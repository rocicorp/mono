import * as v from 'shared/src/valita.js';

// TODO: Do we maybe want to send the client timestamp for any reason?
// Server could reply with its time. Seems useful ... somehow.
export const pingBodySchema = v.object({});
export const pingMessageSchema = v.tuple([v.literal('ping'), pingBodySchema]);

export type PingBody = v.Infer<typeof pingBodySchema>;
export type PingMessage = v.Infer<typeof pingMessageSchema>;
