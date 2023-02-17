import * as s from 'superstruct';

// TODO: Do we maybe want to send the client timestamp for any reason?
// Server could reply with its time. Seems useful ... somehow.
export const pingBodySchema = s.type({});
export const pingMessageSchema = s.tuple([s.literal('ping'), pingBodySchema]);

export type PingBody = s.Infer<typeof pingBodySchema>;
export type PingMessage = s.Infer<typeof pingMessageSchema>;
