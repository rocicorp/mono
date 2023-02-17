import {z} from 'zod';

// TODO: Do we maybe want to send the client timestamp for any reason?
// Server could reply with its time. Seems useful ... somehow.
export const pingBodySchema = z.object({});
export const pingMessageSchema = z.tuple([z.literal('ping'), pingBodySchema]);

export type PingBody = z.infer<typeof pingBodySchema>;
export type PingMessage = z.infer<typeof pingMessageSchema>;
