import * as v from '@badrap/valita';

export const pongBodySchema = v.object({});
export const pongMessageSchema = v.tuple([v.literal('pong'), pongBodySchema]);

export type PongBody = v.Infer<typeof pongBodySchema>;
export type PongMessage = v.Infer<typeof pongMessageSchema>;
