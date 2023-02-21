import * as s from 'superstruct';

export const pongBodySchema = s.object({});
export const pongMessageSchema = s.tuple([s.literal('pong'), pongBodySchema]);

export type PongBody = s.Infer<typeof pongBodySchema>;
export type PongMessage = s.Infer<typeof pongMessageSchema>;
