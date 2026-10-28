import * as v from '../../shared/src/valita.ts';

const pongBodySchema = v.object({});
export const pongMessageSchema = v.tuple([v.literal('pong'), pongBodySchema]);

export type PongMessage = v.Infer<typeof pongMessageSchema>;
