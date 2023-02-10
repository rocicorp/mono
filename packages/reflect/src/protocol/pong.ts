import {z} from 'zod';

export const pongBodySchema = z.object({});
export const pongMessageSchema = z.tuple([z.literal('pong'), pongBodySchema]);

export type PongBody = z.infer<typeof pongBodySchema>;
export type PongMessage = z.infer<typeof pongMessageSchema>;
