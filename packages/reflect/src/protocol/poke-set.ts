import * as z from 'zod';
import {pokeBodySchema} from './poke';

export const pokeFrameSchema = z.object({
  frame: z.number(),
  poke: pokeBodySchema,
});

export const pokeSetSchema = z.object({
  rate: z.optional(z.number()),
  frames: z.array(pokeFrameSchema),
});

export const pokeSetMessageSchema = z.tuple([
  z.literal('pokeset'),
  pokeSetSchema,
]);

export type PokeFrame = z.infer<typeof pokeFrameSchema>;
export type PokeSet = z.infer<typeof pokeSetSchema>;
export type PokeSetMessage = z.infer<typeof pokeSetMessageSchema>;
