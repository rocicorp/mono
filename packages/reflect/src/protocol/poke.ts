import {z} from 'zod';
import {nullableVersionSchema, versionSchema} from '../types/version.js';
import {patchSchema} from './patch.js';

const pokeBodySchema = z.object({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationIDChanges: z.record(z.string(), z.number()),
  patch: patchSchema,
  timestamp: z.number(),

  // When we change to multiple pokes per WS message we should only have one
  // requestID per message.
  requestID: z.string(),
});

export const pokeMessageSchema = z.tuple([z.literal('poke'), pokeBodySchema]);

export type PokeBody = z.infer<typeof pokeBodySchema>;
export type PokeMessage = z.infer<typeof pokeMessageSchema>;
