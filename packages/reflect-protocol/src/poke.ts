import * as v from '@badrap/valita';
import {nullableVersionSchema, versionSchema} from './version.js';
import {patchSchema} from './patch.js';

export const pokeSchema = v.object({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationIDChanges: v.record(v.number()),
  patch: patchSchema,
  timestamp: v.number().optional(),
});

export const pokeBodySchema = v.object({
  pokes: v.array(pokeSchema),
  requestID: v.string(),
});
export const pokeMessageSchema = v.tuple([v.literal('poke'), pokeBodySchema]);

export type Poke = v.Infer<typeof pokeSchema>;
export type PokeBody = v.Infer<typeof pokeBodySchema>;
export type PokeMessage = v.Infer<typeof pokeMessageSchema>;
