import * as s from 'superstruct';
import {nullableVersionSchema, versionSchema} from './version.js';
import {patchSchema} from './patch.js';

export const pokeSchema = s.type({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationIDChanges: s.record(s.string(), s.number()),
  patch: patchSchema,
  timestamp: s.optional(s.number()),
});

export const pokeBodySchema = s.type({
  pokes: s.array(pokeSchema),
  requestID: s.string(),
});
export const pokeMessageSchema = s.tuple([s.literal('poke'), pokeBodySchema]);

export type Poke = s.Infer<typeof pokeSchema>;
export type PokeBody = s.Infer<typeof pokeBodySchema>;
export type PokeMessage = s.Infer<typeof pokeMessageSchema>;
