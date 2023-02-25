import * as s from 'superstruct';
import {nullableVersionSchema, versionSchema} from './version.js';
import {patchSchema} from './patch.js';

const pokeBodySchema = s.object({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationIDChanges: s.record(s.string(), s.number()),
  patch: patchSchema,
  timestamp: s.number(),

  // When we change to multiple pokes per WS message we should only have one
  // requestID per message.
  requestID: s.string(),
});

export const pokeMessageSchema = s.tuple([s.literal('poke'), pokeBodySchema]);

export type PokeBody = s.Infer<typeof pokeBodySchema>;
export type PokeMessage = s.Infer<typeof pokeMessageSchema>;
