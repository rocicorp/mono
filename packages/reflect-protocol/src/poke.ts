import * as v from 'shared/src/valita.js';
import {patchSchema} from './patch.js';
import {nullableVersionSchema, versionSchema} from './version.js';

export const pokeSchema = v.object({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationIDChanges: v.record(v.number()),
  // Patch keys are clientIDs.  Currently only the keys are used to indicate
  // presense/absence of clientID, values are ignored but may be used
  // in the future.
  presence: patchSchema.optional(),
  patch: patchSchema,
  timestamp: v.number().optional(),
  // Following debug fields are set when client's connect request has
  // debugPerf=true
  debugOriginTimestamp: v.number().optional(),
  debugServerReceivedTimestamp: v.number().optional(),
  debugServerSentTimestamp: v.number().optional(),
});

export const pokeBodySchema = v.object({
  pokes: v.array(pokeSchema),
  requestID: v.string(),
  // Set when client's connect request has debugPerf=true
  debugServerBufferMs: v.number().optional(),
});
export const pokeMessageSchema = v.tuple([v.literal('poke'), pokeBodySchema]);

export type Poke = v.Infer<typeof pokeSchema>;
export type PokeBody = v.Infer<typeof pokeBodySchema>;
export type PokeMessage = v.Infer<typeof pokeMessageSchema>;
