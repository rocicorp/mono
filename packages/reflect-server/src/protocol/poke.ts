import {nullableVersionSchema, versionSchema} from '../types/version.js';
import * as s from 'superstruct';
import {jsonSchema} from './json.js';

export const putOpSchema = s.type({
  op: s.literal('put'),
  key: s.string(),
  value: jsonSchema,
});

export const delOpSchema = s.type({
  op: s.literal('del'),
  key: s.string(),
});

export const patchOpSchema = s.union([putOpSchema, delOpSchema]);
export const patchSchema = s.array(patchOpSchema);

export const pokeBodySchema = s.type({
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

export type PutOp = s.Infer<typeof putOpSchema>;
export type DelOp = s.Infer<typeof delOpSchema>;
export type PatchOp = s.Infer<typeof patchOpSchema>;
export type Patch = s.Infer<typeof patchSchema>;
export type PokeBody = s.Infer<typeof pokeBodySchema>;
export type PokeMessage = s.Infer<typeof pokeMessageSchema>;
