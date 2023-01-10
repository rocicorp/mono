import {z} from 'zod';
import {nullableVersionSchema, versionSchema} from '../types/version.js';
import {jsonSchema} from './json.js';

const putOpSchema = z.object({
  op: z.literal('put'),
  key: z.string(),
  value: jsonSchema,
});

const delOpSchema = z.object({
  op: z.literal('del'),
  key: z.string(),
});

const patchOpSchema = z.union([putOpSchema, delOpSchema]);
const patchSchema = z.array(patchOpSchema);

const pokeBodySchema = z.object({
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  lastMutationID: z.number(),
  patch: patchSchema,
  clientID: z.optional(z.string()),
  timestamp: z.number(),
});

export const pokeMessageSchema = z.tuple([
  z.literal('poke'),
  z.union([pokeBodySchema, z.array(pokeBodySchema)]),
]);

export type PutOp = z.infer<typeof putOpSchema>;
export type DelOp = z.infer<typeof delOpSchema>;
export type PatchOp = z.infer<typeof patchOpSchema>;
export type Patch = z.infer<typeof patchSchema>;
export type PokeBody = z.infer<typeof pokeBodySchema>;
export type PokeMessage = z.infer<typeof pokeMessageSchema>;
