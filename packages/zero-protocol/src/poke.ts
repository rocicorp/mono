import * as v from '../../shared/src/valita.js';
import {clientsPatchSchema} from './clients-patch.js';
import {queriesPatchSchema} from './queries-patch.js';
import {rowsPatchSchema} from './row-patch.js';
import {nullableVersionSchema, versionSchema} from './version.js';

/**
 * Pokes use a multi-part format. Pokes send entity data to the client and can
 * be multiple mega-bytes in size. Using a multi-part format allows the server
 * to avoid having to have the full poke in memory at one time.
 *
 * Each poke is assigned a `pokeID`, a unique id (within the context of the
 * connection) for identifying the poke.  All messages for a poke will have the
 * same `pokeID`.
 *
 * A poke begins with a `poke-start` message which contains the `baseCookie`
 * the poke is updating from and the `cookie` the poke is updating to.
 *
 * The poke continues with zero to many `poke-part` messages, each of which
 * can contain patch parts.  These patch parts should be merged in the order
 * received.
 *
 * Finally, the poke ends with a `poke-end` message.  The merged `poke-parts`
 * can now be applied as a whole to update from `baseCookie` to `cookie`.
 *
 * Poke messages can be intermingled with other `down` messages, but cannot be
 * intermingled with poke messages for a different `pokeID`. If this is
 * observed it is an unexpected error; the client should ignore both pokes,
 * disconnect, and reconnect.
 */

export const pokeStartBodySchema = v.object({
  pokeID: v.string(),
  // We always specify a Version as our cookie, but Replicache starts clients
  // with initial cookie `null`, before the first request. So we have to be
  // able to send a base cookie with value `null` to match that state.
  baseCookie: nullableVersionSchema,
  cookie: versionSchema,
  schemaVersions: v.object({
    minSupportedVersion: v.number(),
    maxSupportedVersion: v.number(),
  }),
  timestamp: v.number().optional(),
});

export const pokePartBodySchema = v.object({
  pokeID: v.string(),
  // Changes to last mutation id by client id.
  lastMutationIDChanges: v.record(v.number()).optional(),
  // Patches to the set of "alive" clients (according to server) belonging to
  // this client group.
  clientsPatch: clientsPatchSchema.optional(),
  // Patches to the desired query sets by client id.
  desiredQueriesPatches: v.record(queriesPatchSchema).optional(),
  // Patches to the set of queries for which entities are sync'd in
  // rowsPatch.
  gotQueriesPatch: queriesPatchSchema.optional(),
  // Patches to the rows set.
  rowsPatch: rowsPatchSchema.optional(),
});

export const pokeEndBodySchema = v.object({
  pokeID: v.string(),
});

export const pokeStartMessageSchema = v.tuple([
  v.literal('pokeStart'),
  pokeStartBodySchema,
]);
export const pokePartMessageSchema = v.tuple([
  v.literal('pokePart'),
  pokePartBodySchema,
]);
export const pokeEndMessageSchema = v.tuple([
  v.literal('pokeEnd'),
  pokeEndBodySchema,
]);

export type PokeStartBody = v.Infer<typeof pokeStartBodySchema>;
export type PokePartBody = v.Infer<typeof pokePartBodySchema>;
export type PokeEndBody = v.Infer<typeof pokeEndBodySchema>;

export type PokeStartMessage = v.Infer<typeof pokeStartMessageSchema>;
export type PokePartMessage = v.Infer<typeof pokePartMessageSchema>;
export type PokeEndMessage = v.Infer<typeof pokeEndMessageSchema>;
