import * as v from '../../shared/src/valita.ts';
import {mutationsPatchSchema} from './mutations-patch.ts';
import {queriesPatchSchema} from './queries-patch.ts';
import {rowsPatchSchema} from './row-patch.ts';
import {nullableVersionSchema, versionSchema} from './version.ts';

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
 * Through protocol version 51, the poke continues with zero to many
 * `poke-part` JSON messages. Starting in version 52, it continues with zero to
 * many binary `pokeChunk` messages. Each binary message starts with a message
 * type byte. Concatenating and decoding the payload after that byte produces a
 * PokePartBody array. pokeChunk boundaries have no semantic meaning.
 *
 * Finally, the poke ends with a `poke-end` message. The decoded patches can
 * now be applied as a whole to update from `baseCookie` to `cookie`.
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
  /**
   * This field is always set if the poke contains a `rowsPatch`.
   * It may be absent for patches that only update clients and queries.
   */
  schemaVersions: v
    .object({
      minSupportedVersion: v.number(),
      maxSupportedVersion: v.number(),
    })
    .optional(),
  timestamp: v.number().optional(),
});

export const pokePartBodySchema = v.object({
  // Changes to last mutation id by client id.
  lastMutationIDChanges: v.record(v.number()).optional(),
  // Patches to the desired query sets by client id.
  desiredQueriesPatches: v.record(queriesPatchSchema).optional(),
  // Patches to the set of queries for which entities are sync'd in
  // rowsPatch.
  gotQueriesPatch: queriesPatchSchema.optional(),
  // Patches to the rows set.
  rowsPatch: rowsPatchSchema.optional(),
  // Mutation results patch
  mutationsPatch: mutationsPatchSchema.optional(),
  pokeID: v.string(),
});

/**
 * The logical payload carried by binary poke chunks. Chunk boundaries are
 * arbitrary; concatenating and decoding all chunks for a poke produces this
 * array.
 */
export const pokePartsSchema = v.array(pokePartBodySchema);

export const pokeEndBodySchema = v.object({
  pokeID: v.string(),
  // Note: This should be ignored (and may be empty) if cancel === `true`.
  cookie: versionSchema,
  // If `true`, the poke with id `pokeID` should be discarded without
  // applying it.
  cancel: v.boolean().optional(),
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
export type PokeParts = v.Infer<typeof pokePartsSchema>;
export type PokeEndBody = v.Infer<typeof pokeEndBodySchema>;

/** The first byte identifying a binary WebSocket message as a poke chunk. */
export const POKE_CHUNK_MESSAGE_TYPE = 0x00;

/** The first protocol version that receives binary poke chunks. */
export const POKE_CHUNK_PROTOCOL_VERSION = 52;

/** The last protocol version that receives JSON pokePart messages. */
export const LAST_POKE_PART_PROTOCOL_VERSION = 51;

/** A tagged binary poke chunk WebSocket message. */
export type PokeChunk = Uint8Array;

export type PokeStartMessage = v.Infer<typeof pokeStartMessageSchema>;
export type PokePartMessage = v.Infer<typeof pokePartMessageSchema>;
export type PokeEndMessage = v.Infer<typeof pokeEndMessageSchema>;
