import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';

export const cvrVersionSchema = v.object({
  /**
   * The database `stateVersion` with which the rows in the CVR are consistent.
   */
  stateVersion: v.string(), // LexiVersion

  /**
   * `metaVersion` is an initially absent counter that is incremented when
   * the stateVersion remains the same but the metadata of the CVR changes,
   * including:
   * * client set changes
   * * query set changes
   * * query transformation changes (which may happen for changes
   *   in server-side logic or authorization policies)
   *
   * When the `stateVersion` moves forward, the `metaVersion` can be reset to
   * absent.
   */
  metaVersion: v.number().optional(),
});

export type CVRVersion = v.Infer<typeof cvrVersionSchema>;

const cvrRecordSchema = v.object({
  /**
   * CVR records store the CVRVersion at which record was "put" into the CVR,
   * which corresponds with a patch row that can be cleaned up when the record
   * is deleted (or updated in the case of rows).
   *
   * Note that this do delete patches are not tracked, as tombstones are
   * not stored, so logic to expire old patch entries is still needed to
   * bound storage usage.
   */
  putPatch: cvrVersionSchema,
});

export const clientRecordSchema = cvrRecordSchema.extend({
  /** The client ID, of which there can be multiple for a client group view. */
  id: v.string(),

  /** The client's desired query IDs. Patch information is stored in the QueryRecord. */
  desiredQueryIDs: v.array(v.string()),
});

export type ClientRecord = v.Infer<typeof clientRecordSchema>;

export const queryRecordSchema = cvrRecordSchema.extend({
  /** The client-specified ID used to identify this query. Typically a hash. */
  id: v.string(),

  /** The original AST as supplied by the client. */
  ast: astSchema,

  /**
   * The hash of the query after server-side transformations, which include:
   *
   * * Normalization (which may differ from what the client does)
   * * Query "explosion" to fetch primary keys of rows for JOINed tables
   * * Authorization transforms
   *
   * Transformations depend on conditions that are independent of the db state version,
   * such as server-side logic and authorization policies. As such, the version of a CVR
   * version may need to be advanced independent of db state changes. This is done
   * via the `metaVersion` counter of the CVRVersion object, which is used to account
   * for both changes to the query set and changes to query transformations (which are
   * effectively remove-old-query + add-new-query).
   *
   * Note that the transformed AST itself is **not** stored, as the result of the previous
   * transformation is not useful in and of itself. If the current transformation results in
   * a different hash than that of the transformation used for the last version of the CVR,
   * it is simply handled by invalidating the existing rows, re-executed the query with
   * the new transformation, and advancing the `querySerVersion`.
   */
  transformationHash: v.string(),

  // For queries, the putPatch indicates when query was added to the got set,
  // which can be undefined if not yet gotten.
  putPatch: cvrVersionSchema.optional(),

  // Maps each of the desiring client's IDs to the version at which
  // the queryID was added to their desired query set (i.e. individual `putPatch`es).
  desiredBy: v.record(cvrVersionSchema),

  // TODO: Iron this out.
  // estimatedBytes: v.number(),
  // lru information?
});

export type QueryRecord = v.Infer<typeof queryRecordSchema>;

export const rowIDSchema = v.object({
  schema: v.string(),
  table: v.string(),
  rowKeyHash: v.string(),
});

export type RowID = v.Infer<typeof rowIDSchema>;

/**
 * The RowView contains exactly the information needed to look up and project
 * the view of the row sent to and cached by the client. Specifically, the
 * primary key of the `ChangeLog` table consists of the columns:
 * * `stateVersion`
 * * `schema`
 * * `table`
 * * `rowKeyHash`
 *
 * From which the `rowKey` and `row` data can be fetched. The `columns` field
 * is then used to compute the projection of the `row` visible to the client.
 *
 * Note that the CVR never stores any row data itself---not even the row key.
 * This has the benefit of
 * 1. Resilience to large column values (DO storage has relatively low
 *    size limits)
 * 2. A privacy guarantee of only metadata being stored in Cloudflare.
 *    Database values are only persisted in the Postgres replica.
 */
export const rowViewSchema = v.object({
  id: rowIDSchema,
  stateVersion: v.string(),
  columns: v.array(v.string()),
});

export type RowView = v.Infer<typeof rowViewSchema>;

export const rowRecordSchema = cvrRecordSchema.extend({
  row: rowViewSchema,
  queryIDs: v.array(v.string()),
});

export type RowRecord = v.Infer<typeof rowRecordSchema>;

export const patchSchema = v.object({
  type: v.union(v.literal('client'), v.literal('row'), v.literal('query')),
  op: v.union(v.literal('put'), v.literal('del')),
});

export const rowPatchSchema = patchSchema.extend({
  type: v.literal('row'),
  // Note that the row key needs to be looked up from the ChangeLog even for
  // deletes, since only row key hashes are stored in the CVR.
  //
  // TODO: Figure out how to handle TRUNCATE, in which case there will not be a ChangeLog
  //       entry for the (deleted) row at the version in which TRUNCATE happens.
  //
  //       We can't use rely on the row's pre-TRUNCATE `stateVersion` because there is
  //       no guarantee that there is a ChangeLog entry at that version, since the row
  //       may pre-date all mutations in the ChangeLog).
  //
  //       Some options are to:
  //       1. Add a truncate (i.e. clear table) patch operation to the poke protocol.
  //       2. Not support TRUNCATE.
  //       3. Have the clients remember the rowKeyHash for each row. This seems a bit
  //          wasteful and requires exporting an otherwise server-internal identifier.
  //       4. Store the actual row key in the delete patch. This would counter the
  //          benefits of not storing database values in the CVR.
  row: rowViewSchema,
});

export type RowPatch = v.Infer<typeof rowPatchSchema>;

export const queryPatchSchema = patchSchema.extend({
  type: v.literal('query'),
  id: v.string(),
  clientID: v.string().optional(), // defined for "desired", undefined for "got"
});

export type QueryPatch = v.Infer<typeof queryPatchSchema>;

export const clientPatchSchema = patchSchema.extend({
  type: v.literal('client'),
  id: v.string(),
});

export type ClientPatch = v.Infer<typeof clientPatchSchema>;
