import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';

export const cvrVersionSchema = v.object({
  /**
   * The database `stateVersion` with which the rows in the CVR are consistent.
   */
  stateVersion: v.string(), // LexiVersion

  /**
   * `querySetVersion` is an initially absent counter that is incremented when
   * the stateVersion remains the same but either (1) the query set changes or
   * (2) query transformations change (the latter of which happens for changes
   * in server-side logic or authorization policies).
   *
   * When the `stateVersion` moves forward, the `queryVersion` can be reset to
   * absent.
   */
  querySetVersion: v.number().optional(),
});

export type CVRVersion = v.Infer<typeof cvrVersionSchema>;

export const queryRecordSchema = v.object({
  /** The client-specified ID used to identify this query. */
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
   * via the `querySetVersion` counter of the CVRVersion object, which is used to account
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

  // TODO: Iron this out.
  // estimatedBytes: v.number(),
  // evictable: v.boolean().optional(),
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

export const rowRecordSchema = v.object({
  row: rowViewSchema,
  queryIDs: v.array(v.string()),
});

export type RowRecord = v.Infer<typeof rowRecordSchema>;

export const rowPatchSchema = v.object({
  op: v.union(v.literal('set'), v.literal('del')),
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

export const queryPatch = v.object({
  op: v.union(v.literal('set'), v.literal('del')),
  id: v.string(),
});

export type QueryPatch = v.Infer<typeof queryPatch>;
