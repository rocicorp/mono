import * as v from 'shared/src/valita.js';
import {astSchema} from 'zero-protocol';
import {jsonValueSchema} from '../../../types/bigint-json.js';
import {versionFromLexi} from '../../../types/lexi-version.js';
import {versionString} from './paths.js';

export const cvrVersionSchema = v.object({
  /**
   * The database `stateVersion` with which the rows in the CVR are consistent.
   */
  stateVersion: v.string(), // LexiVersion

  /**
   * `minorVersion` is subversion of `stateVersion` that is initially absent for each
   * `stateVersion`, and incremented for configuration changes that affect the contents
   * of the CVR such as:
   *
   * * client set changes
   * * query set changes
   * * query transformation changes (which may happen for changes
   *   in server-side logic or authorization policies)
   *
   * Such configuration changes are always correlated with a change to one or more
   * `/meta/...` records in the CVR, often (but not always) with corresponding
   * patches in `/patches/meta/...`.
   *
   * When the `stateVersion` moves forward, the `minorVersion` is reset to absent.
   * In this manner it behaves like the analogous concept in semantic versioning.
   */
  minorVersion: v.number().optional(),
});

export type CVRVersion = v.Infer<typeof cvrVersionSchema>;

export type NullableCVRVersion = CVRVersion | null;

export function cmpVersions(
  a: NullableCVRVersion,
  b: NullableCVRVersion,
): number {
  return a === null && b === null
    ? 0
    : a === null
    ? -1
    : b === null
    ? 1
    : a.stateVersion < b.stateVersion
    ? -1
    : a.stateVersion > b.stateVersion
    ? 1
    : (a.minorVersion ?? 0) - (b.minorVersion ?? 0);
}

export function versionToCookie(v: CVRVersion): string {
  return versionString(v);
}

export function versionToNullableCookie(v: NullableCVRVersion): string | null {
  return v === null ? null : versionToCookie(v);
}

export function cookieToVersion(cookie: string | null): NullableCVRVersion {
  if (cookie === null) {
    return null;
  }
  const parts = cookie.split('.');
  const stateVersion = parts[0];
  switch (parts.length) {
    case 1: {
      versionFromLexi(stateVersion); // Purely for validation.
      return {stateVersion};
    }
    case 2: {
      const minorVersion = versionFromLexi(parts[1]);
      if (minorVersion > BigInt(Number.MAX_SAFE_INTEGER)) {
        throw new Error(`minorVersion ${parts[1]} exceeds max safe integer`);
      }
      return {stateVersion, minorVersion: Number(minorVersion)};
    }
    default:
      throw new TypeError(`Invalid cookie ${cookie}`);
  }
}

// Last Active tracking.

export const lastActiveSchema = v.object({epochMillis: v.number()});
export type LastActive = v.Infer<typeof lastActiveSchema>;

export const cvrIDSchema = v.object({id: v.string()});
export type CvrID = v.Infer<typeof cvrIDSchema>;

const cvrRecordSchema = v.object({
  /**
   * CVR records store the CVRVersion at which record was last "put" into the CVR,
   * which corresponds with a patch row that can be cleaned up when the record
   * is deleted (or updated in the case of rows).
   *
   * Note that delete patches are not tracked, as tombstones are not stored,
   * so logic to expire old patch entries is still needed to bound storage usage.
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
   * * Query "expansion" to include primary keys and query-execution-related columns
   * * Authorization transforms
   *
   * Transformations depend on conditions that are independent of the db state version,
   * such as server-side logic and authorization policies. As such, the version of a CVR
   * version may need to be advanced independent of db state changes. This is done
   * via the `minorVersion` counter of the CVRVersion object, which is used to account
   * for both changes to the query set and changes to query transformations (which are
   * effectively remove-old-query + add-new-query).
   *
   * Note that the transformed AST itself is **not** stored, as the result of the previous
   * transformation is not useful in and of itself. If the current transformation results in
   * a different hash than that of the transformation used for the last version of the CVR,
   * it is simply handled by invalidating the existing rows, re-executed the query with
   * the new transformation, and advancing the CVR's `minorVersion` and this query's
   * `transformationVersion`.
   *
   * Note that the transformationHash is only stored when the query has reached the "gotten"
   * state. If the query is in the "desired" but not yet "gotten" state, the field is absent.
   */
  transformationHash: v.string().optional(),

  /**
   * The CVR version corresponding to the `transformationHash`. This essentially tracks when
   * this version of the query was effectively added to the CVR (as opposed to the `putPatch`,
   * which is simply when the client was notified that its query was added to the gotten set).
   * Catchup of clients from old CVR versions require executing all queries with newer
   * `transformationVersion`.
   */
  transformationVersion: cvrVersionSchema.optional(),

  // For queries, the putPatch indicates when query was added to the got set,
  // and is absent if not yet gotten.
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
  rowKey: v.record(jsonValueSchema),
});

export const metaRecordSchema = v.union(
  cvrVersionSchema,
  lastActiveSchema,
  clientRecordSchema,
  queryRecordSchema,
);

// Union type of rows under "/meta/..."x" for fetching all rows in a single list() call.
export type MetaRecord = v.Infer<typeof metaRecordSchema>;

export type RowID = v.Infer<typeof rowIDSchema>;

export const rowRecordSchema = cvrRecordSchema.extend({
  id: rowIDSchema,
  rowVersion: v.string(), // '_0_version' of the row
  queriedColumns: v.record(v.array(v.string())), // column => query IDs
});

export type RowRecord = v.Infer<typeof rowRecordSchema>;

export const patchSchema = v.object({
  type: v.union(v.literal('client'), v.literal('row'), v.literal('query')),
  op: v.union(v.literal('put'), v.literal('del')),
});

export const putRowPatchSchema = patchSchema.extend({
  type: v.literal('row'),
  op: v.literal('put'),
  id: rowIDSchema,
  rowVersion: v.string(), // '_0_version' of the row
  columns: v.array(v.string()),
});

export const delRowPatchSchema = patchSchema.extend({
  type: v.literal('row'),
  op: v.literal('del'),
  id: rowIDSchema,
});

export const rowPatchSchema = v.union(putRowPatchSchema, delRowPatchSchema);

export type RowPatch = v.Infer<typeof rowPatchSchema>;

export const queryPatchSchema = patchSchema.extend({
  type: v.literal('query'),
  id: v.string(),
  clientID: v.string().optional(), // defined for "desired", undefined for "got"
});

export type QueryPatch = v.Infer<typeof queryPatchSchema>;

export type PutQueryPatch = QueryPatch & {op: 'put'};
export type DelQueryPatch = QueryPatch & {op: 'del'};

export const clientPatchSchema = patchSchema.extend({
  type: v.literal('client'),
  id: v.string(),
});

export type ClientPatch = v.Infer<typeof clientPatchSchema>;
