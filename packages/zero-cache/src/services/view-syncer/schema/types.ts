import {jsonValueSchema} from '../../../../../shared/src/bigint-json.ts';
import {jsonSchema} from '../../../../../shared/src/json-schema.ts';
import * as v from '../../../../../shared/src/valita.ts';
import {astSchema} from '../../../../../zero-protocol/src/ast.ts';
import {versionFromLexi, versionToLexi} from '../../../types/lexi-version.ts';
import {majorVersionToString} from '../../../types/state-version.ts';
import {ttlClockSchema} from '../ttl-clock.ts';
import type {QueriesRow} from './cvr.ts';

export const cvrVersionSchema = v.object({
  /**
   * The database `stateVersion` with which the rows in the CVR are consistent.
   */
  stateVersion: v.string(), // LexiVersion

  /**
   * `configVersion` is subversion of `stateVersion` that is initially absent for each
   * `stateVersion`, and incremented for configuration changes that affect the contents
   * of the CVR such as:
   *
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
  configVersion: v.number().optional(),
});

export type CVRVersion = v.Infer<typeof cvrVersionSchema>;

export const EMPTY_CVR_VERSION: CVRVersion = {
  stateVersion: majorVersionToString(0),
} as const;

export function oneAfter(v: NullableCVRVersion): CVRVersion {
  return v === null
    ? {stateVersion: majorVersionToString(0)}
    : {
        stateVersion: v.stateVersion,
        configVersion: (v.configVersion ?? 0) + 1,
      };
}

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
            : (a.configVersion ?? 0) - (b.configVersion ?? 0);
}

export function maxVersion(a: CVRVersion, b?: CVRVersion): CVRVersion {
  return !b ? a : cmpVersions(b, a) > 0 ? b : a;
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
  return versionFromString(cookie);
}

// Last Active tracking.

export const cvrIDSchema = v.object({id: v.string()});
export type CvrID = v.Infer<typeof cvrIDSchema>;

const cvrRecordSchema = v.object({
  /**
   * CVR records store the CVRVersion at which the record was last patched into
   * the CVR, which corresponds with a patch row that is cleaned up when the
   * record is changed (updated, deleted, and re-added in the case of rows).
   *
   * Tombstones are stored for row records but not for config records. This means
   * that "orphaned" delete patches for config records may exist, and therefore
   * scans of config patches must always run until the end of the list. On the
   * contrary, for row patches, the row record tombstones allow cleanup of delete
   * patches.
   */
  patchVersion: cvrVersionSchema,
});

export const clientRecordSchema = v.object({
  /** The client ID, of which there can be multiple for a client group view. */
  id: v.string(),

  /** The client's desired query IDs. Patch information is stored in the QueryRecord. */
  desiredQueryIDs: v.array(v.string()),
});

export type ClientRecord = v.Infer<typeof clientRecordSchema>;

export const baseQueryRecordSchema = v.object({
  /** The client-specified ID used to identify this query. Typically a hash. */
  id: v.string(),

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
   * this version of the query was effectively added to the CVR (as opposed to the
   * `patchVersion`, which is simply when the client was notified that its query was added
   * to the gotten set). Catchup of clients from old CVR versions require executing all
   * queries with a newer `transformationVersion`.
   */
  transformationVersion: cvrVersionSchema.optional(),
});

/**
 * Internal queries track rows in the database for internal use, such as the
 * `lastMutationID`s in the `zero.clients` table. They participate in the standard
 * invalidation / update logic for row contents, but not in the desired/got or
 * size-based quota logic for client-requested queries.
 */
export const internalQueryRecordSchema = baseQueryRecordSchema.extend({
  type: v.literal('internal'),
  ast: astSchema,
});

export type InternalQueryRecord = v.Infer<typeof internalQueryRecordSchema>;

const clientStateSchema = v.object({
  /**
   * The time at which the query was last inactivated. If this undefined or
   * missing then the query is active.
   *
   * Desired queries are always active and have an undefined inactivatedAt.
   */
  inactivatedAt: ttlClockSchema.optional(),

  /**
   * TTL, time to live in milliseconds. If the query is not updated within this
   * time. The time to live is the time after it has become inactive. Negative
   * values are treated as `'forever'`.
   *
   * We do clamp this to a maximum of 10 minutes, so that queries do not
   * live for a very long time in the CVR.
   */
  ttl: v.number(),

  /**
   * The version at which the client state changed (i.e. individual `patchVersion`s).
   */
  version: cvrVersionSchema,
});

const externalQueryRecordSchema = baseQueryRecordSchema.extend({
  /**
   * The client state for this query, which includes the inactivatedAt, ttl and
   * version. The client state is stored in a record with the client ID as the
   * key.
   */
  clientState: v.record(clientStateSchema),

  // For queries, the `patchVersion` indicates when query was added to the got set,
  // and is absent if not yet gotten.
  patchVersion: cvrVersionSchema.optional(),
});

export const clientQueryRecordSchema = externalQueryRecordSchema.extend({
  type: v.literal('client'),

  /** The original AST as supplied by the client. */
  ast: astSchema,
});

export type ClientQueryRecord = v.Infer<typeof clientQueryRecordSchema>;

export const customQueryRecordSchema = externalQueryRecordSchema.extend({
  type: v.literal('custom'),
  name: v.string(),
  args: v.readonly(v.array(jsonSchema)),
});

export type CustomQueryRecord = v.Infer<typeof customQueryRecordSchema>;

export const queryRecordSchema = v.union(
  clientQueryRecordSchema,
  customQueryRecordSchema,
  internalQueryRecordSchema,
);

export type QueryRecord = v.Infer<typeof queryRecordSchema>;

export const rowIDSchema = v.object({
  schema: v.string(),
  table: v.string(),
  rowKey: v.record(jsonValueSchema),
});

export type RowID = v.Infer<typeof rowIDSchema>;

export const rowRecordSchema = cvrRecordSchema.extend({
  id: rowIDSchema,
  rowVersion: v.string(), // '_0_version' of the row
  // query hashes => refCount, or `null` for a row that was removed from the
  // view (i.e. tombstone).
  refCounts: v.record(v.number()).nullable(),
});

export type RowRecord = v.Infer<typeof rowRecordSchema>;

export const patchSchema = v.object({
  type: v.literalUnion('row', 'query'),
  op: v.literalUnion('put', 'del'),
});

export const putRowPatchSchema = patchSchema.extend({
  type: v.literal('row'),
  op: v.literal('put'),
  id: rowIDSchema,
  rowVersion: v.string(), // '_0_version' of the row
});

export type PutRowPatch = v.Infer<typeof putRowPatchSchema>;

export const delRowPatchSchema = patchSchema.extend({
  type: v.literal('row'),
  op: v.literal('del'),
  id: rowIDSchema,
});

export type DelRowPatch = v.Infer<typeof delRowPatchSchema>;

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

export const metadataPatchSchema = queryPatchSchema;

export type MetadataPatch = v.Infer<typeof metadataPatchSchema>;

export function versionString(v: CVRVersion) {
  // The separator (e.g. ":") needs to be lexicographically greater than the
  // storage key path separator (e.g. "/") so that "01/row-hash" is less than "01:01/row-hash".
  // In particular, the traditional separator for major.minor versions (".") does not
  // satisfy this quality.
  return v.configVersion
    ? `${v.stateVersion}:${versionToLexi(v.configVersion)}`
    : v.stateVersion;
}

export function versionFromString(str: string): CVRVersion {
  const parts = str.split(':');
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
      return {stateVersion, configVersion: Number(minorVersion)};
    }
    default:
      throw new TypeError(`Invalid version string ${str}`);
  }
}

export function queryRecordToQueryRow(
  clientGroupID: string,
  query: QueryRecord,
): QueriesRow {
  switch (query.type) {
    case 'internal':
      return {
        clientGroupID,
        queryHash: query.id,
        clientAST: query.ast,
        queryName: null,
        queryArgs: null,
        patchVersion: null,
        transformationHash: query.transformationHash ?? null,
        transformationVersion: maybeVersionString(query.transformationVersion),
        internal: true,
        deleted: false, // put vs del "got" query
      };
    case 'client':
      return {
        clientGroupID,
        queryHash: query.id,
        clientAST: query.ast,
        queryName: null,
        queryArgs: null,
        patchVersion: maybeVersionString(query.patchVersion),
        transformationHash: query.transformationHash ?? null,
        transformationVersion: maybeVersionString(query.transformationVersion),
        internal: null,
        deleted: false, // put vs del "got" query
      };
    case 'custom':
      return {
        clientGroupID,
        queryHash: query.id,
        clientAST: null,
        queryName: query.name,
        queryArgs: query.args,
        patchVersion: maybeVersionString(query.patchVersion),
        transformationHash: query.transformationHash ?? null,
        transformationVersion: maybeVersionString(query.transformationVersion),
        internal: null,
        deleted: false, // put vs del "got" query
      };
  }
}

export const maybeVersionString = (v: CVRVersion | undefined) =>
  v ? versionString(v) : null;
