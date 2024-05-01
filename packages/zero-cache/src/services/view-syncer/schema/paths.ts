import {versionFromLexi, versionToLexi} from '../../../types/lexi-version.js';
import {rowIDHash} from '../../../types/row-key.js';
import type {CVRVersion, ClientRecord, QueryRecord, RowID} from './types.js';

// At a glance
//
// -----------------------------------------------------------------------
// Schema Version
// -----------------------------------------------------------------------
// /vs/storage_schema_meta
//
// -----------------------------------------------------------------------
// CVR
// -----------------------------------------------------------------------
//
// Abbreviations:
// m: metadata
// d: data
// p: patches
// r: rows
// q: queries
// c: clients
//
// /vs/cvr/{id}/m/version: {stateVersion: LexiVersion, minorVersion?: number}
// /vs/cvr/{id}/m/lastActive: {epochMillis: string}  // Can include non-version activity
//
// /vs/cvr/{id}/m/c/{cid}: ClientRecord (desiredQueries)
// /vs/cvr/{id}/m/c/{cid}: ClientRecord
// /vs/cvr/{id}/m/c/{cid}: ClientRecord
//
// /vs/cvr/{id}/m/q/{qid}: QueryRecord (ast, transformationHash)
// /vs/cvr/{id}/m/q/{qid}: QueryRecord
// /vs/cvr/{id}/m/q/{qid}: QueryRecord
//
// /vs/cvr/{id}/d/r/{row-id-hash}: RowRecord (stateVersion, columns, queries)
// /vs/cvr/{id}/d/r/{row-id-hash}: RowRecord
// /vs/cvr/{id}/d/r/{row-id-hash}: RowRecord
//
// Patches to data and metadata are indexed under separate prefixes.
// Although this means that both indexes need to be scanned to perform incremental
// catchup, it allows only the metadata index to be scanned when performing
// a re-query catchup (the metadata patch index being generally much smaller than
// the data patch index).
//
// /vs/cvr/{id}/p/m/{version-str}/c/{cid}}: ClientPatch
// /vs/cvr/{id}/p/m/{version-str}/c/{cid}}: ClientPatch
// /vs/cvr/{id}/p/m/{version-str}/c/{cid}}: ClientPatch
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}: QueryPatch (got)
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}: QueryPatch (got)
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}: QueryPatch (got)
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}/c/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}/c/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/p/m/{version-str}/q/{qid}/c/{cid}: QueryPatch (desired)
//
// /vs/cvr/{id}/p/d/{version-str}/r/{row-id-hash}: RowPatch
// /vs/cvr/{id}/p/d/{version-str}/r/{row-id-hash}: RowPatch
// /vs/cvr/{id}/p/d/{version-str}/r/{row-id-hash}: RowPatch
//
// -----------------------------------------------------------------------
// Last Active Index
// -----------------------------------------------------------------------
// /vs/lastActive/{day.toISOString()}/{cvrID}: CvrID
// /vs/lastActive/{day.toISOString()}/{cvrID}: CvrID
// /vs/lastActive/{day.toISOString()}/{cvrID}: CvrID

export const schemaRoot = '/vs';

export const lastActiveIndex = {
  entry(cvrID: string, lastActive: number): string {
    return `${this.dayPrefix(lastActive)}/${cvrID}`;
  },

  /** dayPrefix is used for index scans to expunge very old CVRs. */
  dayPrefix(epochMillis: number): string {
    const dateStr = new Date(epochMillis).toISOString();
    const dayStr = dateStr.substring(0, dateStr.indexOf('T'));
    return `/vs/lastActive/${dayStr}`;
  },
};

/** CVR-specific paths. */
export class CVRPaths {
  readonly root: string;

  constructor(cvrID: string) {
    this.root = `/vs/cvr/${cvrID}`;
  }

  metaPrefix(): string {
    return `${this.root}/m/`;
  }

  version(): string {
    return `${this.root}/m/version`;
  }

  lastActive(): string {
    return `${this.root}/m/lastActive`;
  }

  client(client: ClientRecord | {id: string}): string {
    return `${this.root}/m/c/${client.id}`;
  }

  query(query: QueryRecord | {id: string}): string {
    return `${this.root}/m/q/${query.id}`;
  }

  row(row: RowID): string {
    return `${this.root}/d/r/${rowIDHash(row)}`;
  }

  rowPrefix(): string {
    return `${this.root}/d/r/`;
  }

  rowPatchVersionPrefix(cvrVersion: CVRVersion): string {
    const v = versionString(cvrVersion);
    return `${this.root}/p/d/${v}/`;
  }

  rowPatch(v: CVRVersion, row: RowID): string {
    return `${this.rowPatchVersionPrefix(v)}r/${rowIDHash(row)}`;
  }

  versionFromPatchPath(path: string): CVRVersion {
    const start = this.root.length + '/p/d/'.length; // Also works for '/p/m/' for metadata patches.
    const end = path.indexOf('/', start);
    const version = path.substring(start, end);
    return versionFromString(version);
  }

  metadataPatchPrefix(): string {
    return `${this.root}/p/m/`;
  }

  metadataPatchVersionPrefix(cvrVersion: CVRVersion): string {
    const v = versionString(cvrVersion);
    return `${this.root}/p/m/${v}/`;
  }

  clientPatch(v: CVRVersion, client: ClientRecord | {id: string}): string {
    return `${this.metadataPatchVersionPrefix(v)}c/${client.id}`;
  }

  queryPatch(v: CVRVersion, query: QueryRecord | {id: string}): string {
    return `${this.metadataPatchVersionPrefix(v)}q/${query.id}`;
  }

  desiredQueryPatch(
    v: CVRVersion,
    query: QueryRecord | {id: string},
    client: ClientRecord | {id: string},
  ): string {
    return `${this.metadataPatchVersionPrefix(v)}q/${query.id}/c/${client.id}`;
  }
}

export function versionString(v: CVRVersion) {
  // The separator (e.g. ":") needs to be lexicographically greater than the
  // storage key path separator (e.g. "/") so that "01/row-hash" is less than "01:01/row-hash".
  // In particular, the traditional separator for major.minor versions (".") does not
  // satisfy this quality.
  return v.minorVersion
    ? `${v.stateVersion}:${versionToLexi(v.minorVersion)}`
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
      return {stateVersion, minorVersion: Number(minorVersion)};
    }
    default:
      throw new TypeError(`Invalid version string ${str}`);
  }
}
