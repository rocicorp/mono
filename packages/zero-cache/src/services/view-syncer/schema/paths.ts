import {versionToLexi} from '../../../types/lexi-version.js';
import {rowKeyHash} from '../../../types/row-key.js';
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
// /vs/cvr/{id}/meta/version: {stateVersion: LexiVersion, minorVersion?: number}
// /vs/cvr/{id}/meta/lastActive: {epochMillis: string}  // Can include non-version activity
//
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord (desiredQueries)
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord
//
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord (ast, transformationHash)
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord
//
// /vs/cvr/{id}/data/rows/{schema}/{table}/{row-key-hash}: RowRecord (stateVersion, columns, queries)
// /vs/cvr/{id}/data/rows/{schema}/{table}/{row-key-hash}: RowRecord
// /vs/cvr/{id}/data/rows/{schema}/{table}/{row-key-hash}: RowRecord
//
// Patches to data and metadata are indexed under separate prefixes.
// Although this means that both indexes need to be scanned to perform incremental
// catchup, it allows only the metadata index to be scanned when performing
// a re-query catchup (the metadata patch index being generally much smaller than
// the data patch index).
//
// /vs/cvr/{id}/patches/meta/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/meta/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/meta/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/patches/meta/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
//
// /vs/cvr/{id}/patches/data/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
// /vs/cvr/{id}/patches/data/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
// /vs/cvr/{id}/patches/data/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
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
    return `${this.root}/meta/`;
  }

  version(): string {
    return `${this.root}/meta/version`;
  }

  lastActive(): string {
    return `${this.root}/meta/lastActive`;
  }

  client(client: ClientRecord | {id: string}): string {
    return `${this.root}/meta/clients/${client.id}`;
  }

  query(query: QueryRecord | {id: string}): string {
    return `${this.root}/meta/queries/${query.id}`;
  }

  row(row: RowID): string {
    const {schema, table, rowKey} = row;
    const hash = rowKeyHash(rowKey);
    return `${this.root}/data/rows/${schema}/${table}/${hash}`;
  }

  rowPrefix(): string {
    return `${this.root}/data/rows/`;
  }

  clientPatch(
    cvrVersion: CVRVersion,
    client: ClientRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/meta/${v}/clients/${client.id}`;
  }

  rowPatch(cvrVersion: CVRVersion, row: RowID): string {
    const v = versionString(cvrVersion);
    const {schema, table, rowKey} = row;
    const hash = rowKeyHash(rowKey);
    return `${this.root}/patches/data/${v}/rows/${schema}/${table}/${hash}`;
  }

  queryPatch(
    cvrVersion: CVRVersion,
    query: QueryRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/meta/${v}/queries/${query.id}`;
  }

  desiredQueryPatch(
    cvrVersion: CVRVersion,
    query: QueryRecord | {id: string},
    client: ClientRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/meta/${v}/queries/${query.id}/clients/${client.id}`;
  }
}

export function versionString(v: CVRVersion) {
  return v.minorVersion
    ? `${v.stateVersion}.${versionToLexi(v.minorVersion)}`
    : v.stateVersion;
}
