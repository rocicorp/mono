import {versionToLexi} from '../../../types/lexi-version.js';
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
// /vs/cvr/{id}/meta/version: {stateVersion: LexiVersion, metaVersion?: number}
// /vs/cvr/{id}/meta/lastActive: {day: Date}  // Updated at most once per day
//
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord (desiredQueries)
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord
// /vs/cvr/{id}/meta/clients/{cid}: ClientRecord
//
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord (ast, transformationHash)
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord
// /vs/cvr/{id}/meta/queries/{qid}: QueryRecord
//
// /vs/cvr/{id}/rows/{schema}/{table}/{row-key-hash}: RowRecord (stateVersion, columns, queries)
// /vs/cvr/{id}/rows/{schema}/{table}/{row-key-hash}: RowRecord
// /vs/cvr/{id}/rows/{schema}/{table}/{row-key-hash}: RowRecord
//
// Note: /patches/... rows are ordered by {version-str} and thus types are interleaved.
// /vs/cvr/{id}/patches/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/{version-str}/clients/{cid}}: ClientPatch
// /vs/cvr/{id}/patches/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
// /vs/cvr/{id}/patches/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
// /vs/cvr/{id}/patches/{version-str}/rows/{schema}/{table}/{row-key-hash}: RowPatch
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}: QueryPatch (got)
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
// /vs/cvr/{id}/patches/{version-str}/queries/{qid}/clients/{cid}: QueryPatch (desired)
//
// -----------------------------------------------------------------------
// Last Active Index
// -----------------------------------------------------------------------
// /vs/lastActive/{day.toISOString()}/{cvrID}
// /vs/lastActive/{day.toISOString()}/{cvrID}
// /vs/lastActive/{day.toISOString()}/{cvrID}

export const schemaRoot = '/vs';

export class LastActiveIndex {
  entry(cvrID: string, lastActive: Date): string {
    return `${this.dayPrefix(lastActive)}/${cvrID}`;
  }

  /** dayPrefix is used for index scans to expunge very old CVRs. */
  dayPrefix(date: Date): string {
    const day = new Date(
      Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()),
    );
    const dateStr = day.toISOString();
    const dayStr = dateStr.substring(0, dateStr.indexOf('T'));
    return `/vs/lastActive/${dayStr}`;
  }
}

/** CVR-specific paths. */
export class CVRPaths {
  readonly root: string;

  constructor(cvrID: string) {
    this.root = `/vs/cvr/${cvrID}`;
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
    const schema = pathEscape(row.schema);
    const table = pathEscape(row.table);
    const hash = row.rowKeyHash;
    return `${this.root}/rows/${schema}/${table}/${hash}`;
  }

  clientPatch(
    cvrVersion: CVRVersion,
    client: ClientRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/${v}/clients/${client.id}`;
  }

  rowPatch(cvrVersion: CVRVersion, row: RowID): string {
    const v = versionString(cvrVersion);
    const schema = pathEscape(row.schema);
    const table = pathEscape(row.table);
    const hash = row.rowKeyHash;
    return `${this.root}/patches/${v}/rows/${schema}/${table}/${hash}`;
  }

  queryPatch(
    cvrVersion: CVRVersion,
    query: QueryRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/${v}/queries/${query.id}`;
  }

  desiredQueryPatch(
    cvrVersion: CVRVersion,
    query: QueryRecord | {id: string},
    client: ClientRecord | {id: string},
  ): string {
    const v = versionString(cvrVersion);
    return `${this.root}/patches/${v}/queries/${query.id}/clients/${client.id}`;
  }
}

const pathEscapeChars = /[\\/\\"]/;

function pathEscape(part: string) {
  // In the common case, schema and table appear as-is in the path, but if the
  // either have slashes or double quotes, JSON escape them to eliminate any
  // ambiguities.
  return pathEscapeChars.test(part) ? JSON.stringify(part) : part;
}

function versionString(v: CVRVersion) {
  return v.metaVersion === undefined
    ? v.stateVersion
    : `${v.stateVersion}-${versionToLexi(v.metaVersion)}`;
}
