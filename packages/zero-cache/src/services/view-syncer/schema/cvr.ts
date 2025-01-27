import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {stringCompare} from '../../../../../shared/src/string-compare.ts';
import {
  type JSONObject,
  type JSONValue,
  stringify,
} from '../../../types/bigint-json.ts';
import {normalizedKeyOrder, type RowKey} from '../../../types/row-key.ts';
import {
  type RowID,
  type RowRecord,
  versionFromString,
  versionString,
} from './types.ts';

export const PG_SCHEMA = 'cvr';

const CREATE_CVR_SCHEMA = `CREATE SCHEMA IF NOT EXISTS cvr;`;

export type InstancesRow = {
  clientGroupID: string;
  version: string;
  lastActive: number;
  replicaVersion: string | null;
  owner: string | null;
  grantedAt: number | null;
};

const CREATE_CVR_INSTANCES_TABLE = `
CREATE TABLE cvr.instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,        -- Sortable representation of CVRVersion, e.g. "5nbqa2w:09"
  "lastActive"     TIMESTAMPTZ NOT NULL, -- For garbage collection
  "replicaVersion" TEXT,                 -- Identifies the replica (i.e. initial-sync point) from which the CVR data comes.
  "owner"          TEXT,                 -- The ID of the task / server that has been granted ownership of the CVR.
  "grantedAt"      TIMESTAMPTZ           -- The time at which the current owner was last granted ownership (most recent connection time).
);
`;

export function compareInstancesRows(a: InstancesRow, b: InstancesRow) {
  return stringCompare(a.clientGroupID, b.clientGroupID);
}

export type ClientsRow = {
  clientGroupID: string;
  clientID: string;
  patchVersion: string;
  deleted: boolean | null;
};

const CREATE_CVR_CLIENTS_TABLE = `
CREATE TABLE cvr.clients (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "patchVersion"       TEXT NOT NULL,  -- Version at which added or deleted
  deleted              BOOL,           -- put vs del client patch

  PRIMARY KEY ("clientGroupID", "clientID"),

  CONSTRAINT fk_clients_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES cvr.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX client_patch_version ON cvr.clients ("patchVersion");
`;

export function compareClientsRows(a: ClientsRow, b: ClientsRow) {
  const clientGroupIDComp = stringCompare(a.clientGroupID, b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  return stringCompare(a.clientID, b.clientID);
}

export type QueriesRow = {
  clientGroupID: string;
  queryHash: string;
  clientAST: JSONValue;
  patchVersion: string | null;
  transformationHash: string | null;
  transformationVersion: string | null;
  internal: boolean | null;
  deleted: boolean | null;
};

const CREATE_CVR_QUERIES_TABLE = `
CREATE TABLE cvr.queries (
  "clientGroupID"         TEXT,
  "queryHash"             TEXT,
  "clientAST"             JSONB NOT NULL,
  "patchVersion"          TEXT,  -- NULL if only desired but not yet "got"
  "transformationHash"    TEXT,
  "transformationVersion" TEXT,
  "internal"              BOOL,  -- If true, no need to track / send patches
  "deleted"               BOOL,  -- put vs del "got" query

  PRIMARY KEY ("clientGroupID", "queryHash"),

  CONSTRAINT fk_queries_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES cvr.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX queries_patch_version ON cvr.queries ("patchVersion" NULLS FIRST);
`;

export function compareQueriesRows(a: QueriesRow, b: QueriesRow) {
  const clientGroupIDComp = stringCompare(a.clientGroupID, b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  return stringCompare(a.queryHash, b.queryHash);
}

export type DesiresRow = {
  clientGroupID: string;
  clientID: string;
  queryHash: string;
  patchVersion: string;
  deleted: boolean | null;
};

const CREATE_CVR_DESIRES_TABLE = `
CREATE TABLE cvr.desires (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "queryHash"          TEXT,
  "patchVersion"       TEXT NOT NULL,
  "deleted"            BOOL,  -- put vs del "desired" query

  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),

  CONSTRAINT fk_desires_client
    FOREIGN KEY("clientGroupID", "clientID")
    REFERENCES cvr.clients("clientGroupID", "clientID"),

  CONSTRAINT fk_desires_query
    FOREIGN KEY("clientGroupID", "queryHash")
    REFERENCES cvr.queries("clientGroupID", "queryHash")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX desires_patch_version ON cvr.desires ("patchVersion");
`;

export function compareDesiresRows(a: DesiresRow, b: DesiresRow) {
  const clientGroupIDComp = stringCompare(a.clientGroupID, b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  const clientIDComp = stringCompare(a.clientID, b.clientID);
  if (clientIDComp !== 0) {
    return clientIDComp;
  }
  return stringCompare(a.queryHash, b.queryHash);
}

export type RowsRow = {
  clientGroupID: string;
  schema: string;
  table: string;
  rowKey: JSONObject;
  rowVersion: string;
  patchVersion: string;
  refCounts: {[queryHash: string]: number} | null;
};

export function rowsRowToRowID(rowsRow: RowsRow): RowID {
  return {
    schema: rowsRow.schema,
    table: rowsRow.table,
    rowKey: rowsRow.rowKey as Record<string, JSONValue>,
  };
}

export function rowsRowToRowRecord(rowsRow: RowsRow): RowRecord {
  return {
    id: rowsRowToRowID(rowsRow),
    rowVersion: rowsRow.rowVersion,
    patchVersion: versionFromString(rowsRow.patchVersion),
    refCounts: rowsRow.refCounts,
  };
}

export function rowRecordToRowsRow(
  clientGroupID: string,
  rowRecord: RowRecord,
): RowsRow {
  return {
    clientGroupID,
    schema: rowRecord.id.schema,
    table: rowRecord.id.table,
    rowKey: rowRecord.id.rowKey as Record<string, JSONValue>,
    rowVersion: rowRecord.rowVersion,
    patchVersion: versionString(rowRecord.patchVersion),
    refCounts: rowRecord.refCounts,
  };
}

export function compareRowsRows(a: RowsRow, b: RowsRow) {
  const clientGroupIDComp = stringCompare(a.clientGroupID, b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  const schemaComp = stringCompare(a.schema, b.schema);
  if (schemaComp !== 0) {
    return schemaComp;
  }
  const tableComp = stringCompare(b.table, b.table);
  if (tableComp !== 0) {
    return tableComp;
  }
  return stringCompare(
    stringifySorted(a.rowKey as RowKey),
    stringifySorted(b.rowKey as RowKey),
  );
}

/**
 * Note: Although `clientGroupID` logically references the same column in
 * `cvr.instances`, a FOREIGN KEY constraint must not be declared as the
 * `cvr.rows` TABLE needs to be updated without affecting the
 * `SELECT ... FOR UPDATE` lock when `cvr.instances` is updated.
 */
const CREATE_CVR_ROWS_TABLE = `
CREATE TABLE cvr.rows (
  "clientGroupID"    TEXT,
  "schema"           TEXT,
  "table"            TEXT,
  "rowKey"           JSONB,
  "rowVersion"       TEXT NOT NULL,
  "patchVersion"     TEXT NOT NULL,
  "refCounts"        JSONB,  -- {[queryHash: string]: number}, NULL for tombstone

  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey")
);

-- For catchup patches.
CREATE INDEX row_patch_version ON cvr.rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr.rows WHERE "refCounts" ?| array[...queryHashes...];
CREATE INDEX row_ref_counts ON cvr.rows USING GIN ("refCounts");
`;

/**
 * The version of the data in the `cvr.rows` table. This may lag
 * `version` in `cvr.instances` but eventually catches up, modulo
 * exceptional circumstances like a server crash.
 *
 * The `rowsVersion` is tracked in a separate table (as opposed to
 * a column in the `cvr.instances` table) so that general `cvr` updates
 * and `row` updates can be executed independently without serialization
 * conflicts.
 *
 * Note: Although `clientGroupID` logically references the same column in
 * `cvr.instances`, a FOREIGN KEY constraint must not be declared as the
 * `cvr.rows` TABLE needs to be updated without affecting the
 * `SELECT ... FOR UPDATE` lock when `cvr.instances` is updated.
 */
export const CREATE_CVR_ROWS_VERSION_TABLE = `
CREATE TABLE cvr."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY,
  "version"       TEXT NOT NULL
);
`;

export type RowsVersionRow = {
  clientGroupID: string;
  version: string;
};

const CREATE_CVR_TABLES =
  CREATE_CVR_SCHEMA +
  CREATE_CVR_INSTANCES_TABLE +
  CREATE_CVR_CLIENTS_TABLE +
  CREATE_CVR_QUERIES_TABLE +
  CREATE_CVR_DESIRES_TABLE +
  CREATE_CVR_ROWS_TABLE +
  CREATE_CVR_ROWS_VERSION_TABLE;

export async function setupCVRTables(
  lc: LogContext,
  db: postgres.TransactionSql,
) {
  lc.info?.(`Setting up CVR tables`);
  await db.unsafe(CREATE_CVR_TABLES);
}

function stringifySorted(r: RowKey) {
  return stringify(normalizedKeyOrder(r));
}
