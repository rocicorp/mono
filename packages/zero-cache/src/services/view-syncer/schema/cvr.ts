import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import type {JSONValue, JSONObject} from '../../../types/bigint-json.js';
import {
  RowID,
  versionFromString,
  versionString,
  type RowRecord,
} from './types.js';

export const PG_SCHEMA = 'cvr';

const CREATE_CVR_SCHEMA = `CREATE SCHEMA IF NOT EXISTS cvr;`;

export type InstancesRow = {
  clientGroupID: string;
  version: string;
  lastActive: Date;
};

const CREATE_CVR_INSTANCES_TABLE = `
CREATE TABLE cvr.instances (
  "clientGroupID" TEXT PRIMARY KEY,
  version         TEXT NOT NULL,        -- Sortable representation of CVRVersion, e.g. "5nbqa2w:09"
  "lastActive"    TIMESTAMPTZ NOT NULL -- For garbage collection
);
`;

export function compareInstancesRows(a: InstancesRow, b: InstancesRow) {
  return a.clientGroupID.localeCompare(b.clientGroupID);
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

export function compareClientsRow(a: ClientsRow, b: ClientsRow) {
  const clientGroupIDComp = a.clientGroupID.localeCompare(b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  return a.clientID.localeCompare(b.clientID);
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
  const clientGroupIDComp = a.clientGroupID.localeCompare(b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  return a.queryHash.localeCompare(b.queryHash);
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
  const clientGroupIDComp = a.clientGroupID.localeCompare(b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  const clientIDComp = a.clientID.localeCompare(b.clientID);
  if (clientIDComp !== 0) {
    return clientIDComp;
  }
  return a.queryHash.localeCompare(b.queryHash);
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
  const clientGroupIDComp = a.clientGroupID.localeCompare(b.clientGroupID);
  if (clientGroupIDComp !== 0) {
    return clientGroupIDComp;
  }
  const schemaComp = a.schema.localeCompare(b.schema);
  if (schemaComp !== 0) {
    return schemaComp;
  }
  const tableComp = b.table.localeCompare(b.table);
  if (tableComp !== 0) {
    return tableComp;
  }
  return stringifySorted(a.rowKey).localeCompare(stringifySorted(b.rowKey));
}

const CREATE_CVR_ROWS_TABLE = `
CREATE TABLE cvr.rows (
  "clientGroupID"    TEXT,
  "schema"           TEXT,
  "table"            TEXT,
  "rowKey"           JSONB,
  "rowVersion"       TEXT NOT NULL,
  "patchVersion"     TEXT NOT NULL,
  "refCounts"        JSONB,  -- {[queryHash: string]: number}, NULL for tombstone

  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey"),

  CONSTRAINT fk_rows_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES cvr.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX row_patch_version ON cvr.rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr.rows WHERE "refCounts" ?| array[...queryHashes...];
CREATE INDEX row_ref_counts ON cvr.rows USING GIN ("refCounts");
`;

const CREATE_CVR_TABLES =
  CREATE_CVR_SCHEMA +
  CREATE_CVR_INSTANCES_TABLE +
  CREATE_CVR_CLIENTS_TABLE +
  CREATE_CVR_QUERIES_TABLE +
  CREATE_CVR_DESIRES_TABLE +
  CREATE_CVR_ROWS_TABLE;

export async function setupCVRTables(
  lc: LogContext,
  db: postgres.TransactionSql,
) {
  lc.info?.(`Setting up CVR tables`);
  await db.unsafe(CREATE_CVR_TABLES);
}

function stringifySorted(o: JSONObject) {
  return JSON.stringify(o, Object.keys(o).sort());
}
