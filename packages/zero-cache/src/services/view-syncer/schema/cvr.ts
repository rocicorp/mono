import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import type {JSONValue} from '../../../types/bigint-json.js';
import {
  RowID,
  versionFromString,
  versionString,
  type RowRecord,
} from './types.js';

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

export type RowsRow = {
  clientGroupID: string;
  schema: string;
  table: string;
  rowKey: JSONValue;
  rowVersion: string;
  patchVersion: string;
  queriedColumns: {[queryHash: string]: string[]} | null;
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
    queriedColumns: rowsRow.queriedColumns,
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
    queriedColumns: rowRecord.queriedColumns,
  };
}

const CREATE_CVR_ROWS_TABLE = `
CREATE TABLE cvr.rows (
  "clientGroupID"    TEXT,
  "schema"           TEXT,
  "table"            TEXT,
  "rowKey"           JSONB,
  "rowVersion"       TEXT NOT NULL,
  "patchVersion"     TEXT NOT NULL,
  "queriedColumns"   JSONB,  -- {[queryHash: string]: string[]}, NULL for tombstone

  PRIMARY KEY ("clientGroupID", "schema", "table", "rowKey"),

  CONSTRAINT fk_rows_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES cvr.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX row_patch_version ON cvr.rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr.rows WHERE "queriedColumns" ?| array[...queryHashes...];
CREATE INDEX row_queried_columns ON cvr.rows USING GIN ("queriedColumns");
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
