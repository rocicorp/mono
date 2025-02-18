import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
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

export function cvrSchema(shardID: string) {
  return `cvr_${shardID}`;
}

// For readability in the sql statements.
function schema(shardID: string) {
  return ident(cvrSchema(shardID));
}

function createSchema(shardID: string) {
  return `CREATE SCHEMA IF NOT EXISTS ${schema(shardID)};`;
}

export type InstancesRow = {
  clientGroupID: string;
  version: string;
  lastActive: number;
  replicaVersion: string | null;
  owner: string | null;
  grantedAt: number | null;
};

function createInstancesTable(shardID: string) {
  return `
CREATE TABLE ${schema(shardID)}.instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,        -- Sortable representation of CVRVersion, e.g. "5nbqa2w:09"
  "lastActive"     TIMESTAMPTZ NOT NULL, -- For garbage collection
  "replicaVersion" TEXT,                 -- Identifies the replica (i.e. initial-sync point) from which the CVR data comes.
  "owner"          TEXT,                 -- The ID of the task / server that has been granted ownership of the CVR.
  "grantedAt"      TIMESTAMPTZ           -- The time at which the current owner was last granted ownership (most recent connection time).
);
`;
}

export function compareInstancesRows(a: InstancesRow, b: InstancesRow) {
  return stringCompare(a.clientGroupID, b.clientGroupID);
}

export type ClientsRow = {
  clientGroupID: string;
  clientID: string;
  /** @deprecated */
  patchVersion: string;
  /** @deprecated */
  deleted: boolean | null;
};

function createClientsTable(shardID: string) {
  // patchVersion and deleted are not used. Remove after all readers are migrated.
  return `
CREATE TABLE ${schema(shardID)}.clients (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "patchVersion"       TEXT NOT NULL,  -- Deprecated
  "deleted"            BOOL,           -- Deprecated

  PRIMARY KEY ("clientGroupID", "clientID"),

  CONSTRAINT fk_clients_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES ${schema(shardID)}.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX client_patch_version
  ON ${schema(shardID)}.clients ("patchVersion");
`;
}
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

function createQueriesTable(shardID: string) {
  return `
CREATE TABLE ${schema(shardID)}.queries (
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
    REFERENCES ${schema(shardID)}.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX queries_patch_version 
  ON ${schema(shardID)}.queries ("patchVersion" NULLS FIRST);
`;
}

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
  ttl: number | null;
  expiresAt: number | null;
  inactivatedAt: number | null;
};

function createDesiresTable(shardID: string) {
  return `
CREATE TABLE ${schema(shardID)}.desires (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "queryHash"          TEXT,
  "patchVersion"       TEXT NOT NULL,
  "deleted"            BOOL,  -- put vs del "desired" query
  "ttl"                INTERVAL,  -- Time to live for this client
  "expiresAt"          TIMESTAMPTZ,  -- Time at which this row expires
  "inactivatedAt"      TIMESTAMPTZ,  -- Time at which this row was inactivated

  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),

  CONSTRAINT fk_desires_client
    FOREIGN KEY("clientGroupID", "clientID")
    REFERENCES ${ident(
      cvrSchema(shardID),
    )}.clients("clientGroupID", "clientID"),

  CONSTRAINT fk_desires_query
    FOREIGN KEY("clientGroupID", "queryHash")
    REFERENCES ${ident(
      cvrSchema(shardID),
    )}.queries("clientGroupID", "queryHash")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX desires_patch_version
  ON ${schema(shardID)}.desires ("patchVersion");

CREATE INDEX desires_expires_at
  ON ${schema(shardID)}.desires ("expiresAt");

CREATE INDEX desires_inactivated_at
  ON ${schema(shardID)}.desires ("inactivatedAt");
`;
}

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
function createRowsTable(shardID: string) {
  return `
CREATE TABLE ${schema(shardID)}.rows (
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
CREATE INDEX row_patch_version 
  ON ${schema(shardID)}.rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr_shard.rows WHERE "refCounts" ?| array[...queryHashes...];
CREATE INDEX row_ref_counts ON ${schema(shardID)}.rows 
  USING GIN ("refCounts");
`;
}

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
export function createRowsVersionTable(shardID: string) {
  return `
CREATE TABLE ${schema(shardID)}."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY,
  "version"       TEXT NOT NULL
);
`;
}

export type RowsVersionRow = {
  clientGroupID: string;
  version: string;
};

function createTables(shardID: string) {
  return (
    createSchema(shardID) +
    createInstancesTable(shardID) +
    createClientsTable(shardID) +
    createQueriesTable(shardID) +
    createDesiresTable(shardID) +
    createRowsTable(shardID) +
    createRowsVersionTable(shardID)
  );
}

export async function setupCVRTables(
  lc: LogContext,
  db: postgres.TransactionSql,
  shardID: string,
) {
  lc.info?.(`Setting up CVR tables`);
  await db.unsafe(createTables(shardID));
}

function stringifySorted(r: RowKey) {
  return stringify(normalizedKeyOrder(r));
}
