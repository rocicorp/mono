import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import type postgres from 'postgres';
import {cvrSchema, type ShardID} from '../../../../types/shards.ts';

// For readability in the sql statements.
function schema(shard: ShardID) {
  return ident(cvrSchema(shard));
}

function createSchema(shard: ShardID) {
  return `CREATE SCHEMA IF NOT EXISTS ${schema(shard)};`;
}

function createInstancesTable(shard: ShardID) {
  return `
CREATE TABLE ${schema(shard)}.instances (
  "clientGroupID"  TEXT PRIMARY KEY,
  "version"        TEXT NOT NULL,        -- Sortable representation of CVRVersion, e.g. "5nbqa2w:09"
  "lastActive"     TIMESTAMPTZ NOT NULL, -- For garbage collection
  "replicaVersion" TEXT,                 -- Identifies the replica (i.e. initial-sync point) from which the CVR data comes.
  "owner"          TEXT,                 -- The ID of the task / server that has been granted ownership of the CVR.
  "grantedAt"      TIMESTAMPTZ           -- The time at which the current owner was last granted ownership (most recent connection time).
);
`;
}

function createClientsTable(shard: ShardID) {
  // patchVersion and deleted are not used. Remove after all readers are migrated.
  return `
CREATE TABLE ${schema(shard)}.clients (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "patchVersion"       TEXT NOT NULL,  -- Deprecated
  "deleted"            BOOL,           -- Deprecated

  PRIMARY KEY ("clientGroupID", "clientID"),

  CONSTRAINT fk_clients_client_group
    FOREIGN KEY("clientGroupID")
    REFERENCES ${schema(shard)}.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX client_patch_version
  ON ${schema(shard)}.clients ("patchVersion");
`;
}

function createQueriesTable(shard: ShardID) {
  return `
CREATE TABLE ${schema(shard)}.queries (
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
    REFERENCES ${schema(shard)}.instances("clientGroupID")
);

-- For catchup patches.
CREATE INDEX queries_patch_version 
  ON ${schema(shard)}.queries ("patchVersion" NULLS FIRST);
`;
}

function createDesiresTable(shard: ShardID) {
  return `
CREATE TABLE ${schema(shard)}.desires (
  "clientGroupID"      TEXT,
  "clientID"           TEXT,
  "queryHash"          TEXT,
  "patchVersion"       TEXT NOT NULL,
  "deleted"            BOOL,  -- put vs del "desired" query
  "ttl"                INTERVAL,  -- Time to live for this client
  "expiresAt"          TIMESTAMPTZ,  -- DEPRECATED Time at which this row expires
  "inactivatedAt"      TIMESTAMPTZ,  -- Time at which this row was inactivated

  PRIMARY KEY ("clientGroupID", "clientID", "queryHash"),

  CONSTRAINT fk_desires_client
    FOREIGN KEY("clientGroupID", "clientID")
    REFERENCES ${ident(cvrSchema(shard))}.clients("clientGroupID", "clientID"),

  CONSTRAINT fk_desires_query
    FOREIGN KEY("clientGroupID", "queryHash")
    REFERENCES ${ident(cvrSchema(shard))}.queries("clientGroupID", "queryHash")
    ON DELETE CASCADE
);

-- For catchup patches.
CREATE INDEX desires_patch_version
  ON ${schema(shard)}.desires ("patchVersion");

CREATE INDEX desires_expires_at
  ON ${schema(shard)}.desires ("expiresAt");

CREATE INDEX desires_inactivated_at
  ON ${schema(shard)}.desires ("inactivatedAt");
`;
}

/**
 * Note: Although `clientGroupID` logically references the same column in
 * `cvr.instances`, a FOREIGN KEY constraint must not be declared as the
 * `cvr.rows` TABLE needs to be updated without affecting the
 * `SELECT ... FOR UPDATE` lock when `cvr.instances` is updated.
 */
function createRowsTable(shard: ShardID) {
  return `
CREATE TABLE ${schema(shard)}.rows (
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
  ON ${schema(shard)}.rows ("patchVersion");

-- For listing rows returned by one or more query hashes. e.g.
-- SELECT * FROM cvr_shard.rows WHERE "refCounts" ?| array[...queryHashes...];
CREATE INDEX row_ref_counts ON ${schema(shard)}.rows 
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
function createRowsVersionTable(shard: ShardID) {
  return `
CREATE TABLE ${schema(shard)}."rowsVersion" (
  "clientGroupID" TEXT PRIMARY KEY,
  "version"       TEXT NOT NULL
);
`;
}

function createTables(shard: ShardID) {
  return (
    createSchema(shard) +
    createInstancesTable(shard) +
    createClientsTable(shard) +
    createQueriesTable(shard) +
    createDesiresTable(shard) +
    createRowsTable(shard) +
    createRowsVersionTable(shard)
  );
}

export async function setupCVRTables(
  lc: LogContext,
  db: postgres.TransactionSql,
  shard: ShardID,
) {
  lc.info?.(`Setting up CVR tables`);
  await db.unsafe(createTables(shard));
}
