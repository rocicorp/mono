/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {CREATE_INVALIDATION_TABLES} from './invalidation.js';
import {getPublicationInfo} from './published.js';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';

export const CREATE_REPLICATION_TABLES =
  // The transaction log maps each LSN to transaction information.
  // Note that the lsn may become optional for supporting non-Postgres upstreams.
  `
  CREATE SCHEMA IF NOT EXISTS _zero;
  CREATE TABLE _zero."TxLog" (
    "stateVersion" VARCHAR(38) NOT NULL,
    lsn            PG_LSN      NOT NULL,
    time           TIMESTAMPTZ NOT NULL,
    xid            INTEGER     NOT NULL,
    PRIMARY KEY("stateVersion")
  );
` +
  // The change log contains row changes.
  //
  // * `op`        : 't' for table truncation, 's' for set (insert/update), and 'd' for delete
  // * `rowKeyHash`: Hash of the row key for row identification (see {@link rowKeyHash}). Empty string for truncate op.
  // * `rowKey`    : JSON row key, as `{[$columnName]: $columnValue}`, or NULL for TRUNCATE
  // * `row`       : JSON formatted full row contents, NULL for DELETE / TRUNCATE
  //
  // Note that the `row` data is stored as JSON rather than JSONB to prioritize write
  // throughput, as replication is critical bottleneck in the system. Row values are
  // only needed for catchup, for which JSONB is not particularly advantageous over JSON.
  `
  CREATE TABLE _zero."ChangeLog" (
    "stateVersion" VARCHAR(38)  NOT NULL,
    "schema"       VARCHAR(128) NOT NULL,
    "table"        VARCHAR(128) NOT NULL,
    "op"           CHAR         NOT NULL,
    "rowKeyHash"   VARCHAR(22)  NOT NULL,
    "rowKey"       JSON,
    "row"          JSON,
    CONSTRAINT PK_change_log PRIMARY KEY("stateVersion", "schema", "table", "rowKeyHash")
  );
`; /**
 * Migration step that sets up the initialized Sync Replica for incremental replication.
 * This includes:
 *
 * * Setting up the internal _zero tables that track replication state.
 *
 * * Removing the _0_version DEFAULT (used only for initial sync)
 *   and requiring that it be NOT NULL. This is a defensive measure to
 *   enforce that the incremental replication logic always sets the _0_version.
 */

export async function setupReplicationTables(
  lc: LogContext,
  _replicaID: string,
  tx: postgres.TransactionSql,
  upstreamUri: string,
) {
  lc.info?.(`Setting up replication tables for ${upstreamUri}`);

  const replicated = await getPublicationInfo(tx, 'zero_');
  const alterStmts = Object.keys(replicated.tables).map(
    table => `
      ALTER TABLE ${table} 
        ALTER COLUMN ${ZERO_VERSION_COLUMN_NAME} DROP DEFAULT, 
        ALTER COLUMN ${ZERO_VERSION_COLUMN_NAME} SET NOT NULL;
        `,
  );

  await tx.unsafe(
    alterStmts.join('') +
      CREATE_REPLICATION_TABLES +
      CREATE_INVALIDATION_TABLES,
  );
}
