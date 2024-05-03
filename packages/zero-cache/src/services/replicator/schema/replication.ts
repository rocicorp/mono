/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {getPublicationInfo} from '../tables/published.js';
import {CREATE_INVALIDATION_TABLES} from './invalidation.js';

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
  // The change log contains row changes. Only the latest version of each row is
  // recorded, with each new version replacing the previous (via the UNIQUE constraint).
  // This is optimal because catchup / IVM is always executed up to the current state of
  // the database snapshot (i.g. never to a past version), and thus incremental row state
  // is unnecessary (and in fact, unwanted). This also constrains the size of the ChangeLog
  // to `O(database-size)` as opposed to `O(history-size)`.
  //
  // * `op`        : 't' for table truncation, 's' for set (insert/update), and 'd' for delete
  // * `rowKey`    : JSONB row key, as `{[$columnName]: $columnValue}`, or '{}' for TRUNCATE
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
    "rowKey"       JSONB        NOT NULL,
    "row"          JSON,
    CONSTRAINT "PK_change_log" PRIMARY KEY("stateVersion", "schema", "table", "rowKey"),
    CONSTRAINT "RK_change_log" UNIQUE("schema", "table", "rowKey")
  );
`;

/**
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

  const replicated = await getPublicationInfo(tx);
  const alterStmts = replicated.tables.map(
    table => tx`
      ALTER TABLE ${tx(table.schema)}.${tx(table.name)} 
        ALTER COLUMN ${tx(ZERO_VERSION_COLUMN_NAME)} DROP DEFAULT, 
        ALTER COLUMN ${tx(ZERO_VERSION_COLUMN_NAME)} SET NOT NULL;
        `,
  );

  await Promise.all(alterStmts);
  await tx.unsafe(CREATE_REPLICATION_TABLES + CREATE_INVALIDATION_TABLES);
}
