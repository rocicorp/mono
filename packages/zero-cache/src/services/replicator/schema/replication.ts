/**
 * Replication metadata, used for incremental view maintenance and catchup.
 *
 * These tables are created atomically in {@link setupReplicationTables}
 * after the logical replication handoff when initial data synchronization has completed.
 */

import type {LogContext} from '@rocicorp/logger';
import {Database} from 'better-sqlite3';
import type postgres from 'postgres';
import {toLexiVersion} from 'zero-cache/src/types/lsn.js';
import type {LexiVersion} from '../../../types/lexi-version.js';

export const ZERO_VERSION_COLUMN_NAME = '_0_version';

const CREATE_REPLICATION_STATE_SCHEMA =
  // publications     : JSON stringified array of publication names
  // watermark        : Opaque, upstream-specific watermark denoting the point from which replication
  //                    should continue. For a Postgres upstream, for example, this is the LSN string.
  // nextStateVersion : The value to use for the _0_version column of rows in the _next_ transaction.
  //                    This is generally a lexicographically sortable representation of the watermark.
  // lock             : Auto-magic column for enforcing single-row semantics.
  `
  CREATE TABLE "_zero.ReplicationState" (
    publications TEXT NOT NULL,
    watermark TEXT NOT NULL,
    nextStateVersion TEXT NOT NULL,
    lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  )
  `;

export function initReplicationState(
  db: Database,
  publications: string[],
  lsn: string,
) {
  db.exec(CREATE_REPLICATION_STATE_SCHEMA);
  db.prepare(
    `
      INSERT INTO "_zero.ReplicationState" 
        (publications, watermark, nextStateVersion) VALUES (?,?,?)
    `,
  ).run(JSON.stringify(publications), lsn, toLexiVersion(lsn));
}

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
  //
  // Note that the row data itself is not stored; since catchup is always done at the current
  // snapshot of the database, row contents can instead be looked up from the database tables.
  `
  CREATE TABLE _zero."ChangeLog" (
    "stateVersion" VARCHAR(38)  NOT NULL,
    "schema"       VARCHAR(128) NOT NULL,
    "table"        VARCHAR(128) NOT NULL,
    "op"           CHAR         NOT NULL,
    "rowKey"       JSONB        NOT NULL,
    CONSTRAINT "PK_change_log" PRIMARY KEY("stateVersion", "schema", "table", "rowKey"),
    CONSTRAINT "RK_change_log" UNIQUE("schema", "table", "rowKey")
  );
`;

/**
 * Migration step that sets up the initialized Sync Replica for incremental replication.
 * This includes:
 *
 * * Setting up the internal _zero tables that track replication state.
 */

export async function setupReplicationTables(
  lc: LogContext,
  tx: postgres.TransactionSql,
  upstreamUri: string,
) {
  lc.info?.(`Setting up replication tables for ${upstreamUri}`);
  await tx.unsafe(CREATE_REPLICATION_TABLES);
}

export function queryStateVersion(db: postgres.Sql) {
  return db<
    {max: LexiVersion | null}[]
  >`SELECT MAX("stateVersion") FROM _zero."TxLog";`;
}

export async function queryLastLSN(db: postgres.Sql): Promise<string | null> {
  const result = await db<
    {lsn: string}[]
  >`SELECT "lsn" FROM _zero."TxLog" ORDER BY "stateVersion" desc LIMIT 1;`;
  return result.length ? result[0].lsn : null;
}
