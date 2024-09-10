import {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {equals} from 'shared/src/set-utils.js';
import {PostgresDB} from 'zero-cache/src/types/pg.js';
import {Change} from './change.js';

export const PG_SCHEMA = 'cdc';

const CREATE_CDC_SCHEMA = `CREATE SCHEMA IF NOT EXISTS cdc;`;

export type ChangeLogEntry = {
  // Lexicographically sortable representation of the LSN.
  watermark: string;
  // The position of the change within the transaction, with the initial
  // BEGIN starting at 0. This is needed for uniqueness / ordering because
  // multiple changes can share the same LSN.
  pos: number;

  change: Change;
};

const CREATE_CHANGE_LOG_TABLE = `
  CREATE TABLE cdc."ChangeLog" (
    watermark  pg_lsn,
    pos        INT8,
    change     JSONB NOT NULL,

    PRIMARY KEY (watermark, pos)
  );
`;

/**
 * This mirrors the analogously named table in the SQLite replica
 * (`services/replicator/schema/replication-state.ts`), and is used
 * to detect when the replica has been reset and is no longer compatible
 * with the current ChangeLog.
 */
export type ReplicationConfig = {
  replicaVersion: string;
  publications: string[];
};

const CREATE_REPLICATION_CONFIG_TABLE = `
  CREATE TABLE cdc."ReplicationConfig" (
    "replicaVersion" TEXT NOT NULL,
    "publications" TEXT[] NOT NULL,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;

const CREATE_CDC_TABLES =
  CREATE_CDC_SCHEMA + CREATE_CHANGE_LOG_TABLE + CREATE_REPLICATION_CONFIG_TABLE;

export async function setupCDCTables(
  lc: LogContext,
  db: postgres.TransactionSql,
) {
  lc.info?.(`Setting up CDC tables`);
  await db.unsafe(CREATE_CDC_TABLES);
}

export async function ensureReplicationConfig(
  lc: LogContext,
  db: PostgresDB,
  config: ReplicationConfig,
) {
  // Restrict the fields of the supplied `config`.
  const {publications, replicaVersion} = config;
  const replicaConfig = {publications, replicaVersion};

  await db.begin(async tx => {
    const results = await tx<
      {
        replicaVersion: string;
        publications: string[];
      }[]
    >`SELECT "replicaVersion", "publications" FROM cdc."ReplicationConfig"`;

    if (results.length === 0) {
      return tx`INSERT INTO cdc."ReplicationConfig" ${tx(replicaConfig)}`;
    }

    const {replicaVersion, publications} = results[0];
    if (
      replicaVersion !== replicaConfig.replicaVersion ||
      !equals(new Set(publications), new Set(replicaConfig.publications))
    ) {
      lc.info?.(
        `Data in cdc tables @${replicaVersion} is incompatible ` +
          `with replica @${replicaConfig.replicaVersion}. Clearing tables.`,
      );
      return [
        tx`TRUNCATE TABLE cdc."ChangeLog"`,
        tx`UPDATE cdc."ReplicationConfig" SET ${tx(replicaConfig)}`,
      ].map(stmt => stmt.execute());
    }
    return [];
  });
}
