import {LogContext} from '@rocicorp/logger';
import postgres from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.js';
import {equals} from '../../../../../shared/src/set-utils.js';
import type {PostgresDB} from '../../../types/pg.js';
import type {Change} from './change.js';

export const PG_SCHEMA = 'cdc';

const CREATE_CDC_SCHEMA = `CREATE SCHEMA IF NOT EXISTS cdc;`;

export type ChangeLogEntry = {
  // A strictly monotonically increasing, lexicographically sortable
  // value that uniquely identifies a position in the change stream.
  watermark: string;
  change: Change;
};

const CREATE_CHANGE_LOG_TABLE = `
  CREATE TABLE cdc."changeLog" (
    watermark  TEXT,
    pos        INT8,
    change     JSONB NOT NULL,
    precommit  TEXT,  -- Only exists on commit entries. Purely for debugging.
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
  CREATE TABLE cdc."replicationConfig" (
    "replicaVersion" TEXT NOT NULL,
    "publications" TEXT[] NOT NULL,
    "resetRequired" BOOL,
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

export async function markResetRequired(db: PostgresDB) {
  await db`UPDATE cdc."replicationConfig" SET "resetRequired" = true`;
}

export async function ensureReplicationConfig(
  lc: LogContext,
  db: PostgresDB,
  config: ReplicationConfig,
  autoReset: boolean,
) {
  // Restrict the fields of the supplied `config`.
  const {publications, replicaVersion} = config;
  const replicaConfig = {publications, replicaVersion};

  await db.begin(async tx => {
    const results = await tx<
      {
        replicaVersion: string;
        publications: string[];
        resetRequired: boolean | null;
      }[]
    >`SELECT "replicaVersion", "publications", "resetRequired" FROM cdc."replicationConfig"`;

    if (results.length === 0) {
      return tx`INSERT INTO cdc."replicationConfig" ${tx(replicaConfig)}`;
    }

    const {replicaVersion, publications, resetRequired} = results[0];
    if (
      replicaVersion !== replicaConfig.replicaVersion ||
      !equals(new Set(publications), new Set(replicaConfig.publications))
    ) {
      lc.info?.(
        `Data in cdc tables @${replicaVersion} is incompatible ` +
          `with replica @${replicaConfig.replicaVersion}. Clearing tables.`,
      );
      return [
        tx`TRUNCATE TABLE cdc."changeLog"`,
        tx`TRUNCATE TABLE cdc."replicationConfig"`,
        tx`INSERT INTO cdc."replicationConfig" ${tx(replicaConfig)}`,
      ].map(stmt => stmt.execute());
    }

    if (resetRequired) {
      if (autoReset) {
        throw new AutoResetSignal();
      }
      lc.warn?.('reset required but auto-reset is disabled');
    }

    return [];
  });
}

export class AutoResetSignal extends AbortError {
  readonly name = 'AutoResetSignal';
}
