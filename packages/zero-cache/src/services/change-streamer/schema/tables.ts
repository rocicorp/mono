import {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import postgres, {type PendingQuery, type Row} from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../../types/shards.ts';
import type {Change} from '../../change-source/protocol/current/data.ts';

// For readability in the sql statements.
function schema(shard: ShardID) {
  return ident(cdcSchema(shard));
}

export const PG_SCHEMA = 'cdc';

function createSchema(shard: ShardID) {
  return `CREATE SCHEMA IF NOT EXISTS ${schema(shard)};`;
}

export type ChangeLogEntry = {
  // A strictly monotonically increasing, lexicographically sortable
  // value that uniquely identifies a position in the change stream.
  watermark: string;
  change: Change;
};

function createChangeLogTable(shard: ShardID) {
  // Note: The "change" column used to be JSONB, but that was problematic in that
  // it does not handle the NULL unicode character.
  // https://vladimir.varank.in/notes/2021/01/you-dont-insert-unicode-null-character-as-postgres-jsonb/
  return `
  CREATE TABLE ${schema(shard)}."changeLog" (
    watermark  TEXT,
    pos        INT8,
    change     JSON NOT NULL,
    precommit  TEXT,  -- Only exists on commit entries. Purely for debugging.
    PRIMARY KEY (watermark, pos)
  );
`;
}

/**
 * Tracks the watermark from which to resume the change stream and the
 * current owner (task ID) acting as the single writer to the changeLog.
 */
export type ReplicationState = {
  lastWatermark: string;
  owner: string | null;
};

export function createReplicationStateTable(shard: ShardID) {
  return `
  CREATE TABLE ${schema(shard)}."replicationState" (
    "lastWatermark" TEXT NOT NULL,
    "owner" TEXT,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;
}

/**
 * This mirrors the analogously named table in the SQLite replica
 * (`services/replicator/schema/replication-state.ts`), and is used
 * to detect when the replica has been reset and is no longer compatible
 * with the current ChangeLog.
 */
export type ReplicationConfig = {
  replicaVersion: string;
  publications: readonly string[];
};

function createReplicationConfigTable(shard: ShardID) {
  return `
  CREATE TABLE ${schema(shard)}."replicationConfig" (
    "replicaVersion" TEXT NOT NULL,
    "publications" TEXT[] NOT NULL,
    "resetRequired" BOOL,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;
}

function createTables(shard: ShardID) {
  return (
    createSchema(shard) +
    createChangeLogTable(shard) +
    createReplicationStateTable(shard) +
    createReplicationConfigTable(shard)
  );
}

export async function setupCDCTables(
  lc: LogContext,
  db: postgres.TransactionSql,
  shard: ShardID,
) {
  lc.info?.(`Setting up CDC tables`);
  await db.unsafe(createTables(shard));
}

export async function markResetRequired(db: PostgresDB, shard: ShardID) {
  const schema = cdcSchema(shard);
  await db`
  UPDATE ${db(schema)}."replicationConfig"
    SET "resetRequired" = true`;
}

export async function ensureReplicationConfig(
  lc: LogContext,
  db: PostgresDB,
  config: ReplicationConfig,
  shard: ShardID,
  autoReset: boolean,
) {
  // Restrict the fields of the supplied `config`.
  const {publications, replicaVersion} = config;
  const replicaConfig = {publications, replicaVersion};
  const replicationState: Partial<ReplicationState> = {
    lastWatermark: replicaVersion,
  };
  const schema = cdcSchema(shard);

  await db.begin(async tx => {
    const stmts: PendingQuery<Row[]>[] = [];
    const results = await tx<
      {
        replicaVersion: string;
        publications: string[];
        resetRequired: boolean | null;
      }[]
    >`
    SELECT "replicaVersion", "publications", "resetRequired" 
      FROM ${tx(schema)}."replicationConfig"`;

    if (results.length) {
      const {replicaVersion, publications} = results[0];
      if (
        replicaVersion !== replicaConfig.replicaVersion ||
        !equals(new Set(publications), new Set(replicaConfig.publications))
      ) {
        lc.info?.(
          `Data in cdc tables @${replicaVersion} is incompatible ` +
            `with replica @${replicaConfig.replicaVersion}. Clearing tables.`,
        );
        stmts.push(
          tx`TRUNCATE TABLE ${tx(schema)}."changeLog"`,
          tx`TRUNCATE TABLE ${tx(schema)}."replicationConfig"`,
          tx`TRUNCATE TABLE ${tx(schema)}."replicationState"`,
        );
      }
    }
    // Initialize (or re-initialize TRUNCATED) tables
    if (results.length === 0 || stmts.length > 0) {
      stmts.push(
        tx`INSERT INTO ${tx(schema)}."replicationConfig" ${tx(replicaConfig)}`,
        tx`INSERT INTO ${tx(schema)}."replicationState" 
           ${tx(replicationState)}`,
      );
      return Promise.all(stmts);
    }

    const {resetRequired} = results[0];
    if (resetRequired) {
      if (autoReset) {
        throw new AutoResetSignal('reset required by replication stream');
      }
      lc.error?.(
        '\n\n\n' +
          'Reset required but --auto-reset is not enabled.\n' +
          'This can happen for upstream databases that do not support event triggers.\n' +
          'To correct this, see https://zero.rocicorp.dev/docs/connecting-to-postgres#schema-changes' +
          '\n\n\n',
      );
    }

    return [];
  });
}

export class AutoResetSignal extends AbortError {
  readonly name = 'AutoResetSignal';
}
