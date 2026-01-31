import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import type postgres from 'postgres';
import {type PendingQuery, type Row} from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import {disableStatementTimeout, type PostgresDB} from '../../../types/pg.ts';
import {cdcSchema, type ShardID} from '../../../types/shards.ts';
import type {
  BackfillID,
  Change,
  TableMetadata,
} from '../../change-source/protocol/current/data.ts';
import type {SubscriptionState} from '../../replicator/schema/replication-state.ts';

// For readability in the sql statements.
function schema(shard: ShardID) {
  return ident(cdcSchema(shard));
}

export const PG_SCHEMA = 'cdc';

function createSchema(shard: ShardID) {
  return /*sql*/ `CREATE SCHEMA IF NOT EXISTS ${schema(shard)};`;
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
  return /*sql*/ `
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
  ownerAddress: string | null;
};

export function createReplicationStateTable(shard: ShardID) {
  return /*sql*/ `
  CREATE TABLE ${schema(shard)}."replicationState" (
    "lastWatermark" TEXT NOT NULL,
    "owner" TEXT,
    "ownerAddress" TEXT,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;
}

export async function discoverChangeStreamerAddress(
  shard: ShardID,
  sql: PostgresDB,
): Promise<string | null> {
  const result = await sql<{ownerAddress: string | null}[]> /*sql*/ `
    SELECT "ownerAddress" FROM ${sql(cdcSchema(shard))}."replicationState"`;
  return result[0].ownerAddress;
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
  return /*sql*/ `
  CREATE TABLE ${schema(shard)}."replicationConfig" (
    "replicaVersion" TEXT NOT NULL,
    "publications" TEXT[] NOT NULL,
    "resetRequired" BOOL,
    "lock" INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
  );
`;
}

export function createBackfillTables(shard: ShardID) {
  return /*sql*/ `
  CREATE TABLE ${schema(shard)}."tableMetadata" (
    "schema" TEXT NOT NULL,
    "table" TEXT NOT NULL,
    "metadata" JSONB NOT NULL,
    PRIMARY KEY("schema", "table")
  );

  CREATE TABLE ${schema(shard)}."backfilling" (
    "schema" TEXT NOT NULL,
    "table" TEXT NOT NULL,
    "column" TEXT NOT NULL,
    "backfill" JSONB NOT NULL,
    PRIMARY KEY("schema", "table", "column")
  );
  `;
}

export type TableMetadataRow = {
  schema: string;
  table: string;
  metadata: TableMetadata;
};

export type BackfillingColumn = {
  schema: string;
  table: string;
  column: string;
  backfill: BackfillID;
};

function createTables(shard: ShardID) {
  return (
    createSchema(shard) +
    createChangeLogTable(shard) +
    createReplicationStateTable(shard) +
    createReplicationConfigTable(shard) +
    createBackfillTables(shard)
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

export async function markResetRequired(sql: PostgresDB, shard: ShardID) {
  const schema = cdcSchema(shard);
  await sql`
  UPDATE ${sql(schema)}."replicationConfig"
    SET "resetRequired" = true`;
}

export async function ensureReplicationConfig(
  lc: LogContext,
  db: PostgresDB,
  subscriptionState: SubscriptionState,
  shard: ShardID,
  autoReset: boolean,
) {
  const {publications, replicaVersion, watermark} = subscriptionState;
  const replicaConfig = {publications, replicaVersion};
  const replicationState: ReplicationState = {
    lastWatermark: replicaVersion,
    owner: null,
    ownerAddress: null,
  };
  const schema = cdcSchema(shard);

  await db.begin(async sql => {
    disableStatementTimeout(sql);

    const stmts: PendingQuery<Row[]>[] = [];
    const results = await sql<
      {
        replicaVersion: string;
        publications: string[];
        resetRequired: boolean | null;
      }[]
    > /*sql*/ `
    SELECT "replicaVersion", "publications", "resetRequired" 
      FROM ${sql(schema)}."replicationConfig"`;

    if (results.length) {
      const {replicaVersion, publications} = results[0];
      if (
        replicaVersion !== replicaConfig.replicaVersion ||
        !equals(new Set(publications), new Set(replicaConfig.publications))
      ) {
        if (replicaConfig.replicaVersion !== watermark) {
          throw new AutoResetSignal(
            `Cannot reset change db@${replicaVersion} to ` +
              `service replica@${replicaConfig.replicaVersion} ` +
              `from watermark ${watermark}`,
          );
        }
        lc.info?.(
          `Data in cdc tables @${replicaVersion} is incompatible ` +
            `with replica @${replicaConfig.replicaVersion}. Clearing tables.`,
        );
        // Note: The replicationState table is explicitly not TRUNCATE'd.
        //       Any existing row must be overwritten by an UPDATE or
        //       INSERT ... ON CONFLICT clause in order to correctly abort
        //       any pending transaction by a concurrently running
        //       change-streamer. Deleting the existing row and creating
        //       a new one, on the other hand, may not properly trigger the
        //       SERIALIZATION failure necessary to abort the pending tx.
        stmts.push(
          sql`TRUNCATE TABLE ${sql(schema)}."changeLog"`,
          sql`TRUNCATE TABLE ${sql(schema)}."replicationConfig"`,
          sql`TRUNCATE TABLE ${sql(schema)}."tableMetadata"`,
          sql`TRUNCATE TABLE ${sql(schema)}."backfilling"`,
        );
      }
    }
    // Initialize (or re-initialize TRUNCATED) tables
    if (results.length === 0 || stmts.length > 0) {
      stmts.push(
        sql`INSERT INTO ${sql(schema)}."replicationConfig" ${sql(replicaConfig)}`,
        sql`INSERT INTO ${sql(schema)}."replicationState"  ${sql(replicationState)} 
              ON CONFLICT (lock) DO UPDATE SET ${sql(replicationState)}`,
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
