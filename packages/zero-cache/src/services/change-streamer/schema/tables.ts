import type {LogContext} from '@rocicorp/logger';
import {ident} from 'pg-format';
import type postgres from 'postgres';
import {type PendingQuery, type Row} from 'postgres';
import {AbortError} from '../../../../../shared/src/abort-error.ts';
import {equals} from '../../../../../shared/src/set-utils.ts';
import {runTx} from '../../../db/run-transaction.ts';
import {type PostgresDB} from '../../../types/pg.ts';
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

type FullChangeLogEntry = ChangeLogEntry & {pos: number};

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
  subscriptionState: Pick<
    SubscriptionState,
    'publications' | 'replicaVersion' | 'watermark'
  >,
  shard: ShardID,
  autoReset: boolean,
  setTimeoutFn: typeof setTimeout = setTimeout,
) {
  const {publications, replicaVersion, watermark} = subscriptionState;
  const replicaConfig = {publications, replicaVersion};
  const replicationState: ReplicationState = {
    lastWatermark: replicaVersion,
    owner: null,
    ownerAddress: null,
  };
  const schema = cdcSchema(shard);

  await runTx(db, async sql => {
    const stmts: PendingQuery<Row[]>[] = [];
    let needsTruncate = false;
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
        needsTruncate = true;
        stmts.push(
          sql`TRUNCATE TABLE ${sql(schema)}."changeLog"`,
          sql`TRUNCATE TABLE ${sql(schema)}."replicationConfig"`,
          sql`TRUNCATE TABLE ${sql(schema)}."tableMetadata"`,
          sql`TRUNCATE TABLE ${sql(schema)}."backfilling"`,
        );
      }
    }
    // Initialize (or re-initialize TRUNCATED) tables
    if (results.length === 0 || needsTruncate) {
      // The storer uses the earliest changeLog entry as the safe watermark
      // from which subscribers can be resumed. These initial entries ensure
      // that subscribers can start from a freshly synced replica, even if
      // new changes have been replicated and not purged from the changeLog.
      //
      // TODO: Replace this with an explicit `firstWatermark` column in the
      //       change db.
      const watermark = replicaConfig.replicaVersion;
      const initialTx: FullChangeLogEntry[] = [
        {watermark, pos: 0, change: {tag: 'begin'}},
        {watermark, pos: 1, change: {tag: 'commit'}},
      ];

      stmts.push(
        sql`INSERT INTO ${sql(schema)}."replicationConfig" ${sql(replicaConfig)}`,
        sql`INSERT INTO ${sql(schema)}."replicationState"  ${sql(replicationState)} 
              ON CONFLICT (lock) DO UPDATE SET ${sql(replicationState)}`,
        ...initialTx.map(
          change => sql`INSERT INTO ${sql(schema)}."changeLog" ${sql(change)}`,
        ),
      );

      if (needsTruncate) {
        // The TRUNCATE statements require ACCESS EXCLUSIVE locks, which may
        // be blocked by old storer catchup reads. Race against a timeout
        // that terminates the blocking backends if the TRUNCATE takes too
        // long.
        const timer = setTimeoutFn(async () => {
          lc.info?.(
            'ensureReplicationConfig blocked, terminating lock holders',
          );
          await terminateChangeDBLockHolders(lc, db, shard);
        }, LOCK_HOLDER_TERMINATE_TIMEOUT_MS);
        try {
          return await Promise.all(stmts);
        } finally {
          clearTimeout(timer);
        }
      }
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

// The time to wait for a TRUNCATE in ensureReplicationConfig before
// terminating blocking backends via terminateChangeDBLockHolders.
const LOCK_HOLDER_TERMINATE_TIMEOUT_MS = 5_000;

export const CHANGE_STREAMER_APP_NAME = 'zero-change-streamer';

export class AutoResetSignal extends AbortError {
  readonly name = 'AutoResetSignal';
}

/**
 * Terminates zero-cache backends that are blocking the current backend
 * from acquiring locks on CDC tables (e.g., during TRUNCATE).
 *
 * This is used during change-DB takeover when the new replication-manager's
 * `ensureReplicationConfig` needs to TRUNCATE tables, but the old
 * replication-manager's storer is still reading from them (e.g., large
 * catchup cursors).
 *
 * The function:
 * 1. Finds backends waiting for a lock on a TRUNCATE in {schema}
 * 2. Uses `pg_blocking_pids()` to identify which backends are blocking them
 * 3. Terminates blocking backends that have `application_name = 'zero-change-streamer'`
 *
 * Must be called on a **separate connection** from the one that is blocked,
 * since the blocked connection is inside a pending transaction.
 */
export async function terminateChangeDBLockHolders(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardID,
) {
  const schema = cdcSchema(shard);

  // Step 1: Find backends that are blocked waiting for a lock,
  // whose query involves a TRUNCATE on this shard's CDC schema.
  const blocked = await db<{pid: number}[]>`
    SELECT pid FROM pg_stat_activity
      WHERE wait_event_type = 'Lock'
        AND application_name = ${CHANGE_STREAMER_APP_NAME}
        AND query LIKE ${'%TRUNCATE%' + schema + '%'}`;

  if (blocked.length === 0) {
    lc.info?.('no blocked TRUNCATE backends found');
    return;
  }

  const blockedPids = blocked.map(r => r.pid);
  lc.info?.(`found blocked TRUNCATE backends: ${JSON.stringify(blockedPids)}`);

  // Step 2: For each blocked backend, find and terminate its blockers
  // that are zero-change-streamer connections.
  const terminated = await db<
    {pid: number; applicationName: string; query: string; terminated: boolean}[]
  >`
    SELECT pid, application_name as "applicationName", query,
           pg_terminate_backend(pid) as terminated
      FROM pg_stat_activity
      WHERE pid = ANY(
        SELECT unnest(pg_blocking_pids(blocked.pid))
          FROM unnest(${blockedPids}::int[]) AS blocked(pid)
      )
      AND application_name = ${CHANGE_STREAMER_APP_NAME}
      AND pid != ALL(${blockedPids}::int[])`;

  if (terminated.length === 0) {
    lc.info?.(`no ${CHANGE_STREAMER_APP_NAME} blockers found to terminate`);
  } else {
    for (const {pid, applicationName, query, terminated: ok} of terminated) {
      lc.info?.(
        `terminated blocking backend pid=${pid} app=${applicationName} ok=${ok} query=${query.slice(0, 200)}`,
      );
    }
  }
}
