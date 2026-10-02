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

interface PurgeLock {
  release(): Promise<void>;
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
  purgeLock?: PurgeLock,
  setTimeoutFn: typeof setTimeout = setTimeout,
  pgChangeLogEnabled = true,
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
        // Release any purge lock held by the caller; the changeLog is
        // incompatible with the replica and needs to be truncated.
        void purgeLock?.release();

        // Note: The order of TRUNCATE matters. The replicationState table is
        //       truncated before the changeLog table, in order to acquire
        //       (exclusive) locks on the tables in the same order that the
        //       storer.ts acquires locks when writing the changeLog, namely:
        //
        // 1. SELECT ... FROM replicationState FOR UPDATE;
        // 2. INSERT INTO changeLog ...;
        //    ...
        // n. UPDATE replicationState ...;
        needsTruncate = true;
        stmts.push(
          sql`TRUNCATE TABLE ${sql(schema)}."replicationState"`,
          ...(pgChangeLogEnabled
            ? [sql`TRUNCATE TABLE ${sql(schema)}."changeLog"`]
            : []),
          sql`TRUNCATE TABLE ${sql(schema)}."replicationConfig"`,
          sql`TRUNCATE TABLE ${sql(schema)}."tableMetadata"`,
          sql`TRUNCATE TABLE ${sql(schema)}."backfilling"`,
        );
      }
    }
    // Initialize (or re-initialize TRUNCATED) tables
    if (results.length === 0 || needsTruncate) {
      // When enabled, the PG storer uses the earliest changeLog entry as the
      // safe watermark from which subscribers can be resumed. These initial
      // entries ensure that subscribers can start from a freshly synced
      // replica, even if new changes have been replicated and not purged.
      //
      // TODO: Replace this with an explicit `firstWatermark` column in the
      //       change db.
      const watermark = replicaConfig.replicaVersion;
      const initialTx: FullChangeLogEntry[] = pgChangeLogEnabled
        ? [
            {watermark, pos: 0, change: {tag: 'begin'}},
            {watermark, pos: 1, change: {tag: 'commit'}},
          ]
        : [];

      stmts.push(
        sql`INSERT INTO ${sql(schema)}."replicationConfig" ${sql(replicaConfig)}`,
        sql`INSERT INTO ${sql(schema)}."replicationState"  ${sql(replicationState)} 
              ON CONFLICT (lock) DO UPDATE SET ${sql(replicationState)}`,
        ...initialTx.map(
          change => sql`INSERT INTO ${sql(schema)}."changeLog" ${sql(change)}`,
        ),
      );

      if (needsTruncate) {
        return runStatementsAndTerminateLockHolders(
          lc,
          db,
          shard,
          'ensureReplicationConfig',
          stmts,
          setTimeoutFn,
        );
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

/**
 * Executes `stmts` that require ACCESS EXCLUSIVE locks (e.g. `TRUNCATE`),
 * which may be blocked by old storer catchup reads. Races against a
 * timeout that terminates the blocking backends if the `stmts` take too
 * long. The check is repeated until the statements complete, since the
 * `stmts` may not yet be waiting on a lock when the timer fires.
 */
async function runStatementsAndTerminateLockHolders(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardID,
  name: string,
  stmts: PendingQuery<Row[]>[],
  setTimeoutFn: typeof setTimeout,
) {
  let done = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  const scheduleTerminate = () => {
    timer = setTimeoutFn(async () => {
      lc.info?.(`${name} blocked, terminating lock holders`);
      try {
        await terminateChangeDBLockHolders(lc, db, shard);
      } catch (e) {
        lc.warn?.('error terminating lock holders', e);
      }
      if (!done) {
        scheduleTerminate();
      }
    }, LOCK_HOLDER_TERMINATE_TIMEOUT_MS);
  };
  scheduleTerminate();
  try {
    return await Promise.all(stmts);
  } finally {
    done = true;
    clearTimeout(timer);
  }
}

export type ChangeLogReinitialization = {
  /**
   * The watermark of the replica from which the change log is re-initialized,
   * i.e. the watermark from which the change stream will resume.
   */
  watermark: string;
  cookies: {
    tableMetadata: readonly {
      schema: string;
      table: string;
      metadata: TableMetadata;
    }[];
    backfilling: readonly {
      schema: string;
      table: string;
      column: string;
      backfill: BackfillID;
    }[];
  };
  owner: string;
  ownerAddress: string;
};

/**
 * Re-initializes the change log from the state of the replica if it does not
 * contain all of the replica's changes, i.e. if its `lastWatermark` is before
 * the replica's watermark. This is the case if the change log is no longer
 * maintained (e.g. after running in high-availability mode), or if its last
 * owner stopped before persisting changes that it had already forwarded
 * (and backed up). Ownership of the change log is taken in the same
 * transaction, in which:
 *
 * * The `replicationState`, `changeLog`, `tableMetadata`, and `backfilling`
 *   tables are truncated. (The `replicationConfig` is kept;
 *   {@link ensureReplicationConfig} handles changes of the replica version
 *   or publications.)
 * * The change log is seeded with an empty transaction at the replica's
 *   watermark, so that subscribers at that watermark can be caught up, while
 *   subscribers at earlier watermarks are rejected as being too old.
 * * The `tableMetadata` and `backfilling` cookies are set from the replica.
 *
 * Returns `'maintained'` without modifying anything if the change log's
 * `lastWatermark` is at or past the replica's watermark, i.e. the change log
 * contains all of the replica's changes. Because a replication slot is only
 * acked past a transaction once it has been backed up, this means that the
 * change log contains every transaction up to the position of the replica's
 * replication slot, and it can be taken over (and resumed from its head).
 *
 * This is called when the change log's head is behind the position of the
 * replication slot (see `PgChangeLogPurgeLocker`). That position cannot be
 * compared with the watermarks of transactions to determine whether the
 * change log is maintained, as it is also advanced past positions of changes
 * outside of the publication.
 *
 * @param purgeLock A purge lock held by the caller, if any, which is released
 *     before the tables are truncated.
 */
export function reinitializeChangeLog(
  lc: LogContext,
  db: PostgresDB,
  shard: ShardID,
  {watermark, cookies, owner, ownerAddress}: ChangeLogReinitialization,
  purgeLock?: PurgeLock,
  setTimeoutFn: typeof setTimeout = setTimeout,
): Promise<'reinitialized' | 'maintained'> {
  const schema = cdcSchema(shard);
  return runTx(db, async sql => {
    // Lock the replicationState first, which is the order in which the
    // storer acquires locks when writing the changeLog.
    const state = await sql<{lastWatermark: string}[]>`
      SELECT "lastWatermark" FROM ${sql(schema)}."replicationState" FOR UPDATE`;
    const lastWatermark = state[0]?.lastWatermark;
    if (lastWatermark !== undefined && lastWatermark >= watermark) {
      lc.info?.(
        `changeLog@${lastWatermark} contains the replica's changes (@${watermark})`,
      );
      return 'maintained';
    }
    lc.info?.(
      `re-initializing changeLog@${lastWatermark} from the replica @${watermark}`,
    );
    const replicationState: ReplicationState = {
      lastWatermark: watermark,
      owner,
      ownerAddress,
    };
    const initialTx: FullChangeLogEntry[] = [
      {watermark, pos: 0, change: {tag: 'begin'}},
      {watermark, pos: 1, change: {tag: 'commit'}},
    ];
    // The caller's purge lock would block the TRUNCATE. Releasing it is safe:
    // a concurrent purge checks the ownership after deleting, which blocks
    // on the replicationState lock held by this transaction.
    await purgeLock?.release();
    await runStatementsAndTerminateLockHolders(
      lc,
      db,
      shard,
      'reinitializeChangeLog',
      [
        sql`TRUNCATE TABLE ${sql(schema)}."replicationState"`,
        sql`TRUNCATE TABLE ${sql(schema)}."changeLog"`,
        sql`TRUNCATE TABLE ${sql(schema)}."tableMetadata"`,
        sql`TRUNCATE TABLE ${sql(schema)}."backfilling"`,
      ],
      setTimeoutFn,
    );
    await sql`INSERT INTO ${sql(schema)}."replicationState" ${sql(replicationState)}`;
    for (const entry of initialTx) {
      await sql`INSERT INTO ${sql(schema)}."changeLog" ${sql(entry)}`;
    }
    for (const row of cookies.tableMetadata) {
      await sql`INSERT INTO ${sql(schema)}."tableMetadata" ${sql({...row})}`;
    }
    for (const row of cookies.backfilling) {
      await sql`INSERT INTO ${sql(schema)}."backfilling" ${sql({...row})}`;
    }
    return 'reinitialized';
  });
}

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
