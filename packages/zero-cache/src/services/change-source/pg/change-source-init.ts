import type {LogContext} from '@rocicorp/logger';
import {deepEqual} from '../../../../../shared/src/json.ts';
import {Database} from '../../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../../db/statements.ts';
import {connectPgClient, type PostgresDB} from '../../../types/pg.ts';
import {type ShardConfig, type ShardID} from '../../../types/shards.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {
  getSubscriptionStateAndContext,
  type SubscriptionStateAndContext,
} from '../../replicator/schema/replication-state.ts';
import {
  restoreReplica,
  type InitializeResult,
  type RestoreOptions,
} from '../common/replica-restore.ts';
import {initReplica} from '../common/replica-schema.ts';
import {PostgresChangeSource} from './change-source.ts';
import type {BackfillOptions} from './change-source.ts';
import {
  initialSync,
  type InitialSyncOptions,
  type ReplicaOptions,
  type ServerContext,
} from './initial-sync.ts';
import {type LSN} from './lsn.ts';
import {ensureShardSchema} from './schema/init.ts';
import {
  dropShard,
  getActiveReplicas,
  getReplicaAtVersion,
  internalPublicationPrefix,
  type ReplicaState,
} from './schema/shard.ts';

interface PurgeLock {
  release(): Promise<void>;
}

/**
 * Initializes a Postgres change source, including the initial sync of the
 * replica, before streaming changes from the corresponding logical replication
 * stream.
 */
export async function initializePostgresChangeSource(
  lc: LogContext,
  upstreamURI: string,
  shard: ShardConfig,
  replicaDbFile: string,
  syncOptions: InitialSyncOptions,
  context: ServerContext,
  lagReportIntervalMs = 0,
  restoreOptions: RestoreOptions = {},
  {backupV5}: ReplicaOptions = {backupV5: true},
  purgeLock?: PurgeLock | null,
  streamInboundTimeoutMs?: number | undefined,
  backfillOptions: BackfillOptions = {},
): Promise<InitializeResult> {
  const db = await connectPgClient(lc, upstreamURI, 'change-source-init');
  try {
    await ensureShardSchema(
      lc,
      db,
      shard,
      syncOptions.installPartialIndexTriggers,
    );

    const restoredReplica = await selectAndRestoreReplica(
      lc,
      db,
      shard,
      replicaDbFile,
      restoreOptions,
    );

    let initialSyncedReplica: ReplicaState | undefined;
    await initReplica(
      lc,
      `replica-${shard.appID}-${shard.shardNum}`,
      replicaDbFile,
      async (log, tx) => {
        // In RMv1, the purge lock on the change-db must be released before performing
        // initial sync; if the change-db and upstream are the same db, a lock-holding
        // transaction will prevent a replication slot from being created. This awkward
        // dependency can go away with RMv2.
        void purgeLock?.release();
        initialSyncedReplica = await initialSync(
          log,
          shard,
          tx,
          upstreamURI,
          syncOptions,
          context,
          {backupV5},
        );
      },
    );

    const replica = new Database(lc, replicaDbFile);
    const subscriptionState = getSubscriptionStateAndContext(
      new StatementRunner(replica),
    );
    replica.close();

    // Check that upstream is properly setup, and throw an AutoReset to re-run
    // initial sync if not.
    const {upstreamReplica, pgVersion} = await checkAndUpdateUpstream(
      lc,
      db,
      shard,
      subscriptionState,
      (initialSyncedReplica ?? restoredReplica)?.id,
    );

    const backupPath = initialSyncedReplica
      ? // If initial sync was performed, use that initial backupPath.
        initialSyncedReplica.backupPath
      : // Otherwise, use a new, unique path when backing up with litestream v5. This will be
        // recorded in the replicas table by the PostgresChangeSource.
        backupV5
        ? String(Date.now())
        : (restoredReplica?.backupPath ?? null);

    const changeSource = new PostgresChangeSource(
      lc,
      upstreamURI,
      shard,
      upstreamReplica,
      pgVersion,
      {backupPath, backupV5},
      context,
      lagReportIntervalMs,
      syncOptions.textCopy,
      streamInboundTimeoutMs,
      backfillOptions,
    );

    const destinationBackupURL =
      backupPath && restoreOptions.litestream?.backupURL
        ? new URL(backupPath, restoreOptions.litestream.backupURL).toString()
        : // For legacy RMv1 replicas (on litestream-v3), backup to the same location
          restoreOptions.litestream?.backupURL;

    return {
      subscriptionState,
      changeSource,
      destinationBackupURL,
      // The replica this change stream belongs to. It is part of the identity
      // the SQLite change log records, because a generation (i.e.
      // `replicaVersion`) is shared by every sibling of a forked replica and so
      // cannot distinguish two siblings' logs.
      replicaID: upstreamReplica.id,
      waitForBackupBeforeServing:
        // Wait for the first backup if there was an initial sync,
        initialSyncedReplica !== undefined ||
        // or if the destination differs from where it was restored
        // (i.e. backupV5).
        backupPath !== (restoredReplica?.backupPath ?? null),
    };
  } finally {
    await db.end();
  }
}

async function selectAndRestoreReplica(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  replicaFile: string,
  {litestream, constraints}: RestoreOptions,
): Promise<ReplicaState | undefined> {
  const replicas = (await getActiveReplicas(lc, sql, shard)).filter(
    // filter to the generation specified by the constraints, if present
    ({generation}) =>
      generation === (constraints?.replicaVersion ?? generation),
  );
  if (replicas.length === 0) {
    lc.info?.(`no suitable replicas to restore from`, {replicas});
    return undefined;
  }
  const [replica] = replicas;

  if (litestream?.backupURL) {
    const {backupURL: backupBaseURL} = litestream;
    const {slot, backupPath, confirmedFlushLsn} = replica;
    const backupURL = new URL(backupPath ?? '', backupBaseURL).toString();
    lc.info?.(
      `restoring replica from ${backupURL} (${slot}@${confirmedFlushLsn})`,
      {replicas},
    );
    await restoreReplica(
      lc,
      {...litestream, backupURL}, // includes the replica's backup sub-path
      replicaFile,
      constraints,
    );
  }
  return replica;
}

async function checkAndUpdateUpstream(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardConfig,
  {
    replicaVersion,
    publications: subscribed,
    initialSyncContext,
  }: SubscriptionStateAndContext,
  replicaID: string | undefined,
) {
  const upstreamReplica = await getReplicaAtVersion(
    lc,
    sql,
    shard,
    replicaVersion,
    replicaID,
    initialSyncContext,
  );
  if (!upstreamReplica) {
    throw new AutoResetSignal(
      `No replication slot for replica at version ${replicaVersion} and id ${replicaID}}`,
    );
  }

  // Verify that the publications match what is being replicated.
  const requested = shard.publications.toSorted();
  const replicated = upstreamReplica.publications
    .filter(p => !p.startsWith(internalPublicationPrefix(shard)))
    .sort();
  if (!deepEqual(requested, replicated)) {
    lc.warn?.(`Dropping shard to change publications to: [${requested}]`);
    await sql.unsafe(dropShard(shard.appID, shard.shardNum));
    throw new AutoResetSignal(
      `Requested publications [${requested}] do not match configured ` +
        `publications: [${replicated}]`,
    );
  }

  // Sanity check: The subscription state on the replica should have the
  // same publications. This should be guaranteed by the equivalence of the
  // replicaVersion, but it doesn't hurt to verify.
  if (!deepEqual(upstreamReplica.publications, subscribed)) {
    throw new AutoResetSignal(
      `Upstream publications [${upstreamReplica.publications}] do not ` +
        `match subscribed publications [${subscribed}]`,
    );
  }

  // Verify that the publications exist.
  const exists = await sql`
    SELECT pubname FROM pg_publication WHERE pubname IN ${sql(subscribed)};
  `.values();
  if (exists.length !== subscribed.length) {
    throw new AutoResetSignal(
      `Upstream publications [${exists.flat()}] do not contain ` +
        `all subscribed publications [${subscribed}]`,
    );
  }

  const {slot} = upstreamReplica;
  const result = await sql<{restartLSN: LSN | null; walStatus: string | null}[]>
  /*sql*/ `
    SELECT restart_lsn as "restartLSN", wal_status as "walStatus" FROM pg_replication_slots
      WHERE slot_name = ${slot}`;
  if (result.length === 0) {
    throw new AutoResetSignal(`replication slot ${slot} is missing`);
  }
  const [{restartLSN, walStatus}] = result;
  if (restartLSN === null || walStatus === 'lost') {
    throw new AutoResetSignal(
      `replication slot ${slot} has been invalidated for exceeding the max_slot_wal_keep_size`,
    );
  }
  const [{pgVersion}] = await sql<{pgVersion: number}[]> /*sql*/ `
    SELECT current_setting('server_version_num')::int as "pgVersion"`;
  return {upstreamReplica, pgVersion};
}
