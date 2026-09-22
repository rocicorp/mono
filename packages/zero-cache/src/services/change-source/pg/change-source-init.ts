import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../../shared/src/asserts.ts';
import {deepEqual} from '../../../../../shared/src/json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {promiseVoid} from '../../../../../shared/src/resolved-promises.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
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
import {
  initialSync,
  type InitialSyncOptions,
  type ReplicaOptions,
  type ServerContext,
} from './initial-sync.ts';
import {toBigInt, type LSN} from './lsn.ts';
import {
  claimSlotForResumption,
  createReplicaAndSlot,
  type ReplicationSlotResult,
} from './replication-slots.ts';
import {ensureShardSchema} from './schema/init.ts';
import * as ReplicaStage from './schema/replica-stage-enum.ts';
import {InitialSync, Replicate} from './schema/replica-stage-enum.ts';
import {
  dropShard,
  getActiveReplicas,
  getReplicaAtVersion,
  getRestoreCandidates,
  initRestoreReplica,
  internalPublicationPrefix,
  type ReplicaState,
} from './schema/shard.ts';

interface PurgeLock {
  release(): Promise<void>;
}

export type InitializeOptions = ReplicaOptions & {
  // Create a new slot for the replica rather than taking over
  // an existing one (i.e. high-availability mode).
  slotPerReplica: boolean | undefined;

  inactiveReplicaGracePeriodMs: number;
};

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
  {
    epoch,
    slotPerReplica,
    backupV5,
    inactiveReplicaGracePeriodMs,
  }: InitializeOptions = {
    epoch: 0,
    slotPerReplica: false,
    backupV5: true,
    inactiveReplicaGracePeriodMs: DEFAULT_INACTIVE_REPLICA_GRACE_PERIOD_MS,
  },
  purgeLock?: PurgeLock | null,
  streamInboundTimeoutMs?: number | undefined,
): Promise<InitializeResult> {
  const db = await connectPgClient(lc, upstreamURI, 'change-source-init');
  try {
    await ensureShardSchema(
      lc,
      db,
      shard,
      syncOptions.installPartialIndexTriggers,
    );

    if (slotPerReplica) {
      // Sanity check: This should be disabled via pgChangeLogEnabled=false.
      assert(purgeLock === null, `There should be no purgeLock for RMv2`);
    }

    const restoredReplica = slotPerReplica
      ? await forkOrResumeReplica(
          lc,
          db,
          shard,
          epoch,
          syncOptions.replicationSlotFailover ?? false,
          replicaDbFile,
          restoreOptions,
          inactiveReplicaGracePeriodMs,
        )
      : await selectAndRestoreReplica(
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
          {epoch, backupV5},
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

// RMv1: Selects a replica to restore from and returns it, with the
//       intention of taking over the slot (in the ChangeSource).
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

// RMv2: Restores from an active replica and creates a new replica / slot
// to continue replication (i.e. "fork"). If only orphaned replicas remain,
// claims a slot, restores the backup, and "resumes" replication for that
// slot.
async function forkOrResumeReplica(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  epoch: number,
  slotFailover: boolean,
  replicaFile: string,
  {litestream, constraints}: RestoreOptions,
  gracePeriodMs: number,
): Promise<ReplicaState | undefined> {
  const result = await getSourceAndDestinationReplicas(
    lc,
    sql,
    shard,
    epoch,
    slotFailover,
    gracePeriodMs,
  );
  if (!result) {
    return undefined; // can't restore, must initial-sync
  }
  const {restoreFrom, replicateTo} = result;

  if (litestream?.backupURL) {
    const {backupURL: backupBaseURL} = litestream;
    const {slot, backupPath, confirmedFlushLsn} = restoreFrom;
    const backupURL = new URL(backupPath ?? '', backupBaseURL).toString();
    lc.info?.(
      `restoring replica from ${backupURL} (${slot}@${confirmedFlushLsn})`,
      {restoreFrom, replicateTo},
    );
    await restoreReplica(
      lc,
      {...litestream, backupURL}, // includes the replica's backup sub-path
      replicaFile,
      constraints,
    );
  }
  return replicateTo;
}

const REPLICA_POLL_INTERVAL_MS = 5_000;
const DEFAULT_INACTIVE_REPLICA_GRACE_PERIOD_MS = 20_000;

// Exported for testing.
export async function getSourceAndDestinationReplicas(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  epoch: number,
  slotFailover: boolean,
  gracePeriodMs: number,
  pollIntervalMs = REPLICA_POLL_INTERVAL_MS,
): Promise<{restoreFrom: ReplicaState; replicateTo: ReplicaState} | undefined> {
  const inactiveSince = new Map<string, number>(); // tracks replica inactivity
  let destination: ReplicationSlotResult<void> | undefined;

  try {
    for (let i = 0; ; i++) {
      if (i > 0) {
        await sleep(pollIntervalMs);
      }
      const replicas = await getRestoreCandidates(lc, sql, shard, epoch);
      if (replicas.length === 0) {
        lc.info?.(`no suitable replicas to restore from`, {replicas});
        destination?.initialSession.destroy();
        return undefined;
      }
      for (const replica of replicas) {
        // Track inactivity to resume replicas after a grace period.
        if (replica.active) {
          inactiveSince.delete(replica.id);
        } else if (!inactiveSince.has(replica.id)) {
          inactiveSince.set(replica.id, Date.now());
        }

        // Note: Only `active` InitialSync replicas are returned from
        // getRestoreCandidates().
        if (replica.stage === InitialSync) {
          // Log periodically; initial-sync can be long
          if (i % 12 === 0) {
            lc.info?.(`waiting for initial sync of ${replica.id}`, {replica});
          }
          break;
        }

        if (replica.stage === Replicate) {
          const {active, confirmedFlushLsn} = replica;
          if (active) {
            // Create a replication slot to fork the active replica.
            destination ??= await createReplicaAndSlot(
              lc,
              sql,
              'fork-replica-session',
              shard,
              epoch,
              Date.now().toString(), // replicaID
              slotFailover,
              {
                backupPath: null, // set only after the backup has been confirmed
                backupV5: true, // RMv2 requires backupV5
              },
              () => promiseVoid,
              ReplicaStage.Restore,
            );
            if (
              toBigInt(confirmedFlushLsn) <
              toBigInt(destination.slot.consistent_point)
            ) {
              lc.info?.(
                `waiting for ${replica.id}@${confirmedFlushLsn} to reach ${destination.slot.slot_name}@${destination.slot.consistent_point}`,
                {replica},
              );
              break;
            }
          }

          // If the replica is past the destination LSN, fork it, regardless of
          // whether its slot is still active. Forking is preferable to resuming,
          // as resuming removes the replica from being a candidate for
          // subsequent forks, and carries the risk of stealing the slot from a
          // task attempting to reconnect.
          if (
            destination &&
            toBigInt(confirmedFlushLsn) >=
              toBigInt(destination.slot.consistent_point)
          ) {
            lc.info?.(`forking replica ${replica.id}@${replica.slot}`, {
              replica,
            });
            const replicateTo = must(
              await initRestoreReplica(sql, shard, {
                sourceID: replica.id,
                destID: destination.replica.id,
              }),
              `replica ${destination.replica.id} was deleted`,
            );
            return {restoreFrom: replica, replicateTo};
          }
        }

        if (replica.backupPath && !replica.active) {
          // An inactive replica that has a backupPath may be orphaned, or it
          // may be an active task that was temporarily disconnected and
          // attempting to reestablish a session.
          //
          // Resume the replica after a grace period to avoid stealing the slot
          // from an active task that was temporarily disconnected. Resumption
          // is a last resort only taken in the absence of alternatives.
          const now = Date.now();
          const inactiveMs = now - (inactiveSince.get(replica.id) ?? now);
          lc.info?.(
            `replica ${replica.id}@${replica.slot} as been inactive for ${inactiveMs}ms`,
            {replica},
          );
          if (inactiveMs >= gracePeriodMs) {
            const reserved = await claimSlotForResumption(
              lc,
              sql,
              shard,
              'resume-replica',
              replica.slot,
            );
            if (!reserved) {
              lc.warn?.(
                `unable to resume replica ${replica.id}@${replica.slot}`,
              );
            } else {
              lc.info?.(`resuming replica ${replica.id}@${replica.slot}`);
              // If a new replication slot was created in anticipation of forking,
              // cancel it
              destination?.initialSession.destroy();
              return {
                restoreFrom: replica,
                replicateTo: reserved.replica,
              };
            }
          }
        }
      }
    }
  } catch (e) {
    destination?.initialSession.destroy();
    throw e;
  }
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
