import type {Readable} from 'node:stream';
import {
  PG_CONFIGURATION_LIMIT_EXCEEDED,
  PG_INSUFFICIENT_PRIVILEGE,
  PG_LOCK_NOT_AVAILABLE,
} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import {defu} from 'defu';
import postgres, {type Options, type PostgresType} from 'postgres';
import type {JSONObject} from '../../../../../shared/src/bigint-json.ts';
import {must} from '../../../../../shared/src/must.ts';
import {sleep} from '../../../../../shared/src/sleep.ts';
import {runTx} from '../../../db/run-transaction.ts';
import {PG_17} from '../../../types/pg-versions.ts';
import {isPostgresError, type PostgresDB} from '../../../types/pg.ts';
import {upstreamSchema, type ShardID} from '../../../types/shards.ts';
import {orTimeout} from '../../../types/timeout.ts';
import {AutoResetSignal} from '../../change-streamer/schema/tables.ts';
import {
  createReplicationSessionFor,
  endSession,
  extractConnectionConfig,
  keepSlotActiveUntilTakenOver,
} from './logical-replication/stream.ts';
import {toBigInt, toStateVersionString} from './lsn.ts';
import {
  InitialSync,
  Restore,
  type ReplicaStage,
} from './schema/replica-stage-enum.ts';
import {
  createReplica,
  getReplicaState,
  getRestoreCandidates,
  metadataPublicationName,
  replicationSlotExpression,
  replicationSlotPrefix,
  type BackupOptions,
  type ReplicaState,
} from './schema/shard.ts';

// Record returned by `CREATE_REPLICATION_SLOT`
export type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};

export type ReplicationSlotResult<T> = {
  slot: ReplicationSlot;
  capturedSnapshot: T;
  initialSession: Readable;
  replica: ReplicaState;
};

export type ReservedSlot = {
  slot: string;
  reservation: Readable;
  replica: ReplicaState;
};

export type CreateSlotSpec = {
  slotName: string;

  // Note: ignored if pgVersion < PG_17.
  failover?: boolean;

  // Create a temporary slot (i.e. not persisted, and automatically
  // cleaned up when the replication session ends).
  temporary?: boolean;

  // For overriding in tests.
  lockTimeout?: number | undefined;

  // A (non-replication) connection to the same upstream instance, used to
  // log what slot creation was waiting on if it times out.
  diagnosticsDB?: PostgresDB | undefined;
};

// When creating a replication slot, Postgres waits for open transactions
// to complete before reserving a consistent_point (LSN) in the WAL and creating
// a matching transaction snapshot. As such, it can technically take an arbitrary
// amount of time (e.g. DDL operations, table-wide operations, etc.).
//
// However, to detect pathological situations, bound the amount of time that
// the server waits for replication slot creation, so that a continual failure to
// create a replication slot is surfaced by errors / alerts.
export const CREATE_REPLICATION_SLOT_TIMEOUT_MS = 60_000;

// The lock_timeout is set 1s before the client-side orTimeout so that
// Postgres reliably aborts first and tears down the walsender cleanly.
// The client-side timeout remains as a fallback for network-level failures.
const SERVER_LOCK_TIMEOUT_MS = CREATE_REPLICATION_SLOT_TIMEOUT_MS - 1_000;

/** Thrown when the client-side timeout for creating a slot is reached. */
export class SlotCreationTimeoutError extends Error {
  readonly name = 'SlotCreationTimeoutError';
}

// Note: The replication connection does not support the extended query protocol,
//       so all commands must be sent using sql.unsafe(). This is technically safe
//       because all placeholder values are under our control (i.e. "slotName").
export async function createReplicationSlot(
  lc: LogContext,
  session: postgres.Sql,
  {
    slotName,
    failover,
    temporary,
    lockTimeout = SERVER_LOCK_TIMEOUT_MS,
    diagnosticsDB,
  }: CreateSlotSpec,
): Promise<ReplicationSlot> {
  // CREATE_REPLICATION_SLOT can hang indefinitely waiting for long-running
  // transactions to finish: internally it calls SnapBuildWaitSnapshot →
  // XactLockTableWait → LockAcquire on each running XID. statement_timeout
  // does NOT apply to replication commands, but lock_timeout does (it governs
  // the heavyweight lock wait inside LockAcquire). Setting it here causes
  // Postgres to raise ERRCODE_LOCK_NOT_AVAILABLE and cleanly tear down the
  // walsender, rather than relying solely on the client-side orTimeout
  // which can leave an orphaned backend.
  //
  // An orphaned walsender is actively harmful: by this point the replication
  // slot has already been created and is pinning WAL retention and catalog_xmin.
  // Worse, the slot is marked `active` (the walsender PID is still alive), so
  // the existing cleanup code (which drops inactive slots on retry) can't
  // reclaim it. Without lock_timeout the orphan persists until TCP keepalive
  // fires (~2h default) or the blocking transaction finishes.
  await session.unsafe(`SET lock_timeout = ${lockTimeout}`);

  const {pgVersion, pid: walsenderPID} = (
    await session.unsafe<{pgVersion: number; pid: number}[]>(`
      SELECT current_setting('server_version_num') as "pgVersion",
             pg_backend_pid() as "pid";
  `)
  )[0];

  const maybeTemporary = temporary ? 'TEMPORARY' : '';
  const options = failover && pgVersion >= PG_17 ? '(FAILOVER)' : '';
  const createSlot = session.unsafe<ReplicationSlot[]>(/*sql*/ `
    CREATE_REPLICATION_SLOT "${slotName}" ${maybeTemporary} LOGICAL pgoutput ${options}`);

  try {
    const raced = await orTimeout(
      createSlot,
      CREATE_REPLICATION_SLOT_TIMEOUT_MS,
    );
    if (raced === 'timed-out') {
      throw new SlotCreationTimeoutError(
        `Timed out after ${CREATE_REPLICATION_SLOT_TIMEOUT_MS} ms creating replication slot ${slotName}.`,
      );
    }
    const [slot] = raced;
    lc.info?.(`Created replication slot ${slotName}`, slot);
    return slot;
  } catch (e) {
    if (
      diagnosticsDB &&
      (isPostgresError(e, PG_LOCK_NOT_AVAILABLE) ||
        e instanceof SlotCreationTimeoutError)
    ) {
      // After a lock_timeout, the walsender is no longer waiting, but the
      // transactions it was waiting on are likely still running. After the
      // client-side timeout, it may still be waiting on them.
      await logSlotCreationBlockers(lc, diagnosticsDB, walsenderPID, slotName);
    }
    throw e;
  }
}

const SLOT_DIAGNOSTICS_TIMEOUT_MS = 5_000;
const SLOT_DIAGNOSTICS_MAX_TRANSACTIONS = 20;

/**
 * Best-effort logging of what a timed out CREATE_REPLICATION_SLOT was waiting
 * on: the walsender's wait event and blocking pids, and the oldest
 * transactions holding an xid. Slot creation waits for every such transaction
 * on the instance (in all databases), each with its own lock_timeout, so a
 * chain of overlapping transactions can exceed the client-side timeout
 * without the lock_timeout ever firing.
 *
 * Query text is deliberately omitted since it can contain row values.
 *
 * Exported for testing.
 */
export async function logSlotCreationBlockers(
  lc: LogContext,
  sql: PostgresDB,
  walsenderPID: number,
  slotName: string,
) {
  try {
    const result = await orTimeout(
      Promise.all([
        sql<JSONObject[]> /*sql*/ `
          SELECT pid, wait_event_type as "waitEventType", wait_event as "waitEvent",
                 pg_blocking_pids(pid) as "blockingPIDs"
            FROM pg_stat_activity WHERE pid = ${walsenderPID}`,
        sql<JSONObject[]> /*sql*/ `
          SELECT pid, datname, usename, application_name as "applicationName",
                 backend_type as "backendType", state,
                 wait_event_type as "waitEventType", wait_event as "waitEvent",
                 backend_xid::text as xid,
                 xact_start::text as "xactStart",
                 extract(epoch from now() - xact_start)::float8 as "xactAgeSec"
            FROM pg_stat_activity
            WHERE backend_xid IS NOT NULL AND pid <> ${walsenderPID}
            ORDER BY xact_start NULLS LAST
            LIMIT ${SLOT_DIAGNOSTICS_MAX_TRANSACTIONS}`,
      ]),
      SLOT_DIAGNOSTICS_TIMEOUT_MS,
    );
    if (result === 'timed-out') {
      lc.warn?.(`Timed out collecting diagnostics for slot ${slotName}`);
      return;
    }
    const [[walsender], transactions] = result;
    lc.warn?.(`Replication slot ${slotName} creation blocked`, {
      walsender: walsender ?? null,
      transactions,
    });
  } catch (e) {
    lc.warn?.(`Unable to collect diagnostics for slot ${slotName}`, e);
  }
}

/**
 * Whether slot creation failed because it was waiting on transactions on the
 * upstream instance: the server's lock_timeout fired, the client-side timeout
 * was reached, or the replication session was closed while it was waiting.
 * Such failures are expected on busy upstreams, and leave no durable state,
 * so the attempt can simply be retried.
 */
function isSlotCreationBlocked(e: unknown): boolean {
  return (
    isPostgresError(e, PG_LOCK_NOT_AVAILABLE) ||
    e instanceof SlotCreationTimeoutError ||
    (e instanceof Error && (e as {code?: unknown}).code === 'CONNECTION_CLOSED')
  );
}

// Slot creation is retried while it is blocked by upstream transactions, but
// a slot that cannot be created for this long is surfaced as an error (i.e.
// by exiting the process).
const DEFAULT_MAX_SLOT_CREATION_BLOCKED_MS = 30 * 60 * 1000;

// Base delay between retries of blocked slot creation. The actual delay is
// jittered to between 1x and 5x of this value.
const DEFAULT_SLOT_CREATION_RETRY_DELAY_MS = 1_000;

export type CreateReplicaAndSlotOptions = {
  // For overriding in tests.
  lockTimeout?: number | undefined;
  maxBlockedMs?: number | undefined;
  retryDelayMs?: number | undefined;
};

/**
 * Creates a dedicated (non-pooled) session for holding the session-level
 * replication slot management lock. Ending the session releases the lock,
 * which guarantees that the lock never outlives the attempt.
 */
function createLockSessionFor(db: PostgresDB, applicationName: string) {
  return postgres(
    defu(
      {
        max: 1,
        ['idle_timeout']: null,
        ['max_lifetime']: null as unknown as number,
        connection: {['application_name']: applicationName},
      },
      // See createReplicationSessionFor() for why this cast is necessary.
      db.options as unknown as Options<Record<string, PostgresType>>,
    ),
  );
}

/**
 * Replica and slot creation involves several sessions for proper
 * coordination with other replica management logic:
 *
 * * A dedicated session acquires a (session-level) advisory lock for
 *   replica slot management. This is the same lock that cleanup logic
 *   acquires before cleaning up replication slots. The lock is held
 *   outside of a transaction so that the session is not "idle in
 *   transaction" while slot creation waits on upstream transactions,
 *   which some upstreams terminate (e.g. idle_in_transaction_session_timeout).
 * * With the lock held, a new replication slot is created in a
 *   replication session. The API of CREATE_REPLICATION_SLOT is such
 *   that it cannot be done in a transaction, and cannot be followed by
 *   any writes, or else its snapshot (which is needed for initial sync)
 *   would be invalidated.
 * * Once the slot is created, the slot and replica information are recorded
 *   in the `replicas` table before releasing the lock.
 *
 * Slot creation waits for all transactions on the upstream instance that
 * hold an xid, so it can be blocked by long-running transactions. A blocked
 * attempt is abandoned (see {@link CREATE_REPLICATION_SLOT_TIMEOUT_MS}) and
 * retried in-process, as it leaves no durable state: the `replicas` row is
 * only inserted after the slot has been created. Retries continue until the
 * slot is created or until `maxBlockedMs` has elapsed, after which the error
 * is thrown.
 *
 * This locking ensures that:
 * 1. multiple replication managers attempting to create a replication slot
 *    will not use the same name for the replication slot (which is selected
 *    from a pool of reused names).
 * 2. Running replication managers (which use an earlier replica of a lower
 *    rank) will not delete the new slot during their cleanup logic, since
 *    the slot will belong to a replica of a higher rank.
 *
 * When the replication slot is created, `captureSnapshot` callback is first
 * run while the snapshot at the consistent point is available. The callback
 * must capture the snapshot in postgres (e.g. `SET TRANSACTION SNAPSHOT`)
 * in order to use it. Once the callback completes, a placeholder replication
 * session (i.e. that does not consume any messages) is started, in order to
 * reserve the slot by mark it "active", effectively distinguishing the
 * initializing session from a failed or abandoned session.
 *
 * Note that starting the replication session releases the initial snapshot,
 * which is why the callback must capture it synchronously, before the
 * replication session is started.
 *
 * The placeholder replication session is closed when the
 * PostgresChangeSource takes over the slot to process the stream in earnest.
 * It is also returned for cleanup in the case of failures (and for control
 * in unit tests).
 *
 * For replica's created in the `Restore` stage, the `generation` is
 * initialized to an empty string instead of the slot's consistent point,
 * since the slot is not being used for initial sync. It is the
 * responsibiliy of the caller to call {@link initRestoreReplica} when
 * the source replica has been selected for restoring, which will carry
 * over the `generation` and other relevant values from the source.
 */
export async function createReplicaAndSlot<T>(
  lc: LogContext,
  sql: PostgresDB,
  sessionName: string,
  shard: ShardID,
  epoch: number,
  replicaID: string,
  failover: boolean,
  backupOptions: BackupOptions,
  captureSnapshot: (snapshot: string) => Promise<T>,
  stage: ReplicaStage,
  {
    lockTimeout,
    maxBlockedMs = DEFAULT_MAX_SLOT_CREATION_BLOCKED_MS,
    retryDelayMs = DEFAULT_SLOT_CREATION_RETRY_DELAY_MS,
  }: CreateReplicaAndSlotOptions = {},
): Promise<ReplicationSlotResult<T>> {
  const lockName = replicationSlotManagementLock(shard);
  const slotPoolPrefix = replicationSlotPrefix(shard);
  const start = Date.now();
  let addedReplicationRole = false;
  let cleanedUpForSlotLimit = false;

  for (let attempt = 1; ; attempt++) {
    await dropUnclaimedSlots(lc, sql, shard);

    // Note: The replicationSession is used to create the replication slot
    // and closed immediately afterwards, or on error. A new session is used
    // for each attempt, since a failed attempt closes it.
    const replicationSession = createReplicationSessionFor(sql, sessionName);
    const lockSession = createLockSessionFor(sql, `${sessionName}-lock`);
    // Ending the lock session releases the replication slot management lock.
    // The sessions are ended with a timeout in case a hung slot creation
    // (e.g. after a network partition) is still pending.
    const endSessions = () =>
      Promise.allSettled([
        replicationSession.end({timeout: 5}),
        lockSession.end({timeout: 5}),
      ]);

    let slotName: string | undefined;
    let initialSession: Readable | undefined;
    let creatingSlot = false;
    try {
      await lockSession`SELECT pg_advisory_lock(hashtext(${lockName}))`;

      if (stage === InitialSync) {
        // With the lock acquired, ensure that only one initial sync is
        // active at a time. getRestoreCandidates() orders its results
        // with stage = InitialSync (and active = true) first.
        const others = await getRestoreCandidates(sql, shard, epoch);
        lc.info?.(`current replicas at epoch ${epoch}`, {replicas: others});
        if (others.length) {
          const [{id, stage, active}] = others;
          if (stage === InitialSync && active) {
            throw new AutoResetSignal(
              `another replica (${id}) is performing initial sync`,
            );
          }
        }
      }

      // Pick an available slotName from the slotPoolPrefix pool.
      const names = await sql<{name: string}[]> /*sql*/ `
        SELECT slot_name as name FROM pg_replication_slots
          WHERE slot_name LIKE ${slotPoolPrefix + '%'};
      `.values();
      const inUse = new Set(names.flat());
      for (let next = 0; ; next++) {
        const candidateName = `${slotPoolPrefix}${slotPoolSuffix(next)}`;
        if (!inUse.has(candidateName)) {
          slotName = candidateName;
          break;
        }
      }

      creatingSlot = true;
      const slot = await createReplicationSlot(lc, replicationSession, {
        slotName,
        failover,
        lockTimeout,
        diagnosticsDB: sql,
      });
      creatingSlot = false;
      const capturedSnapshot = await captureSnapshot(slot.snapshot_name);
      await replicationSession.end({timeout: 5});
      initialSession = await keepSlotActiveUntilTakenOver(lc, {
        db: extractConnectionConfig(sql),
        slot: slot.slot_name,
        dummyPublication: metadataPublicationName(shard.appID, shard.shardNum),
        lsn: String(toBigInt(slot.consistent_point)),
      });

      const replica = await runTx(sql, async tx => {
        await createReplica(
          tx,
          shard,
          replicaID,
          slot.slot_name,
          epoch,
          stage === InitialSync
            ? toStateVersionString(slot.consistent_point)
            : '', // initialized with initRestoreReplica
          backupOptions,
          stage,
        );
        return must(
          await getReplicaState(tx, shard, replicaID),
          `replica ${replicaID} was not created`,
        );
      });

      return {slot, capturedSnapshot, initialSession, replica};
    } catch (e) {
      // Release the lock (and end the attempt's sessions) before handling the
      // error, e.g. as dropUnclaimedSlots() acquires the lock from a different
      // session.
      await endSessions();
      if (
        !addedReplicationRole &&
        isPostgresError(e, PG_INSUFFICIENT_PRIVILEGE)
      ) {
        // Some Postgres variants (e.g. Google Cloud SQL) require that
        // the user have the REPLICATION role in order to create a slot.
        // Note that this must be done by the upstreamDB connection, and
        // does not work in the replicationSession itself.
        await sql`ALTER ROLE current_user WITH REPLICATION`;
        lc.info?.(`Added the REPLICATION role to database user`);
        addedReplicationRole = true;
        continue;
      }
      // Note: This is currently manually tested since max_replication_slots
      //       is a PG startup parameter that other tests depend on.
      // TODO: Figure out a way to unit test this (with the full PG setup).
      if (
        !cleanedUpForSlotLimit &&
        isPostgresError(e, PG_CONFIGURATION_LIMIT_EXCEEDED)
      ) {
        lc.warn?.(
          `Reached max replication slots. Attempting to clean up unused slots`,
          e,
        );
        // Drop any inactive replicas from failed initial syncs (e.g. inactive slots).
        const replicasTable = `${upstreamSchema(shard)}.replicas`;
        await sql`
          DELETE FROM ${sql(replicasTable)} USING pg_replication_slots slots
            WHERE replicas.slot = slots.slot_name AND NOT slots.active`;
        cleanedUpForSlotLimit = true;
        continue; // then let dropUnclaimedSlots() perform its cleanup
      }
      if (initialSession) {
        try {
          await endSession(initialSession);
        } catch {}
      }
      // The slot may not exist (e.g. if its creation failed), and its name
      // may be reused once the lock is released, so any created slot is
      // left to dropUnclaimedSlots(), which only drops slots without a
      // replica. This runs at the start of the next attempt.
      const blockedMs = Date.now() - start;
      if (
        creatingSlot &&
        isSlotCreationBlocked(e) &&
        blockedMs < maxBlockedMs
      ) {
        lc.warn?.(
          `Replication slot ${slotName} creation blocked by upstream transactions ` +
            `(attempt ${attempt}, ${blockedMs} ms). Retrying.`,
          e,
        );
        await sleep(retryDelayMs * (1 + 4 * Math.random()));
        continue;
      }
      if (slotName) {
        lc.warn?.(`deleting slot ${slotName} due to error`, e);
        await dropUnclaimedSlots(lc, sql, shard);
      }
      throw e;
    } finally {
      await endSessions();
    }
  }
}

/**
 * Claims the inactive slot of the given replica for use by this task, opening
 * a replication session to mark it as active. The process should follow up by
 * taking over the slot via the PostgresChangeSource.
 *
 * The replica is identified by its ID (and not just its slot) because rows of
 * replicas whose slots were dropped outside of zero-cache's control may share
 * the slot name.
 */
export async function claimSlotForResumption(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  _sessionName: string,
  {id: replicaID, slot}: Pick<ReplicaState, 'id' | 'slot'>,
): Promise<ReservedSlot | null> {
  const lockName = replicationSlotManagementLock(shard);
  const replicasTable = `${upstreamSchema(shard)}.replicas`;

  let reservation: Readable | undefined;
  try {
    const reserved = await runTx(sql, async tx => {
      await tx`SELECT pg_advisory_xact_lock(hashtext(${lockName}))`;

      const replicas = await tx<{active: boolean; lsn: string}[]> /*sql*/ `
      SELECT active, confirmed_flush_lsn as lsn
        FROM pg_replication_slots
        JOIN ${tx(replicasTable)} replica on slot_name = slot
        WHERE slot_name = ${slot} AND replica.id = ${replicaID};
    `;
      if (replicas.length === 0) {
        lc.warn?.(`no replica ${replicaID} found for slot ${slot}`);
        return null;
      }
      const [{active, lsn}] = replicas;
      if (active) {
        lc.warn?.(
          `replica ${replicaID} for slot ${slot} is active and cannot be claimed`,
        );
        return null;
      }
      // Mark the replica as Restoring.
      await tx`
        UPDATE ${tx(replicasTable)} SET "stage" = ${Restore} WHERE id = ${replicaID};
      `;
      const replica = must(
        await getReplicaState(tx, shard, replicaID),
        `replica ${replicaID} disappeared`,
      );

      reservation = await keepSlotActiveUntilTakenOver(lc, {
        db: extractConnectionConfig(sql),
        slot,
        dummyPublication: metadataPublicationName(shard.appID, shard.shardNum),
        lsn: String(toBigInt(lsn)),
      });
      return {
        slot,
        replica,
        reservation,
      };
    });
    lc.info?.(`successfully claimed slot ${slot}`);
    return reserved;
  } catch (e) {
    if (reservation) {
      try {
        await endSession(reservation);
      } catch {}
    }
    lc.error?.(`unable to claim replication slot ${slot}`, e);
    return null;
  }
}

export function dropInactiveSlotsAndReplicas(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  slots: string[],
) {
  const lockName = replicationSlotManagementLock(shard);
  const replicasTable = `${upstreamSchema(shard)}.replicas`;

  return runTx(sql, async tx => {
    await tx`SELECT pg_advisory_xact_lock(hashtext(${lockName}))`;

    const dropped = await tx<{slot: string; replicaID: string | null}[]>
    /*sql*/ `
      SELECT slot_name as slot, replica.id as "replicaID", pg_drop_replication_slot(slot_name) 
        FROM pg_replication_slots
        LEFT JOIN ${tx(replicasTable)} replica on slot_name = slot
        WHERE slot_name IN ${tx(slots)} AND NOT active;
    `;
    if (dropped.length) {
      lc.info?.(`dropped ${dropped.length} inactive replication slot(s)`, {
        dropped,
      });
    }

    const replicas = dropped
      .map(({replicaID}) => replicaID)
      .filter(id => id !== null);
    if (replicas.length) {
      // Delete replicas associated with the inactive (and now dropped) slots.
      await tx
      /*sql*/ `DELETE FROM ${tx(replicasTable)} WHERE id IN ${tx(replicas)}`;
      lc.info?.(`deleted ${replicas.length} old replica(s)`, {replicas});
    }
  });
}

function dropUnclaimedSlots(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
): Promise<{dropped: number; active: number; draining: number}> {
  // The slot / replica cleanup happens within a transaction while holding
  // the replication slot management lock for this shard, to ensure that no
  // slot that belongs to a newer replica is dropped.
  const lockName = replicationSlotManagementLock(shard);
  const slotExpression = replicationSlotExpression(shard);
  const replicasTable = `${upstreamSchema(shard)}.replicas`;

  return runTx(sql, async tx => {
    await tx`SELECT pg_advisory_xact_lock(hashtext(${lockName}))`;

    const dropped = await tx /*sql*/ `
      SELECT slot_name as slot, pg_drop_replication_slot(slot_name) 
        FROM pg_replication_slots
        LEFT JOIN ${tx(replicasTable)} replica on slot_name = slot
        WHERE slot_name LIKE ${slotExpression} 
          AND NOT active
          AND replica.id IS NULL;
    `;
    if (dropped.length) {
      lc.info?.(`dropped inactive replication slots`, {dropped});
    }

    // Conversely, delete replicas whose slots no longer exist, e.g. because
    // they were dropped outside of zero-cache's control (such as in an
    // upstream failover). Slot names are reused, so such rows would otherwise
    // be associated with a new slot of the same name. This is done (and
    // committed) before the slot is created, both so that the rows are never
    // associated with it, and because CREATE_REPLICATION_SLOT waits for
    // transactions that have written (i.e. been assigned an xid).
    const orphaned = await tx<{id: string}[]> /*sql*/ `
      DELETE FROM ${tx(replicasTable)} replica
        WHERE NOT EXISTS (
          SELECT 1 FROM pg_replication_slots WHERE slot_name = replica.slot
        )
        RETURNING id;
    `;
    if (orphaned.length) {
      lc.warn?.(`deleted replicas whose slots no longer exist`, {
        replicas: orphaned.map(({id}) => id),
      });
    }

    const remaining = await tx<
      {slot: string; pid: number | null; id: string | null}[]
    > /*sql*/ `
      SELECT slot_name as slot, active_pid as pid, replica.id as id
        FROM pg_replication_slots
        LEFT JOIN ${tx(replicasTable)} replica on slot_name = slot
        WHERE slot_name LIKE ${slotExpression};
    `;
    if (remaining.length) {
      lc.info?.(`remaining replication slots`, {remaining});
    }

    let active = 0;
    let draining = 0;
    for (const {id} of remaining) {
      if (id === null) {
        draining++;
      } else {
        active++;
      }
    }

    return {
      dropped: dropped.length,
      active,
      draining,
    };
  });
}

const ALPHABET = 'abcdefghijklmnopqrstuvwxyz';

// Alphabetic notation is used as the slot pool suffix to distinguish
// it from the (numeric) shard num that's also encoded in the slot name.
export function slotPoolSuffix(n: number) {
  n++; // Adjust for 0-based indexing

  let suffix = '';
  while (n > 0) {
    n--;
    suffix = ALPHABET[n % 26] + suffix;
    n = Math.floor(n / 26);
  }
  return suffix;
}

function replicationSlotManagementLock(shard: ShardID) {
  return `replication-slot-management:${shard.appID}_${shard.shardNum}`;
}
