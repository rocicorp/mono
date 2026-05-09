import {PG_INSUFFICIENT_PRIVILEGE} from '@drdgvhbh/postgres-error-codes';
import type {LogContext} from '@rocicorp/logger';
import type postgres from 'postgres';
import {runTx} from '../../../db/run-transaction';
import {isPostgresError, type PostgresDB} from '../../../types/pg';
import {upstreamSchema, type ShardID} from '../../../types/shards';
import {orTimeout} from '../../../types/timeout';
import {toStateVersionString} from './lsn';
import {
  createReplica,
  replicationSlotExpression,
  replicationSlotPrefix,
} from './schema/shard';

// Record returned by `CREATE_REPLICATION_SLOT`
export type ReplicationSlot = {
  slot_name: string;
  consistent_point: string;
  snapshot_name: string;
  output_plugin: string;
};

export type CreateSlotSpec = {
  slotName: string;

  // Note: must be false if pgVersion < PG_17. Caller must verify.
  failover?: boolean;

  // For overriding in tests.
  lockTimeout?: number;
};

// When creating a replication slot, Postgres waits for open transactions
// to complete before reserving a consistent_point (LSN) in the WAL and creating
// a matching transaction snapshot. As such, it can technically take an arbitrary
// amount of time (e.g. DDL operations, table-wide operations, etc.).
//
// However, to detect pathological situations, bound the amount of time that
// the server waits for replication slot creation, so that a continual failure to
// create a replication slot is surfaced by errors / alerts.
const CREATE_REPLICATION_SLOT_TIMEOUT_MS = 30_000;

// The lock_timeout is set 1s before the client-side orTimeout so that
// Postgres reliably aborts first and tears down the walsender cleanly.
// The client-side timeout remains as a fallback for network-level failures.
const SERVER_LOCK_TIMEOUT_MS = CREATE_REPLICATION_SLOT_TIMEOUT_MS - 1_000;

// Note: The replication connection does not support the extended query protocol,
//       so all commands must be sent using sql.unsafe(). This is technically safe
//       because all placeholder values are under our control (i.e. "slotName").
export async function createReplicationSlot(
  lc: LogContext,
  session: postgres.Sql,
  {slotName, failover, lockTimeout = SERVER_LOCK_TIMEOUT_MS}: CreateSlotSpec,
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

  const createSlot = failover
    ? session.unsafe<ReplicationSlot[]>(
        /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput (FAILOVER)`,
      )
    : session.unsafe<ReplicationSlot[]>(
        /*sql*/ `CREATE_REPLICATION_SLOT "${slotName}" LOGICAL pgoutput`,
      );
  const raced = await orTimeout(createSlot, CREATE_REPLICATION_SLOT_TIMEOUT_MS);
  if (raced === 'timed-out') {
    // Create slot can block indefinitely waiting for old transactions. End
    // this connection in the background and fail fast so the process restarts.
    void session
      .end()
      .catch(e =>
        lc.warn?.(`Error closing timed out replication slot session`, e),
      );
    throw new Error(
      `Timed out after ${CREATE_REPLICATION_SLOT_TIMEOUT_MS} ms creating replication slot ${slotName}. ` +
        `Crashing to force a clean restart.`,
    );
  }
  const [slot] = raced;
  lc.info?.(`Created replication slot ${slotName}`, slot);
  return slot;
}

/**
 * Replica and slot creation involves two sessions for proper coordination
 * with other replica management logic:
 *
 * * A normal transaction is started and acquires an advisory lock for
 *   replica slot management. This is the same lock that cleanup logic
 *   acquires before cleaning up replication slots.
 * * With the lock held, a new replication slot is created in a
 *   replication session. The API of CREATE_REPLICATION_SLOT is such
 *   that it cannot be done in a transaction, and cannot be followed by
 *   any writes, or else its snapshot (which is needed for initial sync)
 *   would be invalidated.
 * * Once the slot is created, the slot and replica information are recorded
 *   in the `replicas` table before releasing the lock.
 *
 * This locking ensures that:
 * 1. multiple replication managers attempting to create a replication slot
 *    will not use the same name for the replication slot (which is selected
 *    from a pool of reused names).
 * 2. Running replication managers (which use an earlier replica of a lower
 *    rank) will not delete the new slot during their cleanup logic, since
 *    the slot will belong to a replica of a higher rank.
 */
export async function createReplicaAndSlot(
  lc: LogContext,
  sql: PostgresDB,
  replicationSession: postgres.Sql,
  shard: ShardID,
  replicaID: string,
  failover: boolean,
): Promise<ReplicationSlot> {
  await dropUnclaimedSlots(lc, sql, shard);

  const lockName = replicationSlotManagementLock(shard);
  const slotPoolPrefix = replicationSlotPrefix(shard);
  for (let first = true; ; first = false) {
    try {
      return runTx(sql, async tx => {
        await tx`SELECT pg_advisory_xact_lock(hashtext(${lockName}))`;

        // Pick an available slotName from the slotPoolPrefix pool.
        let slotName: string;
        const names = await tx<{name: string}[]> /*sql*/ `
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

        const slot = await createReplicationSlot(lc, replicationSession, {
          slotName,
          failover,
        });

        await createReplica(
          tx,
          shard,
          replicaID,
          slot.slot_name,
          toStateVersionString(slot.consistent_point),
        );

        return slot;
      });
    } catch (e) {
      if (first && isPostgresError(e, PG_INSUFFICIENT_PRIVILEGE)) {
        // Some Postgres variants (e.g. Google Cloud SQL) require that
        // the user have the REPLICATION role in order to create a slot.
        // Note that this must be done by the upstreamDB connection, and
        // does not work in the replicationSession itself.
        await sql`ALTER ROLE current_user WITH REPLICATION`;
        lc.info?.(`Added the REPLICATION role to database user`);
        continue;
      }
      throw e;
    }
  }
}

/**
 * Deletes "old" replicas (i.e. those with a lower rank than the current)
 * and attempts to drop replication slots that are not associated with any
 * replica.
 *
 * If a slot could not be dropped because there is still an active subscriber,
 * it will be reflected in the `draining` count that is returned. When there
 * are draining slots, the method should be retried until all orphaned slots
 * have been dropped.
 */
export async function dropOldReplicasAndSlots(
  lc: LogContext,
  sql: PostgresDB,
  shard: ShardID,
  beforeRank: bigint,
): Promise<{dropped: number; active: number; draining: number}> {
  const replicasTable = `${upstreamSchema(shard)}.replicas`;
  const oldReplicas = await sql`
    SELECT id, rank::float8, slot, version, "initialSyncContext", "subscriberContext"
     FROM ${sql(replicasTable)} WHERE rank < ${beforeRank};
  `;
  if (oldReplicas.length) {
    lc.info?.(`Deleting ${oldReplicas.length} old replica(s)`, {oldReplicas});
    await sql`DELETE FROM ${sql(replicasTable)} WHERE rank < ${beforeRank}`;
  }

  return dropUnclaimedSlots(lc, sql, shard);
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
