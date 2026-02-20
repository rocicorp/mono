import type {LogContext} from '@rocicorp/logger';
import type {PostgresDB} from '../../../types/pg.ts';
import {dropShard} from './schema/shard.ts';

export async function decommissionShard(
  lc: LogContext,
  db: PostgresDB,
  appID: string,
  shardID: string | number,
) {
  const shard = `${appID}_${shardID}`;

  lc.info?.(`Decommissioning zero shard ${shard}`);
  await db.begin(async sql => {
    // Kill the active_pid's on existing slots before altering publications,
    // as deleting a publication associated with an existing subscriber causes
    // weirdness; the active_pid becomes null and thus unable to be terminated.
    const slots = await sql<
      {
        slot: string;
        pid: string | null;
        terminated: boolean | null;
      }[]
    >`
    SELECT slot_name as slot, pg_terminate_backend(active_pid) as terminated, active_pid as pid
      FROM pg_replication_slots 
      WHERE slot_name = ${shard} 
         OR slot_name LIKE ${shard + '_%'}`;
    if (slots.length > 0) {
      lc.info?.(
        `decommission targeted slots before drop: ${JSON.stringify(slots)}`,
      );
    }
    if (slots.length > 0) {
      const pids = slots.filter(({pid}) => pid !== null).map(({pid}) => pid);
      if (pids.length > 0) {
        lc.info?.(`signaled subscriber(s) ${pids} to shut down`);
      }
      // Escape underscores for the LIKE expression.
      const slotExpression = `${appID}_${shardID}_%`.replaceAll('_', '\\_');
      const dropped = await sql<{slotName: string}[]>`
        SELECT pg_drop_replication_slot(slot_name), slot_name as "slotName"
          FROM pg_replication_slots 
          WHERE slot_name = ${shard} 
             OR slot_name LIKE ${slotExpression}`;
      lc.debug?.(
        `Dropped replication slot(s) ${dropped.map(({slotName}) => slotName)}`,
      );
      const remaining = await sql<{slot: string}[]>`
        SELECT slot_name as slot
          FROM pg_replication_slots
          WHERE slot_name = ${shard}
             OR slot_name LIKE ${slotExpression}
          ORDER BY slot_name`;
      lc.debug?.(
        `Remaining decommission slot(s) after drop: ${remaining.map(({slot}) => slot)}`,
      );
      await sql.unsafe(dropShard(appID, shardID));
      lc.debug?.(`Dropped upstream shard schema ${shard} and event triggers`);
    }
  });
  lc.info?.(`Finished decommissioning zero shard ${shard}`);
}
