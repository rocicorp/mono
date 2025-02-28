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
  await db.begin(async tx => {
    await tx.unsafe(dropShard(appID, shardID));
    lc.debug?.(`Dropped upstream shard schema ${shard} and event triggers`);

    const slots = await tx<{pid: string | null}[]>`
    SELECT pg_terminate_backend(active_pid), active_pid as pid
      FROM pg_replication_slots WHERE slot_name = ${shard}`;
    if (slots.length > 0) {
      if (slots[0].pid !== null) {
        lc.info?.(`signaled subscriber ${slots[0].pid} to shut down`);
      }
      await tx`SELECT pg_drop_replication_slot(${shard})`;
      lc.debug?.(`Dropped replication slot ${shard}`);
    }
  });
  lc.info?.(`Finished decommissioning zero shard ${shard}`);
}
