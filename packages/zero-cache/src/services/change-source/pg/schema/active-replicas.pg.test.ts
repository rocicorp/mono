import {LogContext} from '@rocicorp/logger';
import {expect} from 'vitest';
import {TestLogSink} from '../../../../../../shared/src/logging-test-utils.ts';
import {pgContainerTest as test} from '../../../../test/db.ts';
import {pgClient, type PostgresDB} from '../../../../types/pg.ts';
import {
  createReplica,
  getActiveReplicas,
  initReplica,
  setupTablesAndReplication,
} from './shard.ts';

const SHARD = {appID: 'zro', shardNum: 0};

async function addReplica(
  db: PostgresDB,
  id: string,
  kind: 'logical' | 'unreserved-physical' = 'logical',
) {
  const slot = `zro_0_${id}`;
  if (kind === 'logical') {
    await db`SELECT pg_create_logical_replication_slot(${slot}, 'pgoutput')`;
  } else {
    // A physical slot that does not reserve WAL has a NULL restart_lsn
    // (and a NULL wal_status), isolating the restart_lsn check.
    await db`SELECT pg_create_physical_replication_slot(${slot})`;
  }
  await createReplica(db, SHARD, id, slot, '01', {
    backupPath: null,
    backupV5: true,
  });
  await initReplica(db, SHARD, id, {tables: [], indexes: []}, {});
}

// Uses a dedicated (per-worker) container because invalidating a slot
// requires changing the server-wide max_slot_wal_keep_size.
test('getActiveReplicas excludes replicas with invalidated slots', async ({
  pgConnectionString,
}) => {
  const lc = new LogContext('warn', {}, new TestLogSink());
  const db = pgClient(lc, pgConnectionString, 'active-replicas-test');
  try {
    await db.begin(tx =>
      setupTablesAndReplication(lc, tx, {...SHARD, publications: []}),
    );
    await addReplica(db, 'lost');
    expect((await getActiveReplicas(lc, db, SHARD)).map(r => r.id)).toEqual([
      'lost',
    ]);

    // Invalidate the slot by exceeding max_slot_wal_keep_size.
    await db`ALTER SYSTEM SET max_slot_wal_keep_size = '1MB'`.simple();
    await db`SELECT pg_reload_conf()`;
    try {
      await db`CREATE TABLE wal_filler (data TEXT)`;
      for (let i = 0; i < 3; i++) {
        await db`INSERT INTO wal_filler SELECT repeat('x', 1000) FROM generate_series(1, 1000)`;
        await db`SELECT pg_switch_wal()`;
      }
      await db`CHECKPOINT`;
    } finally {
      await db`ALTER SYSTEM RESET max_slot_wal_keep_size`.simple();
      await db`SELECT pg_reload_conf()`;
    }

    await addReplica(db, 'valid');
    await addReplica(db, 'unreserved', 'unreserved-physical');

    expect(
      await db`SELECT slot_name AS slot, wal_status AS "walStatus",
          restart_lsn IS NULL AS "noRestartLSN"
        FROM pg_replication_slots ORDER BY slot_name`,
    ).toEqual([
      {slot: 'zro_0_lost', walStatus: 'lost', noRestartLSN: true},
      {slot: 'zro_0_unreserved', walStatus: null, noRestartLSN: true},
      {slot: 'zro_0_valid', walStatus: 'reserved', noRestartLSN: false},
    ]);
    expect((await getActiveReplicas(lc, db, SHARD)).map(r => r.id)).toEqual([
      'valid',
    ]);
  } finally {
    await db.end();
  }
});
