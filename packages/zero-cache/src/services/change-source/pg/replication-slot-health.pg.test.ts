import {expect} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import {getConnectionURI, type PgTest, test} from '../../../test/db.ts';
import {pgClient} from '../../../types/pg.ts';
import {
  queryReplicationSlotHealth,
  slotHealthStatus,
} from './replication-slot-health.ts';
import {createReplicationSlot} from './replication-slots.ts';

test('queryReplicationSlotHealth reads pg_replication_slots', async ({
  testDBs,
}: PgTest) => {
  const lc = createSilentLogContext();
  const upstream = await testDBs.create('replication_slot_health');
  const slotName = `slot_health_${Date.now()}`;
  const session = pgClient(lc, getConnectionURI(upstream), 'slot-health', {
    max: 1,
    ['fetch_types']: false,
    connection: {replication: 'database'},
  });

  try {
    const slot = await createReplicationSlot(lc, session, {slotName});
    const row = await queryReplicationSlotHealth(upstream, slot.slot_name);

    expect(row).toBeDefined();
    expect(slotHealthStatus(row)).not.toBe('missing');
    expect(row?.retainedWalBytes).toEqual(expect.any(Number));
    expect(row?.retainedWalBytes).toBeGreaterThanOrEqual(0);
    if (row?.safeWalBytes !== null) {
      expect(row?.safeWalBytes).toEqual(expect.any(Number));
      expect(row?.safeWalBytes).toBeGreaterThanOrEqual(0);
    }

    await expect(
      queryReplicationSlotHealth(upstream, `${slotName}_missing`),
    ).resolves.toBeUndefined();
  } finally {
    await session.end().catch(() => {});
    await upstream`
      SELECT pg_drop_replication_slot(slot_name)
      FROM pg_replication_slots
      WHERE slot_name = ${slotName} AND NOT active`;
    await testDBs.drop(upstream);
  }
});
