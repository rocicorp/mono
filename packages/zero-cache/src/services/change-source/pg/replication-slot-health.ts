import type {ObservableResult} from '@opentelemetry/api';
import type {LogContext} from '@rocicorp/logger';
import {getOrCreateGauge} from '../../../observability/metrics.ts';
import type {PostgresDB} from '../../../types/pg.ts';

const SLOT_HEALTH_STATUSES = [
  'ok',
  'unreserved',
  'lost',
  'missing',
  'unknown',
] as const;

export type SlotHealthStatus = (typeof SLOT_HEALTH_STATUSES)[number];

export type SlotHealthRow = {
  readonly restartLSN: string | null;
  readonly walStatus: string | null;
  readonly retainedWalBytes: number | null;
  readonly safeWalBytes: number | null;
};

export function slotHealthStatus(
  row: Pick<SlotHealthRow, 'restartLSN' | 'walStatus'> | undefined,
): SlotHealthStatus {
  if (row === undefined) {
    return 'missing';
  }
  if (row.restartLSN === null || row.walStatus === 'lost') {
    return 'lost';
  }
  switch (row.walStatus) {
    case 'reserved':
    case 'extended':
      return 'ok';
    case 'unreserved':
      return 'unreserved';
    default:
      return 'unknown';
  }
}

export function registerReplicationSlotHealthMetrics(
  lc: LogContext,
  db: PostgresDB,
  slot: string,
  isStopped: () => boolean,
): void {
  getOrCreateGauge('replication', 'slot_health', {
    description: 'Health of the active logical replication slot.',
    unit: '1',
  }).addCallback(async result => {
    if (isStopped()) {
      return;
    }

    let status: SlotHealthStatus;
    try {
      status = slotHealthStatus(await queryReplicationSlotHealth(db, slot));
    } catch (e) {
      lc.warn?.(`error querying replication slot health`, e);
      status = 'unknown';
    }

    for (const s of SLOT_HEALTH_STATUSES) {
      result.observe(s === status ? 1 : 0, {slot, status: s});
    }
  });

  getOrCreateGauge('replication', 'slot_retained_wal_bytes', {
    description: 'WAL bytes retained by the active logical replication slot.',
    unit: 'By',
  }).addCallback(result =>
    observeSlotBytes(lc, db, slot, isStopped, result, 'retainedWalBytes'),
  );

  getOrCreateGauge('replication', 'slot_safe_wal_bytes', {
    description:
      'WAL bytes the active logical replication slot can retain before loss.',
    unit: 'By',
  }).addCallback(result =>
    observeSlotBytes(lc, db, slot, isStopped, result, 'safeWalBytes'),
  );
}

async function observeSlotBytes(
  lc: LogContext,
  db: PostgresDB,
  slot: string,
  isStopped: () => boolean,
  result: ObservableResult,
  field: 'retainedWalBytes' | 'safeWalBytes',
): Promise<void> {
  if (isStopped()) {
    return;
  }
  try {
    const row = await queryReplicationSlotHealth(db, slot);
    const value = row?.[field];
    if (typeof value === 'number' && Number.isFinite(value)) {
      result.observe(value, {slot});
    }
  } catch (e) {
    lc.warn?.(`error querying replication slot ${field}`, e);
  }
}

export async function queryReplicationSlotHealth(
  db: PostgresDB,
  slot: string,
): Promise<SlotHealthRow | undefined> {
  const rows = await db<SlotHealthRow[]>`
    SELECT
      restart_lsn AS "restartLSN",
      wal_status AS "walStatus",
      CASE
        WHEN restart_lsn IS NULL THEN NULL
        ELSE pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::float8
      END AS "retainedWalBytes",
      safe_wal_size::float8 AS "safeWalBytes"
    FROM pg_replication_slots
    WHERE slot_name = ${slot}`;
  return rows[0];
}
