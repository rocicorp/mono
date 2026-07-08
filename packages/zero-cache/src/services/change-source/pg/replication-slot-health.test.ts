import type {ObservableResult} from '@opentelemetry/api';
import {expect, test, vi, beforeEach} from 'vitest';
import {createSilentLogContext} from '../../../../../shared/src/logging-test-utils.ts';
import type {PostgresDB} from '../../../types/pg.ts';
import {
  registerReplicationSlotHealthMetrics,
  type SlotHealthRow,
  slotHealthStatus,
} from './replication-slot-health.ts';

type GaugeCallback = (result: ObservableResult) => unknown;

const gaugeCallbacks = vi.hoisted(() => new Map<string, GaugeCallback>());
const getOrCreateGauge = vi.hoisted(() =>
  vi.fn((_category: unknown, name: string) => ({
    addCallback: (callback: GaugeCallback) => {
      gaugeCallbacks.set(name, callback);
    },
  })),
);

vi.mock('../../../observability/metrics.ts', () => ({getOrCreateGauge}));

beforeEach(() => {
  gaugeCallbacks.clear();
  getOrCreateGauge.mockClear();
});

test('slotHealthStatus classifies slot rows', () => {
  expect(slotHealthStatus(undefined)).toBe('missing');
  expect(slotHealthStatus({restartLSN: null, walStatus: 'reserved'})).toBe(
    'lost',
  );
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: 'lost'})).toBe('lost');
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: 'reserved'})).toBe(
    'ok',
  );
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: 'extended'})).toBe(
    'ok',
  );
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: 'unreserved'})).toBe(
    'unreserved',
  );
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: null})).toBe(
    'unknown',
  );
  expect(slotHealthStatus({restartLSN: '0/1', walStatus: 'future'})).toBe(
    'unknown',
  );
});

test('slot health metrics observe current slot status and WAL bytes', async () => {
  const {db} = registerForRows([
    {
      restartLSN: '0/1',
      walStatus: 'reserved',
      retainedWalBytes: 123,
      safeWalBytes: 456,
    },
  ]);

  const health = await observeGauge('slot_health');
  expect(statusObservations(health.observe)).toEqual({
    ok: 1,
    unreserved: 0,
    lost: 0,
    missing: 0,
    unknown: 0,
  });

  const retained = await observeGauge('slot_retained_wal_bytes');
  expect(retained.observe).toHaveBeenCalledExactlyOnceWith(123, {
    slot: 'slot_1',
  });

  const safe = await observeGauge('slot_safe_wal_bytes');
  expect(safe.observe).toHaveBeenCalledExactlyOnceWith(456, {slot: 'slot_1'});
  expect(db).toHaveBeenCalledTimes(3);
});

test('slot health metric observes missing when the slot row is absent', async () => {
  registerForRows([]);

  const health = await observeGauge('slot_health');
  expect(statusObservations(health.observe)).toMatchObject({missing: 1});
});

test('slot health metric observes unknown on query error', async () => {
  registerForError(new Error('boom'));

  const health = await observeGauge('slot_health');
  expect(statusObservations(health.observe)).toMatchObject({unknown: 1});
});

test('byte metrics skip null values', async () => {
  registerForRows([
    {
      restartLSN: '0/1',
      walStatus: 'reserved',
      retainedWalBytes: 123,
      safeWalBytes: null,
    },
  ]);

  const safe = await observeGauge('slot_safe_wal_bytes');
  expect(safe.observe).not.toHaveBeenCalled();
});

test('slot health metrics do not query after the change source stops', async () => {
  const {db} = registerForRows(
    [
      {
        restartLSN: '0/1',
        walStatus: 'reserved',
        retainedWalBytes: 123,
        safeWalBytes: 456,
      },
    ],
    true,
  );

  for (const name of [
    'slot_health',
    'slot_retained_wal_bytes',
    'slot_safe_wal_bytes',
  ]) {
    const {observe} = await observeGauge(name);
    expect(observe).not.toHaveBeenCalled();
  }
  expect(db).not.toHaveBeenCalled();
});

function registerForRows(rows: SlotHealthRow[], stopped = false) {
  const db = vi.fn(() => rows);
  registerReplicationSlotHealthMetrics(
    createSilentLogContext(),
    db as unknown as PostgresDB,
    'slot_1',
    () => stopped,
  );
  return {db};
}

function registerForError(error: unknown) {
  const db = vi.fn(() => {
    throw error;
  });
  registerReplicationSlotHealthMetrics(
    createSilentLogContext(),
    db as unknown as PostgresDB,
    'slot_1',
    () => false,
  );
  return {db};
}

async function observeGauge(name: string) {
  const callback = gaugeCallbacks.get(name);
  if (!callback) {
    throw new Error(`missing callback for ${name}`);
  }
  const observe = vi.fn();
  await callback({observe} as unknown as ObservableResult);
  return {observe};
}

function statusObservations(observe: ReturnType<typeof vi.fn>) {
  return Object.fromEntries(
    observe.mock.calls.map(([value, attributes]) => [
      (attributes as {status: string}).status,
      value,
    ]),
  );
}
