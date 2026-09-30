import type {ObservableResult} from '@opentelemetry/api';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {DbFile} from '../test/lite.ts';
import {setSingleProcessMode} from '../types/processes.ts';

setSingleProcessMode(true);

type GaugeCallback = (result: ObservableResult) => unknown;

const gaugeCallbacks = vi.hoisted(() => new Map<string, GaugeCallback>());
const getOrCreateGauge = vi.hoisted(() =>
  vi.fn((_category: unknown, name: string) => ({
    addCallback: (callback: GaugeCallback) => {
      gaugeCallbacks.set(name, callback);
    },
  })),
);

vi.mock('../observability/metrics.ts', () => ({getOrCreateGauge}));

const {setupMetrics} = await import('./replicator.ts');

describe('replicator metrics', () => {
  const lc = createSilentLogContext();
  let dbFile: DbFile;

  beforeEach(() => {
    gaugeCallbacks.clear();
    getOrCreateGauge.mockClear();
    dbFile = new DbFile('replicator-metrics-test');
  });

  afterEach(() => {
    dbFile.delete();
  });

  test.each(['wal', 'wal2'] as const)(
    'observes active and uncheckpointed WAL bytes in %s mode',
    walMode => {
      const db = new Database(lc, dbFile.path);
      db.pragma(`journal_mode = ${walMode.toUpperCase()}`);
      db.pragma('wal_autocheckpoint = 0');
      db.exec('CREATE TABLE test (id INT PRIMARY KEY, val TEXT)');
      for (let i = 0; i < 10; i++) {
        db.exec(`INSERT INTO test VALUES (${i}, 'val-${i}')`);
      }

      const [{page_size: pageSize}] = db.pragma<{page_size: number}>(
        'page_size',
      );
      const [{log: logBefore, checkpointed: checkpointedBefore}] = db.pragma<{
        log: number;
        checkpointed: number;
      }>('wal_checkpoint(NOOP)');

      expect(logBefore).toBeGreaterThan(0);

      setupMetrics(lc, dbFile.path, walMode, pageSize);

      const activeCallback = gaugeCallbacks.get('wal_active_bytes');
      const uncheckpointedCallback = gaugeCallbacks.get(
        'wal_uncheckpointed_bytes',
      );
      expect(activeCallback).toBeDefined();
      expect(uncheckpointedCallback).toBeDefined();

      if (walMode === 'wal2') {
        expect(gaugeCallbacks.get('wal2_size')).toBeDefined();
      } else {
        expect(gaugeCallbacks.get('wal2_size')).toBeUndefined();
      }

      let observedActive: number | undefined;
      let observedUncheckpointed: number | undefined;

      activeCallback!({
        observe: (val: number) => {
          observedActive = val;
        },
      });
      uncheckpointedCallback!({
        observe: (val: number) => {
          observedUncheckpointed = val;
        },
      });

      expect(observedActive).toBe(logBefore * pageSize);
      expect(observedUncheckpointed).toBe(
        (logBefore - checkpointedBefore) * pageSize,
      );

      // Checkpoint the WAL and verify metrics reflect the updated status
      db.pragma('wal_checkpoint(PASSIVE)');
      const [{log: logAfter, checkpointed: checkpointedAfter}] = db.pragma<{
        log: number;
        checkpointed: number;
      }>('wal_checkpoint(NOOP)');

      activeCallback!({
        observe: (val: number) => {
          observedActive = val;
        },
      });
      uncheckpointedCallback!({
        observe: (val: number) => {
          observedUncheckpointed = val;
        },
      });

      expect(observedActive).toBe(logAfter * pageSize);
      expect(observedUncheckpointed).toBe(
        (logAfter - checkpointedAfter) * pageSize,
      );

      if (walMode === 'wal') {
        expect(observedUncheckpointed).toBe(0);
      }

      db.close();
    },
  );
});
