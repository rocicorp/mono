/**
 * `observability/metrics.ts` caches every instrument in module scope, so an
 * instrument created against a previous test's meter provider would be
 * handed back to the next one. Each test therefore resets modules and
 * imports through a fresh provider, matching `life-cycle.metrics.test.ts`
 * and `backfilling-metrics.test.ts`.
 */

import {tmpdir} from 'node:os';
import path from 'node:path';
import {metrics} from '@opentelemetry/api';
import {
  AggregationTemporality,
  InMemoryMetricExporter,
  MeterProvider,
  PeriodicExportingMetricReader,
} from '@opentelemetry/sdk-metrics';
import {afterEach, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {Subscription} from '../../types/subscription.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

afterEach(() => {
  metrics.disable();
  vi.resetModules();
});

const METRIC = 'zero.replica.backup_lag';

function withProvider() {
  const exporter = new InMemoryMetricExporter(
    AggregationTemporality.CUMULATIVE,
  );
  const provider = new MeterProvider({
    readers: [
      new PeriodicExportingMetricReader({
        exporter,
        exportIntervalMillis: 60_000,
      }),
    ],
  });
  expect(metrics.setGlobalMeterProvider(provider)).toBe(true);
  return {
    exporter,
    provider,
    [Symbol.asyncDispose]: () => provider.shutdown(),
  };
}

// InMemoryMetricExporter accumulates one ResourceMetrics per export() call,
// so a forceFlush() partway through a test appends rather than replaces.
// Read only the most recent export, and reset afterwards so the next
// forceFlush() starts a clean batch.
function latestValueAndReset(exporter: InMemoryMetricExporter) {
  const exports = exporter.getMetrics();
  const points = exports
    .at(-1)
    ?.scopeMetrics.flatMap(scope => scope.metrics)
    .find(metric => metric.descriptor.name === METRIC)?.dataPoints;
  exporter.reset();
  return points?.at(-1)?.value as number | undefined;
}

function replicaFile(name: string) {
  return path.join(tmpdir(), `backup-monitor-metrics-${name}-${Date.now()}`);
}

function makeReplica(file: string, writeTimeMs: number) {
  const lc = createSilentLogContext();
  const db = new Database(lc, file);
  db.exec(/*sql*/ `
    CREATE TABLE "_zero.replicationState" (
      stateVersion TEXT NOT NULL,
      writeTimeMs INTEGER NOT NULL,
      lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
    );
  `);
  db.prepare(
    /*sql*/ `INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES (?, ?)`,
  ).run('01', writeTimeMs);
  db.close();
}

function backedUp(watermark: string, backupTimeMs: number): BackedUpWatermark {
  return {watermark, backupTimeMs};
}

test('reports the lag between the replica write time and the latest backup', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {BackupMonitor} = await import('./backup-monitor.ts');
    const file = replicaFile('basic');
    makeReplica(file, 10_000);

    const watermarks = Subscription.create<BackedUpWatermark>();
    const changeStreamer = {
      trackBackupWatermark: () => {},
    } as unknown as ChangeStreamerService;
    const monitor = new BackupMonitor(
      createSilentLogContext(),
      watermarks,
      changeStreamer,
      file,
    );

    const run = monitor.run();
    watermarks.push(backedUp('01', 4_000));
    await monitor.firstBackupReceived();

    await provider.forceFlush();
    expect(latestValueAndReset(exporter)).toBe(6_000); // 10_000 - 4_000

    watermarks.cancel();
    await run;
  } finally {
    await provider.shutdown();
  }
});

test('updates the lag as later backups arrive, not just the first', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {BackupMonitor} = await import('./backup-monitor.ts');
    const file = replicaFile('later-backups');
    makeReplica(file, 10_000);

    const trackBackupWatermark = vi.fn();
    const watermarks = Subscription.create<BackedUpWatermark>();
    const changeStreamer = {
      trackBackupWatermark,
    } as unknown as ChangeStreamerService;
    const monitor = new BackupMonitor(
      createSilentLogContext(),
      watermarks,
      changeStreamer,
      file,
    );

    const run = monitor.run();
    watermarks.push(backedUp('01', 1_000));
    await monitor.firstBackupReceived();
    await provider.forceFlush();
    expect(latestValueAndReset(exporter)).toBe(9_000); // 10_000 - 1_000

    // A later backup should move the lag down accordingly. Regression check
    // for a bug where #latestBackup was only ever set on the first watermark,
    // so the gauge kept reporting the lag against the *first* backup forever.
    watermarks.push(backedUp('02', 9_500));
    await vi.waitFor(() =>
      expect(trackBackupWatermark).toHaveBeenCalledWith('02'),
    );
    await provider.forceFlush();
    expect(latestValueAndReset(exporter)).toBe(500); // 10_000 - 9_500

    watermarks.cancel();
    await run;
  } finally {
    await provider.shutdown();
  }
});

test('clamps negative lag (backup ahead of the last observed write) to zero', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {BackupMonitor} = await import('./backup-monitor.ts');
    const file = replicaFile('clamped');
    makeReplica(file, 1_000);

    const watermarks = Subscription.create<BackedUpWatermark>();
    const changeStreamer = {
      trackBackupWatermark: () => {},
    } as unknown as ChangeStreamerService;
    const monitor = new BackupMonitor(
      createSilentLogContext(),
      watermarks,
      changeStreamer,
      file,
    );

    const run = monitor.run();
    watermarks.push(backedUp('01', 5_000));
    await monitor.firstBackupReceived();

    await provider.forceFlush();
    expect(latestValueAndReset(exporter)).toBe(0);

    watermarks.cancel();
    await run;
  } finally {
    await provider.shutdown();
  }
});

test('does not export a data point before any backup has been observed', async () => {
  const {exporter, provider} = withProvider();
  try {
    const {BackupMonitor} = await import('./backup-monitor.ts');
    const file = replicaFile('no-backup-yet');
    makeReplica(file, 10_000);

    const watermarks = Subscription.create<BackedUpWatermark>();
    const changeStreamer = {
      trackBackupWatermark: () => {},
    } as unknown as ChangeStreamerService;
    const monitor = new BackupMonitor(
      createSilentLogContext(),
      watermarks,
      changeStreamer,
      file,
    );

    const run = monitor.run();
    await provider.forceFlush();
    expect(latestValueAndReset(exporter)).toBeUndefined();

    watermarks.cancel();
    await run;
  } finally {
    await provider.shutdown();
  }
});
