import {tmpdir} from 'node:os';
import path from 'node:path';
import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {Subscription} from '../../types/subscription.ts';
import {BackupMonitor, type BackedUpWatermark} from './backup-monitor.ts';
import type {ChangeStreamerService} from './change-streamer.ts';

describe('change-streamer/backup-monitor', () => {
  let watermarks: Subscription<BackedUpWatermark>;
  let trackBackupWatermark: ReturnType<typeof vi.fn>;
  let changeStreamer: ChangeStreamerService;
  let monitor: BackupMonitor;

  beforeEach(() => {
    watermarks = Subscription.create<BackedUpWatermark>();
    trackBackupWatermark = vi.fn();
    changeStreamer = {
      trackBackupWatermark,
    } as unknown as ChangeStreamerService;
    monitor = new BackupMonitor(
      createSilentLogContext(),
      watermarks,
      changeStreamer,
      '/tmp/backup-monitor-test-replica-does-not-exist.db',
    );
  });

  function backedUp(watermark: string, ms = 0): BackedUpWatermark {
    return {watermark, writeTimeMs: ms, backupTimeMs: ms};
  }

  test('firstBackupReceived stays pending until the first watermark arrives', async () => {
    const run = monitor.run();
    let resolved = false;
    void monitor.firstBackupReceived().then(() => (resolved = true));

    // Give run() a chance to start iterating; it should still be waiting.
    await Promise.resolve();
    await Promise.resolve();
    expect(resolved).toBe(false);
    expect(trackBackupWatermark).not.toHaveBeenCalled();

    watermarks.push(backedUp('01'));

    await monitor.firstBackupReceived();
    expect(resolved).toBe(true);
    expect(trackBackupWatermark).toHaveBeenCalledExactlyOnceWith('01');

    watermarks.cancel();
    await run;
  });

  test('forwards watermarks to the changeStreamer, in order, skipping redundant watermarks', async () => {
    const run = monitor.run();

    watermarks.push(backedUp('01', 1000));
    watermarks.push(backedUp('01', 1234)); // duplicate suppressed
    await vi.waitFor(() =>
      expect(trackBackupWatermark).toHaveBeenCalledTimes(1),
    );

    watermarks.push(backedUp('02'));
    watermarks.push(backedUp('03'));
    watermarks.push(backedUp('03', 2345)); // duplicate suppressed
    watermarks.push(backedUp('02')); // ignored earlier watermarks
    await vi.waitFor(() =>
      expect(trackBackupWatermark).toHaveBeenCalledTimes(3),
    );

    expect(trackBackupWatermark.mock.calls).toEqual([['01'], ['02'], ['03']]);

    watermarks.cancel();
    await run;
  });

  test('firstBackupReceived only reflects the first watermark, not later ones', async () => {
    const run = monitor.run();

    watermarks.push(backedUp('01'));
    await monitor.firstBackupReceived();

    watermarks.push(backedUp('02'));
    await vi.waitFor(() =>
      expect(trackBackupWatermark).toHaveBeenCalledTimes(2),
    );
    // Still resolves to the same (void) promise; no error / re-resolution issue.
    await monitor.firstBackupReceived();

    watermarks.cancel();
    await run;
  });

  /**
   * A new backup destination starts empty and fills by re-uploading the local
   * LTX chain, so the first watermarks read back out of it are real but far
   * behind the replica. The gate must wait for coverage, or the task serves
   * against a backup that does not cover it and demotes restoring
   * view-syncers to Postgres catchup.
   */
  describe('coverage gate', () => {
    function makeReplica(stateVersion: string): string {
      const file = path.join(
        tmpdir(),
        `backup-monitor-coverage-${stateVersion}-${Math.random()
          .toString(36)
          .slice(2)}.db`,
      );
      const db = new Database(createSilentLogContext(), file);
      db.exec(/*sql*/ `
        CREATE TABLE "_zero.replicationState" (
          stateVersion TEXT NOT NULL,
          writeTimeMs INTEGER NOT NULL,
          lock INTEGER PRIMARY KEY DEFAULT 1 CHECK (lock=1)
        );
      `);
      db.prepare(
        /*sql*/ `INSERT INTO "_zero.replicationState" (stateVersion, writeTimeMs) VALUES (?, ?)`,
      ).run(stateVersion, 1000);
      db.close();
      return file;
    }

    function monitorFor(replicaFile: string) {
      return new BackupMonitor(
        createSilentLogContext(),
        watermarks,
        changeStreamer,
        replicaFile,
      );
    }

    test('does not resolve on a watermark behind the replica', async () => {
      const monitor = monitorFor(makeReplica('05'));
      const run = monitor.run();
      let resolved = false;
      void monitor.firstBackupReceived().then(() => (resolved = true));

      // Mid-backfill readings: real watermarks, but behind the replica.
      watermarks.push(backedUp('01'));
      watermarks.push(backedUp('03'));
      await vi.waitFor(() =>
        expect(trackBackupWatermark).toHaveBeenCalledTimes(2),
      );

      // Tracked for the purge floor, but the gate is still closed.
      expect(trackBackupWatermark.mock.calls).toEqual([['01'], ['03']]);
      expect(resolved).toBe(false);

      watermarks.push(backedUp('05'));
      await monitor.firstBackupReceived();
      expect(resolved).toBe(true);

      watermarks.cancel();
      await run;
    });

    test('resolves on a watermark past the replica', async () => {
      const monitor = monitorFor(makeReplica('05'));
      const run = monitor.run();

      watermarks.push(backedUp('09'));
      await monitor.firstBackupReceived();

      watermarks.cancel();
      await run;
    });

    test('stays resolved once covered, even if later watermarks are tracked', async () => {
      const monitor = monitorFor(makeReplica('05'));
      const run = monitor.run();

      watermarks.push(backedUp('05'));
      await monitor.firstBackupReceived();

      watermarks.push(backedUp('06'));
      await vi.waitFor(() =>
        expect(trackBackupWatermark).toHaveBeenCalledTimes(2),
      );
      await monitor.firstBackupReceived();

      watermarks.cancel();
      await run;
    });
  });

  test('stop() cancels the watermark source and run() completes', async () => {
    const run = monitor.run();

    watermarks.push(backedUp('01'));
    await vi.waitFor(() =>
      expect(trackBackupWatermark).toHaveBeenCalledTimes(1),
    );

    await monitor.stop();
    await run;
  });

  test('run() completes when the watermark source is canceled without any backups', async () => {
    const run = monitor.run();
    watermarks.cancel();
    await run;

    expect(trackBackupWatermark).not.toHaveBeenCalled();
  });
});
