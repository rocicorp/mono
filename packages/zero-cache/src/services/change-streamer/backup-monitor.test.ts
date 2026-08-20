import {beforeEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
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
