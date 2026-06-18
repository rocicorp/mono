import EventEmitter from 'node:events';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {inProcChannel, type Worker} from '../../types/processes.ts';
import {
  requestVfsBackupWatermark,
  VfsBackupWatermarkWorkerService,
  type BackupWatermarkSource,
} from './vfs-watermark-worker.ts';

const lc = createSilentLogContext();

describe('litestream/vfs-watermark-worker', () => {
  test('responds to watermark requests', async () => {
    const [parent, child] = inProcChannel();
    const source: BackupWatermarkSource = {
      readWatermark: vi.fn(() => ({
        watermark: '04',
        writeTimeMs: 123,
        txid: '0000000000000004',
        lagSeconds: 2,
        observedAt: new Date(1000),
      })),
      close: vi.fn(),
    };
    const svc = new VfsBackupWatermarkWorkerService(lc, child, source, 30_000);
    const ready = new Promise(resolve => parent.once('message', resolve));
    const run = svc.run();
    expect(await ready).toEqual(['ready', {ready: true}]);

    await expect(requestVfsBackupWatermark(parent, 100)).resolves.toEqual({
      watermark: '04',
      writeTimeMs: 123,
      txid: '0000000000000004',
      lagSeconds: 2,
      observedAt: new Date(1000),
    });

    expect(source.readWatermark).toHaveBeenCalledTimes(1);
    await svc.stop();
    await run;
    expect(source.close).toHaveBeenCalledTimes(1);
  });

  test('sends ready before constructing the watermark source', async () => {
    const [parent, child] = inProcChannel();
    const source: BackupWatermarkSource = {
      readWatermark: vi.fn(() => ({
        watermark: '04',
        writeTimeMs: 123,
        txid: '0000000000000004',
        lagSeconds: 2,
        observedAt: new Date(1000),
      })),
      close: vi.fn(),
    };
    const createSource = vi.fn(() => source);
    const svc = new VfsBackupWatermarkWorkerService(
      lc,
      child,
      createSource,
      30_000,
    );
    const ready = new Promise(resolve => parent.once('message', resolve));
    const run = svc.run();

    expect(await ready).toEqual(['ready', {ready: true}]);
    expect(createSource).not.toHaveBeenCalled();

    await requestVfsBackupWatermark(parent, 100);
    expect(createSource).toHaveBeenCalledTimes(1);

    await svc.stop();
    await run;
    expect(source.close).toHaveBeenCalledTimes(1);
  });

  test('rejects request when source throws', async () => {
    const [parent, child] = inProcChannel();
    const source: BackupWatermarkSource = {
      readWatermark: () => {
        throw new Error('boom');
      },
    };
    const svc = new VfsBackupWatermarkWorkerService(lc, child, source, 30_000);
    const ready = new Promise(resolve => parent.once('message', resolve));
    const run = svc.run();
    await ready;

    await expect(requestVfsBackupWatermark(parent, 100)).rejects.toThrow(
      'boom',
    );

    await svc.stop();
    await run;
  });

  test('times out when no response arrives', async () => {
    const [parent] = inProcChannel();

    await expect(requestVfsBackupWatermark(parent, 1)).rejects.toThrow(
      'timed out waiting 1 ms for backup watermark response',
    );
  });

  test('kills worker on timeout when requested', async () => {
    const worker = Object.assign(new EventEmitter(), {
      send: vi.fn(() => true),
      kill: vi.fn(),
    }) as unknown as Worker;

    await expect(
      requestVfsBackupWatermark(worker, 1, {killOnTimeout: true}),
    ).rejects.toThrow('timed out waiting 1 ms for backup watermark response');

    expect(worker.kill).toHaveBeenCalledWith('SIGKILL');
  });
});
