import EventEmitter from 'node:events';
import {describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {inProcChannel, type Worker} from '../../types/processes.ts';
import {VfsBackupWatermarkWorkerSource} from './vfs-watermark-worker-source.ts';
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
        observedAtMs: 1000,
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
      observedAtMs: 1000,
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
        observedAtMs: 1000,
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

  test('worker source waits for ready and reads watermark', async () => {
    const [parent, child] = inProcChannel();
    const source: BackupWatermarkSource = {
      readWatermark: vi.fn(() => ({
        watermark: '04',
        writeTimeMs: 123,
        txid: '0000000000000004',
        lagSeconds: 2,
        observedAtMs: 1000,
      })),
      close: vi.fn(),
    };
    const workerFactory = vi.fn(() => parent);
    const workerSource = new VfsBackupWatermarkWorkerSource(
      lc,
      workerFactory,
      100,
    );

    const read = workerSource.readWatermark();
    const svc = new VfsBackupWatermarkWorkerService(lc, child, source, 30_000);
    const run = svc.run();

    await expect(read).resolves.toEqual({
      watermark: '04',
      writeTimeMs: 123,
      txid: '0000000000000004',
      lagSeconds: 2,
      observedAtMs: 1000,
    });
    expect(workerFactory).toHaveBeenCalledTimes(1);
    expect(source.readWatermark).toHaveBeenCalledTimes(1);

    workerSource.close();
    await svc.stop();
    await run;
  });

  test('worker source recreates worker after timeout', async () => {
    const workers: Worker[] = [];
    const workerFactory = vi.fn(() => {
      const worker = Object.assign(new EventEmitter(), {
        send: vi.fn(() => true),
        kill: vi.fn(),
      }) as unknown as Worker;
      workers.push(worker);
      queueMicrotask(() => worker.emit('message', ['ready', {ready: true}]));
      return worker;
    });
    const source = new VfsBackupWatermarkWorkerSource(lc, workerFactory, 1);

    await expect(source.readWatermark()).rejects.toThrow(
      'timed out waiting 1 ms for backup watermark response',
    );
    expect(workers[0].kill).toHaveBeenCalledWith('SIGKILL');

    await expect(source.readWatermark()).rejects.toThrow(
      'timed out waiting 1 ms for backup watermark response',
    );
    expect(workerFactory).toHaveBeenCalledTimes(2);

    source.close();
  });
});
