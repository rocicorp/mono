import type {LogContext} from '@rocicorp/logger';
import type {Worker} from '../../types/processes.ts';
import type {VfsBackupWatermark} from './vfs-watermark-reader.ts';
import {
  BackupWatermarkTimeoutError,
  requestVfsBackupWatermark,
} from './vfs-watermark-worker.ts';

export type VfsBackupWatermarkWorkerFactory = () => Worker;

export class VfsBackupWatermarkWorkerSource {
  readonly #lc: LogContext;
  readonly #workerFactory: VfsBackupWatermarkWorkerFactory;
  readonly #timeoutMs: number;

  #worker: Worker | undefined;
  #ready: Promise<Worker> | undefined;

  constructor(
    lc: LogContext,
    workerFactory: VfsBackupWatermarkWorkerFactory,
    timeoutMs: number,
  ) {
    this.#lc = lc.withContext(
      'component',
      'vfs-backup-watermark-worker-source',
    );
    this.#workerFactory = workerFactory;
    this.#timeoutMs = timeoutMs;
  }

  async readWatermark(): Promise<VfsBackupWatermark> {
    const worker = await this.#getWorker();
    try {
      return await requestVfsBackupWatermark(worker, this.#timeoutMs, {
        killOnTimeout: true,
      });
    } catch (e) {
      if (e instanceof BackupWatermarkTimeoutError) {
        this.#clearWorker(worker);
      }
      throw e;
    }
  }

  close(): void {
    this.#worker?.kill('SIGTERM');
    this.#worker = undefined;
    this.#ready = undefined;
  }

  #getWorker(): Promise<Worker> {
    if (this.#ready) {
      return this.#ready;
    }

    const worker = this.#workerFactory();
    this.#worker = worker;
    this.#ready = new Promise((resolve, reject) => {
      const cleanup = () => {
        worker.off('message', onMessage);
        worker.off('error', onError);
        worker.off('close', onClose);
      };
      const fail = (e: unknown) => {
        cleanup();
        this.#clearWorker(worker);
        reject(e);
      };
      const onMessage = (data: unknown) => {
        if (!isReadyMessage(data)) {
          return;
        }
        cleanup();
        resolve(worker);
      };
      const onError = (e: unknown) => fail(e);
      const onClose = (code: unknown, signal: unknown) =>
        fail(
          new Error(
            `backup watermark reader worker exited before ready ` +
              `(code=${String(code)}, signal=${String(signal)})`,
          ),
        );

      worker.on('message', onMessage);
      worker.once('error', onError);
      worker.once('close', onClose);
    });

    worker.once('close', () => this.#clearWorker(worker));
    worker.once('error', e => {
      this.#lc.warn?.(`backup watermark reader worker failed`, e);
      this.#clearWorker(worker);
    });

    return this.#ready;
  }

  #clearWorker(worker: Worker): void {
    if (this.#worker === worker) {
      this.#worker = undefined;
      this.#ready = undefined;
    }
  }
}

function isReadyMessage(data: unknown): boolean {
  return (
    Array.isArray(data) &&
    data.length === 2 &&
    data[0] === 'ready' &&
    typeof data[1] === 'object' &&
    data[1] !== null &&
    'ready' in data[1] &&
    data[1].ready === true
  );
}
