import {Worker} from 'node:worker_threads';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {resolver} from '../../../../shared/src/resolver.ts';
import {SERVING_CHECKPOINT_WORKER_URL} from '../../server/worker-urls.ts';
import {deserializeError, type SerializedError} from './write-worker-client.ts';

export type ServingCheckpointWorkerData = {
  dbPath: string;
  logConfig: LogConfig;
};

export type ServingCheckpointWorkerMessage =
  | {ready: true}
  | {error: SerializedError};

/**
 * Checkpoints the serving replica from a dedicated thread and connection.
 *
 * With `wal_autocheckpoint` disabled, the write worker's COMMIT performs no
 * checkpoint, and so no disk syncs (in WAL mode with `synchronous=NORMAL`,
 * the wal and db files are only synced around checkpoints). The writer still
 * switches wal files once the current one reaches the wal2 size threshold;
 * this thread checkpoints the other one in response to commits.
 *
 * A checkpoint is requested after every commit. Requests queue behind a
 * running checkpoint and are cheap once it completes: a wal2 checkpoint
 * copies a whole wal file or nothing, so a request only does work after the
 * writer has switched files again.
 */
export class ServingCheckpointer {
  readonly #worker: Worker;
  readonly #ready = resolver<void, Error>();

  constructor(
    dbPath: string,
    logConfig: LogConfig,
    onError: (err: Error) => void,
  ) {
    const fail = (err: Error) => {
      this.#ready.reject(err);
      onError(err);
    };
    this.#worker = new Worker(SERVING_CHECKPOINT_WORKER_URL, {
      workerData: {dbPath, logConfig} satisfies ServingCheckpointWorkerData,
    });
    this.#worker.on('message', (msg: ServingCheckpointWorkerMessage) =>
      'ready' in msg
        ? this.#ready.resolve()
        : fail(deserializeError(msg.error)),
    );
    this.#worker.on('error', fail);
  }

  /** Resolves once the worker has opened its connection to the replica. */
  ready(): Promise<void> {
    return this.#ready.promise;
  }

  requestCheckpoint(): void {
    this.#worker.postMessage('checkpoint');
  }

  /**
   * Stops the worker. A running checkpoint completes first, since the
   * synchronous SQLite call cannot be interrupted; queued requests are dropped.
   */
  close(): Promise<unknown> {
    return this.#worker.terminate();
  }
}
