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
  | {checkpointed: true}
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
 * A checkpoint is requested after every commit, with at most one request
 * outstanding: commits during a checkpoint leave a single follow-up request,
 * so a stalled checkpoint cannot queue unbounded work. The follow-up is cheap
 * unless the writer has switched files again, since a wal2 checkpoint copies a
 * whole wal file or nothing.
 */
export class ServingCheckpointer {
  readonly #worker: Worker;
  readonly #ready = resolver<void, Error>();
  #inflight = false;
  #requested = false;

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
    this.#worker.on('message', (msg: ServingCheckpointWorkerMessage) => {
      if ('ready' in msg) {
        this.#ready.resolve();
      } else if ('checkpointed' in msg) {
        this.#inflight = false;
        if (this.#requested) {
          this.#requested = false;
          this.requestCheckpoint();
        }
      } else {
        fail(deserializeError(msg.error));
      }
    });
    this.#worker.on('error', fail);
  }

  /** Resolves once the worker has opened its connection to the replica. */
  ready(): Promise<void> {
    return this.#ready.promise;
  }

  requestCheckpoint(): void {
    if (this.#inflight) {
      this.#requested = true;
      return;
    }
    this.#inflight = true;
    this.#worker.postMessage('checkpoint');
  }

  /**
   * Stops the worker. A running checkpoint completes first, since the
   * synchronous SQLite call cannot be interrupted; a pending request is dropped.
   */
  close(): Promise<unknown> {
    return this.#worker.terminate();
  }
}
