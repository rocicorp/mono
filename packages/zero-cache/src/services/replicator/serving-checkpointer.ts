import {EventEmitter} from 'node:events';
import {Worker} from 'node:worker_threads';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {resolver} from '../../../../shared/src/resolver.ts';
import {SERVING_CHECKPOINT_WORKER_URL} from '../../server/worker-urls.ts';
import {deserializeError, type SerializedError} from './write-worker-client.ts';

export type CheckpointArgs = {dbPath: string; logConfig: LogConfig};
export type CheckpointResult = {
  durationMs: number;
  busy: number;
  log: number;
  checkpointed: number;
};
export type CheckpointRequest = 'checkpoint' | 'stop';
export type CheckpointResponse =
  | {type: 'ready'}
  | {type: 'checkpoint'; result: CheckpointResult}
  | {type: 'error'; error: SerializedError};

export class ServingCheckpointer extends EventEmitter<{
  checkpoint: [CheckpointResult];
  error: [Error];
}> {
  readonly #worker: Worker;
  readonly #ready = resolver<void, Error>();
  readonly #exited = resolver();
  #pending = false;
  #dirty = false;
  #closing = false;
  #failure: Error | undefined;

  constructor(
    dbPath: string,
    logConfig: LogConfig,
    onError: (error: Error) => void,
  ) {
    super();
    this.on('error', onError);
    this.#worker = new Worker(SERVING_CHECKPOINT_WORKER_URL, {
      workerData: {dbPath, logConfig} satisfies CheckpointArgs,
    });
    this.#worker.on('error', (error: Error) => this.#fail(error));
    this.#worker.on('exit', code => {
      if (!this.#failure && (code !== 0 || !this.#closing)) {
        this.#fail(
          new Error(`Serving checkpoint worker exited with code ${code}`),
        );
      }
      this.#exited.resolve();
    });
    this.#worker.on('message', (message: CheckpointResponse) => {
      switch (message.type) {
        case 'ready':
          this.#ready.resolve();
          break;
        case 'error':
          this.#fail(deserializeError(message.error));
          break;
        case 'checkpoint':
          this.#pending = false;
          this.emit('checkpoint', message.result);
          if (this.#closing) {
            this.#send('stop');
          } else if (this.#dirty) {
            this.#dirty = false;
            this.request();
          }
          break;
      }
    });
  }

  get ready(): Promise<void> {
    return this.#ready.promise;
  }

  #send(message: CheckpointRequest) {
    this.#worker.postMessage(message);
  }

  #fail(error: Error) {
    if (this.#failure) return;
    this.#failure = error;
    this.#ready.reject(error);
    this.emit('error', error);
  }

  request(): void {
    if (this.#closing || this.#failure) return;
    if (this.#pending) {
      this.#dirty = true;
      return;
    }
    this.#pending = true;
    this.#send('checkpoint');
  }

  async close(): Promise<void> {
    this.#closing = true;
    if (!this.#pending) this.#send('stop');
    await this.#exited.promise;
    if (this.#failure) throw this.#failure;
  }
}
