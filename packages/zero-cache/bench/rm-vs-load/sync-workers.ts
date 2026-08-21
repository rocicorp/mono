import {Worker} from 'node:worker_threads';
import type {SyncWorkerData, SyncWorkerStats} from './types.ts';

export type SyncWorkerGroupStats = SyncWorkerStats & {
  readonly count: number;
};

export type SharedReplicaSyncWorkersOptions = {
  readonly count: number;
  readonly sqlitePath: string;
  readonly readDelayMs: number;
  readonly workerUrl?: URL | undefined;
};

export class SharedReplicaSyncWorkers {
  readonly #workers: SyncWorkerHandle[];

  private constructor(workers: SyncWorkerHandle[]) {
    this.#workers = workers;
  }

  static async start({
    count,
    sqlitePath,
    readDelayMs,
    workerUrl = new URL('./sync-worker-reader.ts', import.meta.url),
  }: SharedReplicaSyncWorkersOptions): Promise<SharedReplicaSyncWorkers> {
    const workers = Array.from(
      {length: count},
      (_, index) =>
        new SyncWorkerHandle(workerUrl, {
          id: `sync-worker-${index}`,
          sqlitePath,
          readDelayMs,
        }),
    );
    await Promise.all(workers.map(worker => worker.ready));
    return new SharedReplicaSyncWorkers(workers);
  }

  stats(): SyncWorkerGroupStats {
    const workerStats = this.#workers.map(worker => worker.stats());
    return {
      count: this.#workers.length,
      reads: sum(workerStats.map(stats => stats.reads)),
      totalReadMs: sum(workerStats.map(stats => stats.totalReadMs)),
      maxReadMs: Math.max(0, ...workerStats.map(stats => stats.maxReadMs)),
      errors: sum(workerStats.map(stats => stats.errors)),
    };
  }

  async stop(): Promise<void> {
    await Promise.all(this.#workers.map(worker => worker.stop()));
  }
}

class SyncWorkerHandle {
  readonly #worker: Worker;
  readonly ready: Promise<void>;
  readonly #done: Promise<void>;
  #stats = emptyStats();
  #stopped = false;

  constructor(workerUrl: URL, data: SyncWorkerData) {
    this.#worker = new Worker(workerUrl, {workerData: data});

    let resolveReady!: () => void;
    let rejectReady!: (err: Error) => void;
    this.ready = new Promise<void>((resolve, reject) => {
      resolveReady = resolve;
      rejectReady = reject;
    });

    let resolveDone!: () => void;
    let rejectDone!: (err: Error) => void;
    this.#done = new Promise<void>((resolve, reject) => {
      resolveDone = resolve;
      rejectDone = reject;
    });
    void this.#done.catch(() => {});

    const fail = (err: Error) => {
      rejectReady(err);
      rejectDone(err);
    };

    this.#worker.on('message', msg => {
      if (typeof msg !== 'object' || msg === null || !('type' in msg)) {
        return;
      }
      switch (msg.type) {
        case 'ready':
          resolveReady();
          break;
        case 'stats':
          this.#stats = msg.stats as SyncWorkerStats;
          break;
        case 'done':
          this.#stats = msg.stats as SyncWorkerStats;
          resolveDone();
          break;
        case 'error': {
          const error = msg.error as {message?: unknown; stack?: unknown};
          const message =
            typeof error.message === 'string'
              ? error.message
              : 'sync worker reader error';
          const err = new Error(message);
          if (typeof error.stack === 'string') {
            err.stack = error.stack;
          }
          fail(err);
          break;
        }
      }
    });
    this.#worker.on('error', fail);
    this.#worker.on('exit', code => {
      if (!this.#stopped && code !== 0) {
        fail(new Error(`sync worker reader exited with code ${code}`));
      }
    });
  }

  stats(): SyncWorkerStats {
    this.#worker.postMessage({type: 'stats'});
    return this.#stats;
  }

  async stop(): Promise<void> {
    this.#stopped = true;
    this.#worker.postMessage({type: 'stop'});
    await this.#done.catch(() => {});
    await this.#worker.terminate();
  }
}

function emptyStats(): SyncWorkerStats {
  return {
    reads: 0,
    totalReadMs: 0,
    maxReadMs: 0,
    errors: 0,
  };
}

function sum(values: readonly number[]): number {
  return values.reduce((total, value) => total + value, 0);
}
