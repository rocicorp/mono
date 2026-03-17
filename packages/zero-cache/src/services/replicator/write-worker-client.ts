import {assert} from '../../../../shared/src/asserts.ts';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {Worker} from 'node:worker_threads';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import type {ChangeProcessorMode, CommitResult} from './change-processor.ts';
import type {SubscriptionState} from './schema/replication-state.ts';

export type PragmaConfig = {
  busyTimeout: number;
  analysisLimit: number;
  walAutocheckpoint?: number | undefined;
};

type ErrorHandler = (err: Error) => void;

/**
 * Interface for a write worker that processes replication messages.
 */
export interface WriteWorkerClient {
  getSubscriptionState(): Promise<SubscriptionState>;
  processMessage(downstream: ChangeStreamData): Promise<CommitResult | null>;
  abort(): void;
  stop(): Promise<void>;
  onError(handler: ErrorHandler): void;
}

// Wire protocol types — errors are passed directly via structured clone
export type ArgsMap = {
  init: [string, ChangeProcessorMode, PragmaConfig, LogConfig];
  getSubscriptionState: [];
  processMessage: [ChangeStreamData];
  abort: [];
  stop: [];
};

export type Method = keyof ArgsMap;

export type Request<M extends Method = Method> = {method: M; args: ArgsMap[M]};

export type ResultMap = {
  init: void;
  getSubscriptionState: SubscriptionState;
  processMessage: CommitResult | null;
  abort: void;
  stop: void;
};

export type Response<M extends Method = Method> =
  | {method: M; result: ResultMap[M]; error?: undefined}
  | {method: M; error: unknown; result?: undefined};

export type WriteError = {writeError: Error};

export function applyPragmas(db: Database, pragmas: PragmaConfig) {
  db.pragma(`busy_timeout = ${pragmas.busyTimeout}`);
  db.pragma(`analysis_limit = ${pragmas.analysisLimit}`);
  if (pragmas.walAutocheckpoint !== undefined) {
    db.pragma(`wal_autocheckpoint = ${pragmas.walAutocheckpoint}`);
  }
}

/**
 * Delegates SQLite writes to a worker_thread,
 * keeping the main event loop free for WebSocket heartbeats and IPC.
 */
export class ThreadWriteWorkerClient implements WriteWorkerClient {
  readonly #worker: Worker;
  #pending: Resolver<unknown, Error> | null = null;
  #errorHandler: ErrorHandler = () => {};
  #terminated = false;

  constructor(workerUrl: URL) {
    this.#worker = new Worker(workerUrl);

    this.#worker.on('message', (msg: Response | WriteError) => {
      if ('writeError' in msg) {
        const error =
          msg.writeError instanceof Error
            ? msg.writeError
            : new Error(String(msg.writeError));
        this.#rejectAll(error);
        this.#errorHandler(error);
        return;
      }
      const r = this.#pending;
      if (!r) return; // stale abort response
      this.#pending = null;
      if (msg.error !== undefined) {
        r.reject(
          msg.error instanceof Error ? msg.error : new Error(String(msg.error)),
        );
      } else {
        r.resolve(msg.result);
      }
    });

    this.#worker.on('error', (err: Error) => {
      this.#rejectAll(err);
      this.#errorHandler(err);
    });

    this.#worker.on('exit', (code: number) => {
      this.#terminated = true;
      if (code !== 0) {
        const err = new Error(`Worker exited with code ${code}`);
        this.#rejectAll(err);
        this.#errorHandler(err);
      }
    });
  }

  #rejectAll(err: Error) {
    const r = this.#pending;
    if (r) {
      this.#pending = null;
      r.reject(err);
    }
  }

  #call<M extends Method>(method: M, args: ArgsMap[M]): Promise<ResultMap[M]> {
    assert(this.#pending === null, `concurrent call: ${method}`);
    const r = resolver<ResultMap[M]>();
    this.#pending = r as Resolver<unknown, Error>;
    this.#worker.postMessage({method, args} satisfies Request);
    return r.promise;
  }

  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ): Promise<void> {
    return this.#call('init', [dbPath, mode, pragmas, logConfig]);
  }

  getSubscriptionState(): Promise<SubscriptionState> {
    return this.#call('getSubscriptionState', []);
  }

  processMessage(downstream: ChangeStreamData): Promise<CommitResult | null> {
    return this.#call('processMessage', [downstream]);
  }

  abort(): void {
    if (!this.#terminated) {
      this.#worker.postMessage({method: 'abort', args: []} satisfies Request);
    }
  }

  async stop(): Promise<void> {
    await this.#call('stop', []);
    if (!this.#terminated) {
      await this.#worker.terminate();
    }
  }

  onError(handler: ErrorHandler): void {
    this.#errorHandler = handler;
  }
}
