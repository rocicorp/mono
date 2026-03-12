import {assert} from '../../../../shared/src/asserts.ts';
import {resolver} from '@rocicorp/resolver';
import {Worker} from 'node:worker_threads';
import type {Database} from '../../../../zqlite/src/db.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
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
export type Method = 'init' | 'getSubscriptionState' | 'processMessage' | 'abort' | 'stop';
export type Request = {method: Method; args: unknown[]};
export type Response = {result?: unknown; error?: unknown};
export type PushError = {pushError: Error};

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
  #pending: {resolve: (v: unknown) => void; reject: (e: Error) => void} | null =
    null;
  #errorHandler: ErrorHandler = () => {};
  #terminated = false;

  constructor(workerUrl: URL) {
    this.#worker = new Worker(workerUrl);

    this.#worker.on('message', (msg: Response | PushError) => {
      if ('pushError' in msg) {
        const error =
          msg.pushError instanceof Error
            ? msg.pushError
            : new Error(String(msg.pushError));
        this.#rejectAll(error);
        this.#errorHandler(error);
        return;
      }
      const r = this.#pending;
      if (!r) return; // stale abort response
      this.#pending = null;
      if (msg.error) {
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

  #call(method: Method, args: unknown[]): Promise<unknown> {
    assert(this.#pending === null, `concurrent call: ${method}`);
    const {promise, resolve, reject} = resolver<unknown>();
    this.#pending = {resolve, reject};
    this.#worker.postMessage({method, args} satisfies Request);
    return promise;
  }

  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
  ): Promise<void> {
    return this.#call('init', [dbPath, mode, pragmas]) as Promise<void>;
  }

  getSubscriptionState(): Promise<SubscriptionState> {
    return this.#call('getSubscriptionState', []) as Promise<SubscriptionState>;
  }

  processMessage(downstream: ChangeStreamData): Promise<CommitResult | null> {
    return this.#call('processMessage', [
      downstream,
    ]) as Promise<CommitResult | null>;
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
