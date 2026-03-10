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
 * The main implementation uses a real worker_thread for non-blocking writes.
 * A synchronous in-process implementation is used for tests.
 */
export interface WriteWorkerClient {
  getSubscriptionState(): Promise<SubscriptionState>;
  processMessage(downstream: ChangeStreamData): Promise<CommitResult | null>;
  abort(): void;
  stop(): Promise<void>;
  onError(handler: ErrorHandler): void;
}

// Wire protocol types — errors are passed directly via structured clone
export type Request = {id: number; method: string; args: unknown[]};
export type Response = {id: number; result?: unknown; error?: unknown};
export type PushError = {pushError: Error};

export function applyPragmas(db: Database, pragmas: PragmaConfig) {
  db.pragma(`busy_timeout = ${pragmas.busyTimeout}`);
  db.pragma(`analysis_limit = ${pragmas.analysisLimit}`);
  if (pragmas.walAutocheckpoint !== undefined) {
    db.pragma(`wal_autocheckpoint = ${pragmas.walAutocheckpoint}`);
  }
}

/**
 * Production implementation that delegates SQLite writes to a worker_thread,
 * keeping the main event loop free for WebSocket heartbeats and IPC.
 */
export class ThreadWriteWorkerClient implements WriteWorkerClient {
  readonly #worker: Worker;
  readonly #pending = new Map<
    number,
    {resolve: (v: unknown) => void; reject: (e: Error) => void}
  >();
  #nextID = 0;
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
      const r = this.#pending.get(msg.id);
      if (!r) return;
      this.#pending.delete(msg.id);
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
    for (const [, r] of this.#pending) {
      r.reject(err);
    }
    this.#pending.clear();
  }

  #call(method: string, args: unknown[]): Promise<unknown> {
    const id = ++this.#nextID;
    const {promise, resolve, reject} = resolver<unknown>();
    this.#pending.set(id, {resolve, reject});
    this.#worker.postMessage({id, method, args} satisfies Request);
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
    void this.#call('abort', []).catch(() => {});
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
