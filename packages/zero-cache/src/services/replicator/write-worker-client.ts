import {Worker} from 'node:worker_threads';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {WRITE_WORKER_URL} from '../../server/worker-urls.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ChangeProcessorMode, CommitResult} from './change-processor.ts';
import type {SubscriptionState} from './schema/replication-state.ts';

export type SerializedChangeStreamData = {
  data: ChangeStreamData;
  json: string;
};

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
  processMessage(
    downstream: SerializedChangeStreamData,
  ): Promise<CommitResult | null>;
  abort(): void;
  stop(): Promise<void>;
  onError(handler: ErrorHandler): void;
}

export type SerializedError = {
  name: string;
  message: string;
  stack?: string | undefined;
  cause?: SerializedError | string | undefined;
  details?: Record<string, unknown> | undefined;
};

export function serializeError(err: unknown): SerializedError {
  if (!(err instanceof Error)) {
    return {
      name: 'Error',
      message: String(err),
      details: err && typeof err === 'object' ? {...err} : undefined,
    };
  }

  // Error fields such as message, stack, and some native error details are
  // non-enumerable, so JSON.stringify(err) would usually return "{}".
  const details = Object.fromEntries(
    Object.getOwnPropertyNames(err)
      .filter(key => !['name', 'message', 'stack', 'cause'].includes(key))
      .map(key => [key, (err as unknown as Record<string, unknown>)[key]]),
  );
  const cause =
    err.cause instanceof Error
      ? serializeError(err.cause)
      : err.cause === undefined
        ? undefined
        : String(err.cause);

  return {
    name: err.name,
    message: err.message,
    stack: err.stack,
    cause,
    details: Object.keys(details).length ? details : undefined,
  };
}

export function deserializeError(serialized: SerializedError): Error {
  const err = new Error(serialized.message);
  err.name = serialized.name;
  if (serialized.stack !== undefined) {
    err.stack = serialized.stack;
  }
  if (serialized.cause !== undefined) {
    err.cause =
      typeof serialized.cause === 'string'
        ? serialized.cause
        : deserializeError(serialized.cause);
  }
  if (serialized.details) {
    Object.assign(err, serialized.details);
  }
  return err;
}

// Wire protocol types.
export type ArgsMap = {
  init: [string, ChangeProcessorMode, boolean, PragmaConfig, LogConfig];
  getSubscriptionState: [];
  processMessage: [SerializedChangeStreamData];
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
  | {method: M; error: SerializedError; result?: undefined};

export type WriteError = {writeError: SerializedError};

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

  constructor() {
    this.#worker = new Worker(WRITE_WORKER_URL);

    this.#worker.on('message', (msg: Response | WriteError) => {
      if ('writeError' in msg) {
        const error = deserializeError(msg.writeError);
        this.#rejectAll(error);
        this.#errorHandler(error);
        return;
      }
      const r = this.#pending;
      if (!r) return; // stale abort response
      this.#pending = null;
      if (msg.error !== undefined) {
        r.reject(deserializeError(msg.error));
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
    logsChangeStream: boolean,
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ): Promise<void> {
    return this.#call('init', [
      dbPath,
      mode,
      logsChangeStream,
      pragmas,
      logConfig,
    ]);
  }

  getSubscriptionState(): Promise<SubscriptionState> {
    return this.#call('getSubscriptionState', []);
  }

  processMessage(
    downstream: SerializedChangeStreamData,
  ): Promise<CommitResult | null> {
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
