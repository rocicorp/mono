import {Worker} from 'node:worker_threads';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {WRITE_WORKER_URL} from '../../server/worker-urls.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import type {ChangeProcessorMode, CommitResult} from './change-processor.ts';
import type {SubscriptionState} from './schema/replication-state.ts';

export type PragmaConfig = {
  busyTimeout: number;
  analysisLimit: number;
  walAutocheckpoint?: number | undefined;
};

/**
 * Enables write-path checkpoint backpressure in the write worker. At each
 * commit boundary, if the WAL has grown to at least `checkpointThresholdPages`,
 * the worker forces an immediate litestream sync (sealing the WAL to LTX and
 * running litestream's checkpoint).
 *
 * The optional `walMaxPages` is an emergency break that pauses until the WAL drains,
 * rather than letting it exceed available disk space. This replaces litestream's
 * analogous `truncate-page-n` emergency break, which is more disruptive because of
 * https://github.com/benbjohnson/litestream/issues/1332. Note, however, this should
 * never been needed if non-initial snapshots are disabled (which is the default).
 *
 * Set only for the backup replicator when replicating with litestream v5; `null`
 * disables the feature entirely (no per-commit overhead).
 */
export type ForceCheckpointConfig = {
  checkpointThresholdPages: number;
  maxWalPages?: number | undefined;
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
  init: [
    string,
    ChangeProcessorMode,
    PragmaConfig,
    LogConfig,
    ForceCheckpointConfig | null,
  ];
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
    pragmas: PragmaConfig,
    logConfig: LogConfig,
    checkpoint: ForceCheckpointConfig | null = null,
  ): Promise<void> {
    return this.#call('init', [dbPath, mode, pragmas, logConfig, checkpoint]);
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
