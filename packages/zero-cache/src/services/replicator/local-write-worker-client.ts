import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {createLogContext} from '../../server/logging.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {
  ChangeProcessor,
  type ChangeProcessorMode,
  type CommitResult,
} from './change-processor.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  applyPragmas,
  type PragmaConfig,
  type WriteWorkerClient,
} from './write-worker-client.ts';

type ErrorHandler = (err: Error) => void;

// Serving replicator workers are already off the browser/query path. Applying
// serving writes directly inside that worker avoids the extra structured-clone
// and worker-thread hop that dominated row-heavy v6 RM -> VS benchmarks.
export class LocalWriteWorkerClient implements WriteWorkerClient {
  #db: Database | undefined;
  #runner: StatementRunner | undefined;
  #processor: ChangeProcessor | undefined;
  #lc: LogContext | undefined;
  #mode: ChangeProcessorMode | undefined;
  #errorHandler: ErrorHandler = () => {};
  #processorFailure: Error | undefined;

  init(
    dbPath: string,
    mode: ChangeProcessorMode,
    pragmas: PragmaConfig,
    logConfig: LogConfig,
  ): Promise<void> {
    this.#lc = createLogContext({log: logConfig}, 'write-worker');
    this.#db = new Database(this.#lc, dbPath);
    applyPragmas(this.#db, pragmas);
    this.#runner = new StatementRunner(this.#db);
    this.#mode = mode;
    this.#processor = new ChangeProcessor(this.#runner, mode, (_lc, err) => {
      this.#processorFailure = ensureError(err);
    });
    return Promise.resolve();
  }

  getSubscriptionState() {
    assert(this.#runner, 'local write worker not initialized');
    return Promise.resolve(getSubscriptionState(this.#runner));
  }

  processMessage(downstream: ChangeStreamData) {
    assert(this.#processor, 'local write worker not initialized');
    assert(this.#lc, 'local write worker not initialized');
    const processor = this.#processor;
    const lc = this.#lc;
    return this.#processWithFailureCapture(() =>
      processor.processMessage(lc, downstream),
    );
  }

  processMessages(
    downstreams: readonly ChangeStreamData[],
  ): Promise<CommitResult | readonly CommitResult[] | null> {
    assert(this.#processor, 'local write worker not initialized');
    assert(this.#lc, 'local write worker not initialized');
    const processor = this.#processor;
    const lc = this.#lc;

    return this.#processWithFailureCapture(() => {
      const results: CommitResult[] = [];
      for (const downstream of downstreams) {
        const result = processor.processMessage(lc, downstream);
        if (result) {
          results.push(result);
        }
      }
      if (results.length === 0) {
        return null;
      }
      return results.length === 1 ? results[0] : results;
    });
  }

  abort(): void {
    assert(this.#processor, 'local write worker not initialized');
    assert(this.#lc, 'local write worker not initialized');
    assert(this.#runner, 'local write worker not initialized');
    assert(this.#mode, 'local write worker not initialized');

    // Abort discards the ChangeProcessor's in-flight transaction/failure state,
    // but keeps the already-open SQLite connection. That mirrors the thread
    // worker contract without paying a reconnect cost on every stream retry.
    this.#processor.abort(this.#lc);
    this.#processor = new ChangeProcessor(
      this.#runner,
      this.#mode,
      (_lc, err) => {
        this.#processorFailure = ensureError(err);
      },
    );
  }

  stop(): Promise<void> {
    this.#db?.close();
    this.#db = undefined;
    this.#runner = undefined;
    this.#processor = undefined;
    this.#mode = undefined;
    this.#processorFailure = undefined;
    return Promise.resolve();
  }

  onError(handler: ErrorHandler): void {
    this.#errorHandler = handler;
  }

  #processWithFailureCapture<T>(process: () => T): Promise<T> {
    this.#processorFailure = undefined;

    let result: T;
    try {
      result = process();
    } catch (err) {
      this.#processorFailure = undefined;
      return Promise.reject(ensureError(err));
    }

    const failure = this.#processorFailure;
    this.#processorFailure = undefined;
    if (failure) {
      this.#errorHandler(failure);
      return Promise.reject(failure);
    }

    return Promise.resolve(result);
  }
}

function ensureError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }
  if (typeof err === 'string') {
    return new Error(err);
  }
  return new Error(JSON.stringify(err));
}
