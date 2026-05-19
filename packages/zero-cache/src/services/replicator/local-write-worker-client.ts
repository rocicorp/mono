import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import {createLogContext} from '../../server/logging.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
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
    this.#processor = new ChangeProcessor(this.#runner, mode, (_lc, err) =>
      this.#errorHandler(ensureError(err)),
    );
    return Promise.resolve();
  }

  getSubscriptionState() {
    return Promise.resolve(getSubscriptionState(this.#mustRunner()));
  }

  processMessage(downstream: ChangeStreamData) {
    return Promise.resolve(
      this.#mustProcessor().processMessage(this.#mustLogContext(), downstream),
    );
  }

  processMessages(downstreams: readonly ChangeStreamData[]) {
    return Promise.resolve(
      this.#mustProcessor().processMessages(
        this.#mustLogContext(),
        downstreams,
      ),
    );
  }

  abort(): void {
    // Abort discards the ChangeProcessor's in-flight transaction/failure state,
    // but keeps the already-open SQLite connection. That mirrors the thread
    // worker contract without paying a reconnect cost on every stream retry.
    this.#mustProcessor().abort(this.#mustLogContext());
    this.#processor = new ChangeProcessor(
      this.#mustRunner(),
      this.#mustMode(),
      (_lc, err) => this.#errorHandler(ensureError(err)),
    );
  }

  stop(): Promise<void> {
    this.#db?.close();
    this.#db = undefined;
    this.#runner = undefined;
    this.#processor = undefined;
    this.#mode = undefined;
    return Promise.resolve();
  }

  onError(handler: ErrorHandler): void {
    this.#errorHandler = handler;
  }

  #mustLogContext() {
    assert(this.#lc, 'local write worker not initialized');
    return this.#lc;
  }

  #mustRunner() {
    assert(this.#runner, 'local write worker not initialized');
    return this.#runner;
  }

  #mustProcessor() {
    assert(this.#processor, 'local write worker not initialized');
    return this.#processor;
  }

  #mustMode() {
    assert(this.#mode, 'local write worker not initialized');
    return this.#mode;
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
