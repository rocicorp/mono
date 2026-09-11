import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {deleteLiteDB} from '../../db/delete-lite-db.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
  registerSQLiteCorruptionDiagnosticTarget,
} from '../../db/sqlite-corruption.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {LitestreamCheckpointer} from '../litestream/litestream-checkpointer.ts';
import type {LitestreamSyncClient} from '../litestream/litestream-controller.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
import {readBackfillDeclarations} from './schema/backfilling.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  applyPragmas,
  serializeError,
  type ArgsMap,
  type ForceCheckpointConfig,
  type Method,
  type PragmaConfig,
  type Request,
  type Response,
  type ResultMap,
  type WriteError,
} from './write-worker-client.ts';

export type WriteWorkerAPI = {
  [M in Method]: (...args: ArgsMap[M]) => ResultMap[M] | Promise<ResultMap[M]>;
};

/**
 * What the write worker takes from whatever hosts it: a worker thread in
 * production (`write-worker.ts`), or the calling thread.
 */
export type WriteWorkerHost = {
  /** Builds the worker's LogContext when it is initialized. */
  createLogContext(logConfig: LogConfig): LogContext;

  /** Connects to litestream, for write-path checkpoint backpressure. */
  createLitestreamClient(
    lc: LogContext,
    replicaFile: string,
  ): LitestreamSyncClient;

  /**
   * Reports a failure of the ChangeProcessor, which rejects the client's
   * pending request and reaches its error handler.
   */
  postWriteError(error: WriteError): void;
};

export function createWriteWorkerAPI(host: WriteWorkerHost): WriteWorkerAPI {
  let db: Database | undefined;
  let runner: StatementRunner | undefined;
  let processor: ChangeProcessor | undefined;
  let mode: ChangeProcessorMode | undefined;
  let lc: LogContext | undefined;
  let replicaDbPath: string | undefined;
  let unregisterCorruptionDiagnosticTargets: (() => void)[] = [];

  // Set when write-path checkpoint backpressure is enabled (backup replicator
  // on litestream v5). `undefined` disables the feature.
  let checkpointerConfig: ForceCheckpointConfig | undefined;
  let checkpointer: LitestreamCheckpointer | undefined;

  function unregisterCorruptionDiagnostics() {
    unregisterCorruptionDiagnosticTargets.forEach(unregister => unregister());
    unregisterCorruptionDiagnosticTargets = [];
  }

  function handleCorruptedDb(err: unknown) {
    if (!lc || !replicaDbPath || !isSQLiteCorruption(err)) {
      return;
    }
    logSQLiteCorruptionDiagnostics(lc, 'write-worker', replicaDbPath, err);
    try {
      lc.warn?.(`deleting corrupted db at ${replicaDbPath}`);
      deleteLiteDB(replicaDbPath);
    } catch (e) {
      lc.warn?.(`error deleting corrupted db at ${replicaDbPath}`, e);
    }
  }

  function createProcessor() {
    processor = new ChangeProcessor(must(runner), must(mode), (_lc, err) => {
      handleCorruptedDb(err);
      host.postWriteError({writeError: serializeError(err)});
    });
  }

  function createCheckpointer() {
    if (checkpointerConfig) {
      assert(lc && db && replicaDbPath, `not initialized`);
      checkpointer = new LitestreamCheckpointer(
        lc,
        db,
        host.createLitestreamClient(lc, replicaDbPath),
        checkpointerConfig,
      );
    }
  }

  return {
    init(
      dbPath: string,
      cpMode: ChangeProcessorMode,
      pragmas: PragmaConfig,
      logConfig: LogConfig,
      checkpointConfig: ForceCheckpointConfig | null,
    ): void {
      replicaDbPath = dbPath;
      lc = host.createLogContext(logConfig);
      unregisterCorruptionDiagnostics();
      unregisterCorruptionDiagnosticTargets.push(
        registerSQLiteCorruptionDiagnosticTarget({
          debugName: 'write-worker',
          dbPath,
        }),
      );
      try {
        db = new Database(lc, dbPath);
        applyPragmas(db, pragmas);
        runner = new StatementRunner(db);
        mode = cpMode;
        checkpointerConfig = checkpointConfig ?? undefined;
        createProcessor();
        createCheckpointer();
      } catch (e) {
        handleCorruptedDb(e);
        throw e;
      }
    },

    getSubscriptionState() {
      try {
        return getSubscriptionState(must(runner));
      } catch (e) {
        handleCorruptedDb(e);
        throw e;
      }
    },

    getBackfillDeclarations() {
      try {
        return readBackfillDeclarations(must(runner).db);
      } catch (e) {
        handleCorruptedDb(e);
        throw e;
      }
    },

    async processMessages(downstream: readonly ChangeStreamData[]) {
      try {
        let commitResult = null;
        for (const message of downstream) {
          const committed = must(processor).processMessage(must(lc), message);
          if (committed) {
            commitResult = committed;
            if (checkpointer) {
              await checkpointer.maybeCheckpoint();
            }
          }
        }
        return commitResult;
      } catch (e) {
        handleCorruptedDb(e);
        throw e;
      }
    },

    abort() {
      must(processor).abort(must(lc));
      checkpointer?.close();
      createProcessor();
      createCheckpointer();
    },

    stop() {
      checkpointer?.close();
      checkpointer = undefined;
      db?.close();
      db = undefined;
      runner = undefined;
      processor = undefined;
      replicaDbPath = undefined;
      unregisterCorruptionDiagnostics();
    },
  };
}

/**
 * Runs `msg` against `api` and responds with its result or error, as the
 * worker's message loop does. Requests are not serialized here: the client
 * sends one at a time, except for `abort`, which it sends at any time and
 * expects no response to.
 */
export async function handleWriteWorkerRequest(
  api: WriteWorkerAPI,
  msg: Request,
  respond: (response: Response) => void,
): Promise<void> {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- TS can't narrow msg.method + msg.args together
    const result = await (api[msg.method] as (...args: any[]) => unknown)(
      ...msg.args,
    );
    // abort is fire-and-forget — no pending slot on the client side.
    if (msg.method !== 'abort') {
      respond({method: msg.method, result} as Response);
    }
  } catch (e) {
    if (msg.method !== 'abort') {
      respond({
        method: msg.method,
        error: serializeError(e),
      } as Response);
    }
  }
}
