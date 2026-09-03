import {parentPort} from 'node:worker_threads';
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
import {createLogContext} from '../../server/logging.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {LitestreamCheckpointer} from '../litestream/litestream-checkpointer.ts';
import {LitestreamController} from '../litestream/litestream-controller.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
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

if (!parentPort) {
  throw new Error('write-worker must be run as a worker thread');
}

const port = parentPort;

type API = {
  [M in Method]: (...args: ArgsMap[M]) => ResultMap[M] | Promise<ResultMap[M]>;
};

function createAPI(): API {
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
      port.postMessage({
        writeError: serializeError(err),
      } satisfies WriteError);
    });
  }

  function createCheckpointer() {
    if (checkpointerConfig) {
      assert(lc && db && replicaDbPath, `not initialized`);
      checkpointer = new LitestreamCheckpointer(
        lc,
        db,
        new LitestreamController(lc, replicaDbPath),
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
      lc = createLogContext({log: logConfig}, 'write-worker');
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

    async processMessage(downstream: ChangeStreamData) {
      try {
        const committed = must(processor).processMessage(must(lc), downstream);
        if (committed && checkpointer) {
          await checkpointer.maybeCheckpoint();
        }
        return committed;
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

const api = createAPI();

port.on('message', async (msg: Request) => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- TS can't narrow msg.method + msg.args together
    const result = await (api[msg.method] as (...args: any[]) => unknown)(
      ...msg.args,
    );
    // abort is fire-and-forget — no pending slot on the client side.
    if (msg.method !== 'abort') {
      port.postMessage({method: msg.method, result} as Response);
    }
  } catch (e) {
    if (msg.method !== 'abort') {
      port.postMessage({
        method: msg.method,
        error: serializeError(e),
      } as Response);
    }
  }
});
