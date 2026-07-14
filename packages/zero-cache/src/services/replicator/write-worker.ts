import {parentPort} from 'node:worker_threads';
import type {LogContext} from '@rocicorp/logger';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {
  isSQLiteCorruption,
  logSQLiteCorruptionDiagnostics,
  registerSQLiteCorruptionDiagnosticTarget,
} from '../../db/sqlite-corruption.ts';
import {StatementRunner} from '../../db/statements.ts';
import {createLogContext} from '../../server/logging.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  applyPragmas,
  serializeError,
  type ArgsMap,
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

type API = {[M in Method]: (...args: ArgsMap[M]) => ResultMap[M]};

function createAPI(): API {
  let db: Database | undefined;
  let runner: StatementRunner | undefined;
  let processor: ChangeProcessor | undefined;
  let mode: ChangeProcessorMode | undefined;
  let lc: LogContext | undefined;
  let replicaDbPath: string | undefined;
  let unregisterCorruptionDiagnosticTarget: (() => void) | undefined;

  function logCorruptionDiagnostics(err: unknown) {
    if (lc && replicaDbPath && isSQLiteCorruption(err)) {
      logSQLiteCorruptionDiagnostics(lc, 'write-worker', replicaDbPath, err);
    }
  }

  function createProcessor() {
    processor = new ChangeProcessor(must(runner), must(mode), (_lc, err) => {
      logCorruptionDiagnostics(err);
      port.postMessage({
        writeError: serializeError(err),
      } satisfies WriteError);
    });
  }

  return {
    init(
      dbPath: string,
      cpMode: ChangeProcessorMode,
      pragmas: PragmaConfig,
      logConfig: LogConfig,
    ) {
      replicaDbPath = dbPath;
      lc = createLogContext({log: logConfig}, 'write-worker');
      unregisterCorruptionDiagnosticTarget?.();
      unregisterCorruptionDiagnosticTarget =
        registerSQLiteCorruptionDiagnosticTarget({
          debugName: 'write-worker',
          dbPath,
        });
      try {
        db = new Database(lc, dbPath);
        applyPragmas(db, pragmas);
        runner = new StatementRunner(db);
        mode = cpMode;
        createProcessor();
      } catch (e) {
        logCorruptionDiagnostics(e);
        throw e;
      }
    },

    getSubscriptionState() {
      try {
        return getSubscriptionState(must(runner));
      } catch (e) {
        logCorruptionDiagnostics(e);
        throw e;
      }
    },

    processMessage(downstream: ChangeStreamData) {
      try {
        return must(processor).processMessage(must(lc), downstream);
      } catch (e) {
        logCorruptionDiagnostics(e);
        throw e;
      }
    },

    abort() {
      must(processor).abort(must(lc));
      createProcessor();
    },

    stop() {
      db?.close();
      db = undefined;
      runner = undefined;
      processor = undefined;
      replicaDbPath = undefined;
      unregisterCorruptionDiagnosticTarget?.();
      unregisterCorruptionDiagnosticTarget = undefined;
    },
  };
}

const api = createAPI();

port.on('message', (msg: Request) => {
  try {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any -- TS can't narrow msg.method + msg.args together
    const result = (api[msg.method] as (...args: any[]) => unknown)(
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
