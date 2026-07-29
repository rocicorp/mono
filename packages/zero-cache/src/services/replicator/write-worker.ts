import {parentPort} from 'node:worker_threads';
import type {LogContext} from '@rocicorp/logger';
import {assert} from '../../../../shared/src/asserts.ts';
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
import {
  changeLogFileName,
  openChangeLogDBForWriting,
  readReplicaAnchor,
  type ReconcileResult,
} from './change-log-db.ts';
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
  type SerializedChangeStreamData,
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
  let changeLogDb: Database | undefined;
  let changeLogRunner: StatementRunner | undefined;
  let processor: ChangeProcessor | undefined;
  let mode: ChangeProcessorMode | undefined;
  let lc: LogContext | undefined;
  let replicaDbPath: string | undefined;
  let changeLogDbPath: string | undefined;
  let unregisterCorruptionDiagnosticTargets: (() => void)[] = [];

  function unregisterCorruptionDiagnostics() {
    unregisterCorruptionDiagnosticTargets.forEach(unregister => unregister());
    unregisterCorruptionDiagnosticTargets = [];
  }

  // The write path spans two databases and a SqliteError does not say which
  // one it came from, so both are dumped.
  function logCorruptionDiagnostics(err: unknown) {
    if (!lc || !isSQLiteCorruption(err)) {
      return;
    }
    if (replicaDbPath) {
      logSQLiteCorruptionDiagnostics(lc, 'write-worker', replicaDbPath, err);
    }
    if (changeLogDbPath) {
      logSQLiteCorruptionDiagnostics(
        lc,
        'write-worker change-log',
        changeLogDbPath,
        err,
      );
    }
  }

  function createProcessor() {
    processor = new ChangeProcessor(
      must(runner),
      must(mode),
      {changeLog: changeLogRunner},
      (_lc, err) => {
        logCorruptionDiagnostics(err);
        port.postMessage({
          writeError: serializeError(err),
        } satisfies WriteError);
      },
    );
  }

  return {
    init(
      dbPath: string,
      cpMode: ChangeProcessorMode,
      shouldLogChangeStream: boolean,
      pragmas: PragmaConfig,
      logConfig: LogConfig,
    ): ReconcileResult | undefined {
      assert(
        !shouldLogChangeStream || cpMode !== 'initial-sync',
        'initial-sync owns its transaction boundaries and cannot write the ' +
          'change log, which opens a second one',
      );
      replicaDbPath = dbPath;
      changeLogDbPath = shouldLogChangeStream
        ? changeLogFileName(dbPath)
        : undefined;
      lc = createLogContext({log: logConfig}, 'write-worker');
      unregisterCorruptionDiagnostics();
      unregisterCorruptionDiagnosticTargets.push(
        registerSQLiteCorruptionDiagnosticTarget({
          debugName: 'write-worker',
          dbPath,
        }),
      );
      if (changeLogDbPath) {
        unregisterCorruptionDiagnosticTargets.push(
          registerSQLiteCorruptionDiagnosticTarget({
            debugName: 'write-worker change-log',
            dbPath: changeLogDbPath,
          }),
        );
      }
      try {
        db = new Database(lc, dbPath);
        applyPragmas(db, pragmas);
        runner = new StatementRunner(db);
        mode = cpMode;

        // The change log lives beside the replica rather than inside it, and
        // the path is derived here so that the derivation stays the single
        // source of truth rather than becoming a second IPC argument that
        // could disagree with it.
        changeLogDb?.close();
        changeLogDb = undefined;
        changeLogRunner = undefined;
        let reconciled: ReconcileResult | undefined;
        if (shouldLogChangeStream) {
          const opened = openChangeLogDBForWriting(
            must(lc),
            dbPath,
            readReplicaAnchor(db),
          );
          changeLogDb = opened.db;
          reconciled = opened.result;
          changeLogRunner = new StatementRunner(changeLogDb);
        }
        createProcessor();
        return reconciled;
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

    processMessage({data, json}: SerializedChangeStreamData) {
      try {
        return must(processor).processMessage(must(lc), data, json);
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
      changeLogDb?.close();
      changeLogDb = undefined;
      changeLogRunner = undefined;
      processor = undefined;
      replicaDbPath = undefined;
      changeLogDbPath = undefined;
      unregisterCorruptionDiagnostics();
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
