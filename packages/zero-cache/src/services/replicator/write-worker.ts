import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import type {LogConfig} from '../../../../shared/src/logging.ts';
import {createLogContext} from '../../server/logging.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  applyPragmas,
  type ArgsMap,
  type Method,
  type PragmaConfig,
  type ResultMap,
  type WriteError,
  type Request,
  type Response,
} from './write-worker-client.ts';

import {parentPort} from 'node:worker_threads';

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

  function createProcessor() {
    processor = new ChangeProcessor(must(runner), must(mode), (_lc, err) => {
      port.postMessage({
        writeError: err instanceof Error ? err : new Error(String(err)),
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
      lc = createLogContext({log: logConfig}, {worker: 'write-worker'});
      db = new Database(lc, dbPath);
      applyPragmas(db, pragmas);
      runner = new StatementRunner(db);
      mode = cpMode;
      createProcessor();
    },

    getSubscriptionState() {
      return getSubscriptionState(must(runner));
    },

    processMessage(downstream: ChangeStreamData) {
      return must(processor).processMessage(must(lc), downstream);
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
      port.postMessage({method: msg.method, error: e} as Response);
    }
  }
});
