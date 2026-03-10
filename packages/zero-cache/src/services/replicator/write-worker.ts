import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {ChangeStreamData} from '../change-source/protocol/current/downstream.ts';
import {ChangeProcessor, type ChangeProcessorMode} from './change-processor.ts';
import {getSubscriptionState} from './schema/replication-state.ts';
import {
  applyPragmas,
  type PragmaConfig,
  type PushError,
  type Request,
  type Response,
} from './write-worker-client.ts';

import {parentPort} from 'node:worker_threads';

if (!parentPort) {
  throw new Error('write-worker must be run as a worker thread');
}

let db: Database | undefined;
let runner: StatementRunner | undefined;
let processor: ChangeProcessor | undefined;
let mode: ChangeProcessorMode | undefined;

const port = parentPort;

function createProcessor() {
  processor = new ChangeProcessor(runner!, mode!, (_lc, err) => {
    port.postMessage({
      pushError: err instanceof Error ? err : new Error(String(err)),
    } satisfies PushError);
  });
}

type API = Record<string, (...args: never[]) => unknown>;

const api: API = {
  init(dbPath: string, cpMode: ChangeProcessorMode, pragmas: PragmaConfig) {
    const lc = createSilentLogContext();
    db = new Database(lc, dbPath);
    applyPragmas(db, pragmas);
    runner = new StatementRunner(db);
    mode = cpMode;
    createProcessor();
  },

  getSubscriptionState() {
    return getSubscriptionState(runner!);
  },

  processMessage(downstream: ChangeStreamData) {
    const lc = createSilentLogContext();
    return processor!.processMessage(lc, downstream);
  },

  abort() {
    const lc = createSilentLogContext();
    processor!.abort(lc);
    createProcessor();
  },

  stop() {
    db?.close();
    db = undefined;
    runner = undefined;
    processor = undefined;
  },
};

port.on('message', (msg: Request) => {
  try {
    const result = api[msg.method](...(msg.args as Parameters<API[string]>));
    port.postMessage({id: msg.id, result} satisfies Response);
  } catch (e) {
    port.postMessage({id: msg.id, error: e} satisfies Response);
  }
});
