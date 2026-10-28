import {parentPort, workerData} from 'node:worker_threads';
import {must} from '../../../../shared/src/must.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {createLogContext} from '../../server/logging.ts';
import type {
  CheckpointArgs,
  CheckpointRequest,
  CheckpointResponse,
  CheckpointResult,
} from './serving-checkpointer.ts';
import {serializeError} from './write-worker-client.ts';

const port = must(parentPort);
const {dbPath, logConfig} = workerData as CheckpointArgs;
const lc = createLogContext({log: logConfig}, 'serving-checkpoint');
let db: Database | undefined;

function send(message: CheckpointResponse) {
  port.postMessage(message);
}

function close() {
  try {
    db?.close({optimize: false});
  } finally {
    port.close();
  }
}

function fail(error: unknown) {
  send({type: 'error', error: serializeError(error)});
  close();
}

try {
  db = new Database(lc, dbPath, {fileMustExist: true});
  db.pragma('busy_timeout = 0');
  db.pragma('wal_autocheckpoint = 0');
  const [{journal_mode: journalMode}] = db.pragma<{journal_mode: string}>(
    'journal_mode',
  );
  if (journalMode !== 'wal2') {
    throw new Error(
      `Serving checkpoint worker requires WAL2, got ${journalMode}`,
    );
  }
  port.on('message', (message: CheckpointRequest) => {
    try {
      if (message === 'stop') {
        close();
        return;
      }
      const start = performance.now();
      const [result] = must(db).pragma<Omit<CheckpointResult, 'durationMs'>>(
        'wal_checkpoint(PASSIVE)',
      );
      const durationMs = performance.now() - start;
      if (durationMs >= 1000) {
        lc.warn?.('Slow serving replica checkpoint', {durationMs, ...result});
      }
      send({type: 'checkpoint', result: {durationMs, ...result}});
    } catch (error) {
      fail(error);
    }
  });
  send({type: 'ready'});
} catch (error) {
  fail(error);
}
