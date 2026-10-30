import {parentPort, workerData} from 'node:worker_threads';
import {Database} from '../../../../zqlite/src/db.ts';
import {createLogContext} from '../../server/logging.ts';
import type {
  ServingCheckpointWorkerData,
  ServingCheckpointWorkerMessage,
} from './serving-checkpointer.ts';
import {serializeError} from './write-worker-client.ts';

if (!parentPort) {
  throw new Error('serving-checkpoint-worker must be run as a worker thread');
}

const port = parentPort;
const {dbPath, logConfig} = workerData as ServingCheckpointWorkerData;

function send(msg: ServingCheckpointWorkerMessage) {
  port.postMessage(msg);
}

function fail(e: unknown) {
  send({error: serializeError(e)});
  port.close();
}

try {
  const lc = createLogContext({log: logConfig}, 'serving-checkpointer');
  const db = new Database(lc, dbPath, {fileMustExist: true});
  port.on('message', () => {
    try {
      db.pragma('wal_checkpoint(PASSIVE)');
      send({checkpointed: true});
    } catch (e) {
      fail(e);
    }
  });
  send({ready: true});
} catch (e) {
  fail(e);
}
