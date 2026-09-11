import {parentPort} from 'node:worker_threads';
import {createLogContext} from '../../server/logging.ts';
import {LitestreamController} from '../litestream/litestream-controller.ts';
import {
  createWriteWorkerAPI,
  handleWriteWorkerRequest,
} from './write-worker-api.ts';
import type {Request} from './write-worker-client.ts';

if (!parentPort) {
  throw new Error('write-worker must be run as a worker thread');
}

const port = parentPort;

const api = createWriteWorkerAPI({
  createLogContext: log => createLogContext({log}, 'write-worker'),
  createLitestreamClient: (lc, replicaFile) =>
    new LitestreamController(lc, replicaFile),
  postWriteError: error => port.postMessage(error),
});

port.on('message', (msg: Request) =>
  handleWriteWorkerRequest(api, msg, response => port.postMessage(response)),
);
