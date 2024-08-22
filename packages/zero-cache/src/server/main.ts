import {SHARE_ENV, Worker} from 'node:worker_threads';
import {Dispatcher, Workers} from '../services/dispatcher/dispatcher.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const env = configFromEnv();
const lc = createLogContext(env, {thread: 'main'});

function logErrorAndExit(err: unknown) {
  lc.error?.(err);
  process.exit(1);
}

const replicator = new Worker('./src/server/replicator.ts', {
  env: SHARE_ENV,
  workerData: {subscriberPorts: []}, // TODO
  transferList: [
    /* subscriberPorts here too */
  ],
}).on('error', logErrorAndExit);

const workers: Workers = {
  replicator,
  syncers: [
    /* TODO */
  ],
};

const dispatcher = new Dispatcher(lc, () => workers);
try {
  await dispatcher.run();
} catch (err) {
  logErrorAndExit(err);
}
