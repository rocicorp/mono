import {parentPort, workerData} from 'node:worker_threads';
import {createLogContext} from '../services/logging.js';
import {ReplicatorService} from '../services/replicator/replicator.js';
import {ReplicatorWorkerData, runAsWorker} from '../workers/replicator.js';
import {configFromEnv} from './config.js';

const config = configFromEnv();

void runAsWorker(
  new ReplicatorService(
    createLogContext(config, {thread: 'replicator'}),
    config.REPLICA_ID, // TODO: Parameterize replicaID
    config.UPSTREAM_URI,
    config.REPLICA_DB_FILE,
  ),
  parentPort,
  workerData as ReplicatorWorkerData,
);
