import {parentPort, workerData} from 'node:worker_threads';
import {ReplicatorService} from '../services/replicator/replicator.js';
import {ReplicatorWorkerData, runAsWorker} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {ReadySignal} from './life-cycle.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();

const replicator = new ReplicatorService(
  createLogContext(config, {thread: 'replicator'}),
  config.REPLICA_ID, // TODO: Parameterize replicaID
  config.UPSTREAM_URI,
  config.REPLICA_DB_FILE,
);

void runAsWorker(replicator, parentPort, workerData as ReplicatorWorkerData);

// Signal readiness once the first ReplicaVersionReady notification is received.
for await (const _ of replicator.subscribe()) {
  parentPort?.postMessage({ready: true} satisfies ReadySignal);
  break;
}
