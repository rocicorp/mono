import {ReplicatorService} from '../services/replicator/replicator.js';
import {parentWorker} from '../types/processes.js';
import {runAsWorker} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

const config = configFromEnv();

const replicator = new ReplicatorService(
  createLogContext(config, {worker: 'replicator'}),
  config.REPLICA_ID, // TODO: Parameterize replicaID
  config.UPSTREAM_URI,
  config.REPLICA_DB_FILE,
);

void runAsWorker(replicator, parentWorker);

// Signal readiness once the first ReplicaVersionReady notification is received.
for await (const _ of replicator.subscribe()) {
  parentWorker.send(['ready', {ready: true}]);
  break;
}
