import {must} from 'shared/src/must.js';
import {ReplicatorService} from '../services/replicator/replicator.js';
import {parentWorker, singleProcessMode, Worker} from '../types/processes.js';
import {runAsWorker} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

export default async function runWorker(parent: Worker) {
  const config = configFromEnv();

  const replicator = new ReplicatorService(
    createLogContext(config, {worker: 'replicator'}),
    config.REPLICA_ID, // TODO: Parameterize replicaID
    config.UPSTREAM_URI,
    config.REPLICA_DB_FILE,
  );

  void runAsWorker(replicator, parent);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }
}

// fork()
if (!singleProcessMode()) {
  void runWorker(must(parentWorker));
}
