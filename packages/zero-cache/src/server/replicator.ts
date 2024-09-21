import {pid} from 'node:process';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.js';
import {ReplicatorService} from '../services/replicator/replicator.js';
import {runOrExit} from '../services/runner.js';
import {parentWorker, singleProcessMode, Worker} from '../types/processes.js';
import {
  ReplicatorMode,
  setUpMessageHandlers,
  setupReplicaAndCheckpointer,
} from '../workers/replicator.js';
import {configFromEnv} from './config.js';
import {createLogContext} from './logging.js';

export default async function runWorker(parent: Worker, ...args: string[]) {
  const config = configFromEnv();
  assert(args.length > 0, `replicator mode not specified`);

  const mode = args[0] as ReplicatorMode;
  const workerName = `${mode === 'backup' ? 'backup' : 'serving'}-replicator`;
  const lc = createLogContext(config, {worker: workerName});

  const {replica, checkpointer} = setupReplicaAndCheckpointer(
    lc,
    mode,
    config.REPLICA_DB_FILE,
  );

  const changeStreamer = config.CHANGE_STREAMER_URI
    ? new ChangeStreamerHttpClient(lc, config.CHANGE_STREAMER_URI)
    : new ChangeStreamerHttpClient(lc);

  const replicator = new ReplicatorService(
    lc,
    `${workerName}-${pid}`,
    changeStreamer,
    replica,
    checkpointer,
  );

  setUpMessageHandlers(lc, replicator, parent);

  void runOrExit(lc, replicator);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }
}

// fork()
if (!singleProcessMode()) {
  void runWorker(must(parentWorker), ...process.argv.slice(2));
}
