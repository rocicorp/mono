import {pid} from 'node:process';
import {assert} from 'shared/src/asserts.js';
import {must} from 'shared/src/must.js';
import {getZeroConfig} from '../config/zero-config.js';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.js';
import {NULL_CHECKPOINTER} from '../services/replicator/checkpointer.js';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.js';
import {runOrExit} from '../services/runner.js';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {
  setUpMessageHandlers,
  setupReplica,
  type ReplicaFileMode,
} from '../workers/replicator.js';
import {createLogContext} from './logging.js';

export default async function runWorker(parent: Worker, ...args: string[]) {
  const config = await getZeroConfig();
  assert(args.length > 0, `replicator mode not specified`);

  const fileMode = args[0] as ReplicaFileMode;
  const mode: ReplicatorMode = fileMode === 'backup' ? 'backup' : 'serving';
  const workerName = `${mode}-replicator`;
  const lc = createLogContext(config.log, {worker: workerName});

  const replica = setupReplica(lc, fileMode, config.replicaDbFile);

  const changeStreamer = config.changeStreamerUri
    ? new ChangeStreamerHttpClient(lc, config.changeStreamerUri)
    : new ChangeStreamerHttpClient(lc);

  const replicator = new ReplicatorService(
    lc,
    `${workerName}-${pid}`,
    mode,
    changeStreamer,
    replica,
    NULL_CHECKPOINTER, // TODO: Get rid of the Checkpointer stuff; no longer be needed with wal2.
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
