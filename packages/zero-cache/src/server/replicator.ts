import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.js';
import {must} from '../../../shared/src/must.js';
import * as v from '../../../shared/src/valita.js';
import {getZeroConfig} from '../config/zero-config.js';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.js';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.js';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.js';
import {
  replicaFileModeSchema,
  setUpMessageHandlers,
  setupReplica,
} from '../workers/replicator.js';
import {exitAfter, runUntilKilled} from './life-cycle.js';
import {createLogContext} from './logging.js';

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const config = getZeroConfig(env, args.slice(1));
  const mode: ReplicatorMode = fileMode === 'backup' ? 'backup' : 'serving';
  const workerName = `${mode}-replicator`;
  const lc = createLogContext(config.log, {worker: workerName});

  const replica = setupReplica(lc, fileMode, config.replicaFile);

  const changeStreamerPort = config.changeStreamerPort ?? config.port + 1;
  const changeStreamer = config.changeStreamerURI
    ? new ChangeStreamerHttpClient(lc, config.changeStreamerURI)
    : new ChangeStreamerHttpClient(lc, changeStreamerPort);

  const replicator = new ReplicatorService(
    lc,
    must(config.taskID, `main must set --task-id`),
    `${workerName}-${pid}`,
    mode,
    changeStreamer,
    replica,
  );

  setUpMessageHandlers(lc, replicator, parent);

  const running = runUntilKilled(lc, parent, replicator);

  // Signal readiness once the first ReplicaVersionReady notification is received.
  for await (const _ of replicator.subscribe()) {
    parent.send(['ready', {ready: true}]);
    break;
  }

  return running;
}

// fork()
if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
