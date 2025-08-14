import {pid} from 'node:process';
import {assert} from '../../../shared/src/asserts.ts';
import {must} from '../../../shared/src/must.ts';
import * as v from '../../../shared/src/valita.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {ChangeStreamerHttpClient} from '../services/change-streamer/change-streamer-http.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  ReplicatorService,
  type ReplicatorMode,
} from '../services/replicator/replicator.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {getShardConfig} from '../types/shards.ts';
import {
  replicaFileModeSchema,
  setUpMessageHandlers,
  setupReplica,
} from '../workers/replicator.ts';
import {createLogContext} from './logging.ts';

export default async function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  assert(args.length > 0, `replicator mode not specified`);
  const fileMode = v.parse(args[0], replicaFileModeSchema);

  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});
  const mode: ReplicatorMode = fileMode === 'backup' ? 'backup' : 'serving';
  const workerName = `${mode}-replicator`;
  const lc = createLogContext(config, {worker: workerName});

  const replica = await setupReplica(lc, fileMode, config.replica);

  const shard = getShardConfig(config);
  const {
    taskID,
    change,
    changeStreamer: {
      port,
      mode: m,
      uri: changeStreamerURI = m === 'dedicated'
        ? `http://localhost:${port}/`
        : undefined,
    },
  } = config;
  const changeStreamer = new ChangeStreamerHttpClient(
    lc,
    shard,
    change.db,
    changeStreamerURI,
  );

  const replicator = new ReplicatorService(
    lc,
    taskID,
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
