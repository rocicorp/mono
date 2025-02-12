import {must} from '../../../shared/src/must.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {PusherService} from '../services/mutagen/pusher.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {Mutator} from '../workers/mutator.ts';
import {createLogContext} from './logging.ts';

function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getZeroConfig(env, args.slice(1));
  const lc = createLogContext(config, {worker: 'mutator'});

  const pusherFactory = (id: string) =>
    new PusherService(
      lc.withContext('clientGroupID', id),
      id,
      must(config.push.url),
      config.push.apiKey,
    );
  return runUntilKilled(lc, parent, new Mutator(lc, pusherFactory, parent));
}

if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
