import {must} from '../../../shared/src/must.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
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
  const config = getZeroConfig({env, argv: args.slice(1)});
  const lc = createLogContext(config, {worker: 'mutator'});

  // TODO: create `PusherFactory`
  return runUntilKilled(lc, parent, new Mutator());
}

if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
