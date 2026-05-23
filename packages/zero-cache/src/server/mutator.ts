import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {must} from 'shared/src/must.ts';
import {getNormalizedZeroConfig} from '../config/zero-config.ts';
import {initEventSink} from '../observability/events.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {Mutator} from '../workers/mutator.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});
  startOtelAuto(createLogContext(config, 'mutator', 0, false), 'mutator', 0);
  lc = createLogContext(config, 'mutator');
  initEventSink(lc, config);

  // TODO: create `PusherFactory`
  return runUntilKilled(lc, parent, new Mutator());
}

if (!singleProcessMode()) {
  void exitAfter(lc, () =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
