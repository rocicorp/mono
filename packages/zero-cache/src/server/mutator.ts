import type {LogContext} from '@rocicorp/logger';
import {must} from '../../../shared/src/must.ts';
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

function runWorker(lc: LogContext, parent: Worker): Promise<void> {
  // TODO: create `PusherFactory`
  return runUntilKilled(lc, parent, new Mutator());
}

if (!singleProcessMode()) {
  const config = getNormalizedZeroConfig({
    env: process.env,
    argv: process.argv.slice(3),
  });
  startOtelAuto(createLogContext(config, 'mutator', 0, false), 'mutator', 0);
  const lc = createLogContext(config, 'mutator');
  initEventSink(lc, config);
  void exitAfter(lc, () => runWorker(lc, must(parentWorker)));
}
