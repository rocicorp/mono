/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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

function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const config = getNormalizedZeroConfig({env, argv: args.slice(1)});
  const lc = createLogContext(config, {worker: 'mutator'});
  initEventSink(lc, config);

  // TODO: create `PusherFactory`
  return runUntilKilled(lc, parent, new Mutator());
}

if (!singleProcessMode()) {
  void exitAfter(() =>
    runWorker(must(parentWorker), process.env, ...process.argv.slice(2)),
  );
}
