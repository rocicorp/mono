import {resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {consoleLogSink, LogContext} from '@rocicorp/logger';
import {assert} from '../../../shared/src/asserts.ts';
import {getLogConfig} from '../config/zero-config.ts';
import {exitAfter, runUntilKilled} from '../services/life-cycle.ts';
import {ServingCheckpointer} from '../services/replicator/serving-checkpointer.ts';
import {
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../types/processes.ts';
import {createLogContext} from './logging.ts';
import {startOtelAuto} from './otel-start.ts';

// Default LogContext, overridden in runWorker
let lc = new LogContext('info', {}, consoleLogSink);

/**
 * Checkpoints a serving replica on behalf of its replicator, which starts
 * this worker as one of its supporting processes once the replica is set up,
 * passing the path of its replica file as the first argument.
 *
 * Only the logging config is parsed from the remaining arguments, which
 * ignores everything else. In particular, `childWorker()` forwards the
 * replicator's own arguments, which start with its replica file mode and
 * would fail a full config parse.
 */
export default function runWorker(
  parent: Worker,
  env: NodeJS.ProcessEnv,
  ...args: string[]
): Promise<void> {
  const [dbPath, ...argv] = args;
  assert(dbPath, `replica file not specified`);
  const logConfig = getLogConfig({env, argv});
  const workerName = 'serving-checkpointer';
  startOtelAuto(
    createLogContext(logConfig, workerName, 0, false),
    workerName,
    0,
  );
  lc = createLogContext(logConfig, workerName);

  return runUntilKilled(lc, parent, new ServingCheckpointer(lc, dbPath));
}

// fork()
const isMain =
  typeof process.argv[1] === 'string' &&
  fileURLToPath(import.meta.url) === resolve(process.argv[1]);

const parent = parentWorker;
if (isMain && parent !== null && !singleProcessMode()) {
  void exitAfter(
    () => lc,
    () => runWorker(parent, process.env, ...process.argv.slice(2)),
  );
}
