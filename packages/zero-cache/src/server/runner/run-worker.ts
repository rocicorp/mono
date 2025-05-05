import '@dotenvx/dotenvx/config'; // Imports ENV variables from .env
import {resolver, type Resolver} from '@rocicorp/resolver';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {getZeroConfig} from '../../config/zero-config.ts';
import {ProcessManager, runUntilKilled} from '../../services/life-cycle.ts';
import {childWorker, type Worker} from '../../types/processes.ts';
import {createLogContext} from '../logging.ts';
import {getTaskID} from './runtime.ts';
import {ZeroDispatcher} from './zero-dispatcher.ts';

/**
 * Top-level `runner` entry point to the zero-cache. This layer is responsible for:
 * * runtime-based config normalization
 * * lazy startup
 * * serving /statsz
 * * auto-reset restarts (TODO)
 */
export async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const config = getZeroConfig(env);
  const lc = createLogContext(config, {worker: 'runner'});
  const processes = new ProcessManager(lc, parent ?? process);

  const {serverVersion, port, lazyStartup} = config;

  let {taskID} = config;
  if (!taskID) {
    taskID = await getTaskID(lc);
    env['ZERO_TASK_ID'] = taskID;
  }
  lc.info?.(
    `starting server${!serverVersion ? '' : `@${serverVersion}`} ` +
      `protocolVersion=${PROTOCOL_VERSION}, taskID=${taskID}`,
  );

  let zeroCache: Resolver<Worker> | undefined;
  function startZeroCache(): Promise<Worker> {
    if (zeroCache === undefined) {
      const startMs = performance.now();
      lc.info?.('starting zero-cache');

      const r = (zeroCache = resolver<Worker>());
      const w = childWorker('./server/main.ts', env)
        .once('message', () => {
          r.resolve(w);
          lc.info?.(`zero-cache ready (${performance.now() - startMs} ms)`);
        })
        .once('error', r.reject);

      processes.addWorker(w, 'user-facing', 'zero-cache');
    }
    return zeroCache.promise;
  }

  // Eagerly start the zero-cache if it was not configured with --lazy-startup.
  if (!lazyStartup) {
    void startZeroCache();
  }

  await processes.allWorkersReady();
  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(
      lc,
      parent ?? process,
      new ZeroDispatcher(config, lc, {port}, startZeroCache),
    );
  } catch (err) {
    processes.logErrorAndExit(err, 'main');
  }

  await processes.done();
}
