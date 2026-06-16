import '../../../../shared/src/dotenv.ts';

import {styleText} from 'node:util';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {colorConsole} from '../../../../shared/src/logging.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {normalizeZeroConfig} from '../../config/normalize.ts';
import {getServerVersion, getZeroConfig} from '../../config/zero-config.ts';
import {ProcessManager, runUntilKilled} from '../../services/life-cycle.ts';
import {childWorker, type Worker} from '../../types/processes.ts';
import {createLogContext} from '../logging.ts';
import {MAIN_URL} from '../worker-urls.ts';
import {getTaskID} from './runtime.ts';
import {ZeroDispatcher} from './zero-dispatcher.ts';

const startupMessageEnv = 'ZERO_ENABLE_STARTUP_MESSAGE';

function printStartupMessage(env: NodeJS.ProcessEnv) {
  if (env[startupMessageEnv] !== '1') {
    return;
  }

  colorConsole.log(
    `\nBTW, ${styleText(['bold', 'cyan'], 'Cloud Zero')} ` +
      'is now available - professional Zero hosting from the team that built it.\n' +
      `Get started now: ${styleText(['blue', 'underline'], 'https://zero.rocicorp.dev/cloud')}\n\n` +
      styleText('dim', `Disable this message with ${startupMessageEnv}=0`) +
      '\n',
  );
}

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
  // Note: Deprecation warnings are only emitted at this top-level parse;
  //       they are suppressed when parsed in subprocesses.
  const cfg = getZeroConfig({env, emitDeprecationWarnings: true});
  const lc = createLogContext(cfg, 'runner');

  const defaultTaskID = await getTaskID(lc);
  const config = normalizeZeroConfig(lc, cfg, env, defaultTaskID);
  const processes = new ProcessManager(lc, parent ?? process, env);

  const {port, keepaliveTimeoutMs, lazyStartup} = config;
  const serverVersion = getServerVersion(config);
  lc.info?.(`starting server${!serverVersion ? '' : `@${serverVersion}`} `, {
    protocolVersion: PROTOCOL_VERSION,
    taskID: config.taskID,
    app: config.app,
    shard: config.shard,
    port: config.port,
  });

  let zeroCache: Resolver<Worker> | undefined;
  function startZeroCache(): Promise<Worker> {
    if (zeroCache === undefined) {
      const startMs = performance.now();
      lc.info?.('starting zero-cache');

      const r = (zeroCache = resolver<Worker>());
      const w = childWorker(MAIN_URL, env)
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
      new ZeroDispatcher(
        config,
        lc,
        {port, keepaliveTimeoutMs},
        startZeroCache,
        () => printStartupMessage(env),
      ),
    );
  } catch (err) {
    processes.logErrorAndExit(err, 'main');
  }

  await processes.done();
}
