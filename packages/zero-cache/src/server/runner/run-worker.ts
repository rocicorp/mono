import '@dotenvx/dotenvx/config'; // Imports ENV variables from .env
import {resolver, type Resolver} from '@rocicorp/resolver';
import {assert} from '../../../../shared/src/asserts.ts';
import {parseBoolean} from '../../../../shared/src/options.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {ProcessManager, runUntilKilled} from '../../services/life-cycle.ts';
import {childWorker, type Worker} from '../../types/processes.ts';
import {createLogContext} from '../logging.ts';
import {getMultiZeroConfig} from './config.ts';
import {getTaskID} from './runtime.ts';
import {ZeroDispatcher} from './zero-dispatcher.ts';

export async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const {config, env: baseEnv} = getMultiZeroConfig(env);
  const lc = createLogContext(config, {worker: 'runner'});
  const processes = new ProcessManager(lc, parent ?? process);

  const {serverVersion, port, changeStreamerPort = port + 1} = config;
  let {taskID} = config;
  if (!taskID) {
    taskID = await getTaskID(lc);
    baseEnv['ZERO_TASK_ID'] = taskID;
  }
  lc.info?.(
    `starting server${!serverVersion ? '' : `@${serverVersion}`} ` +
      `protocolVersion=${PROTOCOL_VERSION}, taskID=${taskID}`,
  );

  const multiMode = config.tenants.length;
  if (!multiMode) {
    config.tenants.push({
      id: '', // sole tenant signifier
      env: {['ZERO_REPLICA_FILE']: config.replica.file},
    });
  }

  // In the multi-node configuration, determine if this process
  // should be dispatching sync requests (i.e. by `host` / `port`),
  // or change-streamer requests (i.e. by tenant `id`).
  const runAsReplicationManager = config.numSyncWorkers === 0;

  // Start the first tenant at (port + 2) unless explicitly
  // overridden by its own ZERO_PORT ...
  let tenantPort = port;
  const tenants = config.tenants.map(tenant => {
    const mergedEnv: NodeJS.ProcessEnv = {
      ...process.env, // propagate all ENV variables from this process
      ...baseEnv, // defaults
      ['ZERO_TENANT_ID']: tenant.id,
      ['ZERO_PORT']: String((tenantPort += 2)), // and bump the port by 2 thereafter.
      ...tenant.env, // overrides
    };

    if (runAsReplicationManager) {
      // Sanity-check. It doesn't make sense to run sync workers in the
      // replication-manager.
      assert(
        mergedEnv['ZERO_NUM_SYNC_WORKERS'] === '0',
        `Tenant ${tenant.id} cannot run sync workers in the replication-manager`,
      );
    }

    const changeStreamerURI = mergedEnv['ZERO_CHANGE_STREAMER_URI'];
    if (changeStreamerURI && multiMode) {
      // Requests from the view-syncer to the replication-manager are
      // delineated/dispatched by the tenant ID as the first path component.
      mergedEnv['ZERO_CHANGE_STREAMER_URI'] += changeStreamerURI.endsWith('/')
        ? tenant.id
        : `/${tenant.id}`;
    }

    let worker: Resolver<Worker> | undefined;

    function getWorker(): Promise<Worker> {
      if (worker === undefined) {
        lc.info?.(
          'starting zero-cache' + (tenant.id ? ` for ${tenant.id}` : ''),
        );
        const r = (worker = resolver<Worker>());
        const w = childWorker('./server/main.ts', mergedEnv)
          .once('message', () => r.resolve(w))
          .once('error', r.reject);
        processes.addWorker(w, 'user-facing', tenant.id);
      }
      return worker.promise;
    }

    return {...tenant, env: mergedEnv, getWorker};
  });

  // Eagerly start zero-caches that are not configured to --run-lazily.
  for (const tenant of tenants) {
    const lazy = parseBoolean(
      'ZERO_RUN_LAZILY',
      tenant.env['ZERO_RUN_LAZILY'] ?? 'false',
    );
    if (!lazy) {
      void tenant.getWorker();
    }
  }

  const s = tenants.length > 1 ? 's' : '';
  lc.info?.(`waiting for zero-cache${s} to be ready ...`);
  await processes.allWorkersReady();
  lc.info?.(`zero-cache${s} ready (${Date.now() - startMs} ms)`);

  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(
      lc,
      parent ?? process,
      new ZeroDispatcher(lc, runAsReplicationManager, tenants, {
        port: runAsReplicationManager ? changeStreamerPort : port,
      }),
    );
  } catch (err) {
    processes.logErrorAndExit(err, 'main');
  }

  await processes.done();
}
