import 'dotenv/config'; // Imports ENV variables from .env
import {assert} from '../../../../shared/src/asserts.js';
import type {Service} from '../../services/service.js';
import {
  childWorker,
  parentWorker,
  singleProcessMode,
  type Worker,
} from '../../types/processes.js';
import {orTimeout} from '../../types/timeout.js';
import {
  exitAfter,
  HeartbeatMonitor,
  ProcessManager,
  runUntilKilled,
} from '../life-cycle.js';
import {createLogContext} from '../logging.js';
import {getMultiZeroConfig} from './config.js';
import {getTaskID} from './runtime.js';
import {TenantDispatcher} from './tenant-dispatcher.js';

export default async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const {config, env: baseEnv} = getMultiZeroConfig(env);
  const lc = createLogContext(config, {worker: 'main'});
  const processes = new ProcessManager(lc, parent ?? process);

  const {
    port,
    changeStreamerPort = port + 1,
    heartbeatMonitorPort = port + 2,
  } = config;
  let {taskID} = config;
  if (!taskID) {
    taskID = await getTaskID(lc);
    baseEnv['ZERO_TASK_ID'] = taskID;
  }
  lc.info?.(`starting task ${taskID}`);

  const multiMode = config.tenants.length;
  if (!multiMode) {
    // Run a single tenant on main `port`, and skip the TenantDispatcher.
    config.tenants.push({
      id: '',
      env: {
        ['ZERO_PORT']: String(port),
        ['ZERO_CVR_DB']: config.cvr.db,
        ['ZERO_CHANGE_DB']: config.change.db,
        ['ZERO_REPLICA_FILE']: config.replicaFile,
      },
    });
  }

  // In the multi-node configuration, determine if this process
  // should be dispatching sync requests (i.e. by `host` / `port`),
  // or change-streamer requests (i.e. by tenant `id`).
  const runAsReplicationManager = config.numSyncWorkers === 0;

  // Start the first tenant at (port + 1 + 2) unless explicitly
  // overridden by its own ZERO_PORT ...
  let tenantPort = port + 1;
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
    return {...tenant, worker: childWorker('./server/main.ts', mergedEnv)};
  });

  for (const tenant of tenants) {
    processes.addWorker(tenant.worker, 'user-facing', tenant.id);
  }

  const s = tenants.length > 1 ? 's' : '';
  lc.info?.(`waiting for zero-cache${s} to be ready ...`);
  if ((await orTimeout(processes.allWorkersReady(), 60_000)) === 'timed-out') {
    lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
  } else {
    lc.info?.(`zero-cache${s} ready (${Date.now() - startMs} ms)`);
  }

  const mainServices: Service[] = [
    new HeartbeatMonitor(lc, {port: heartbeatMonitorPort}),
  ];
  if (multiMode) {
    mainServices.push(
      new TenantDispatcher(lc, runAsReplicationManager, tenants, {
        port: runAsReplicationManager ? changeStreamerPort : port,
      }),
    );
  }

  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(lc, parent ?? process, ...mainServices);
  } catch (err) {
    processes.logErrorAndExit(err, 'main');
  }

  await processes.done();
}

if (!singleProcessMode()) {
  void exitAfter(() => runWorker(parentWorker, process.env));
}
