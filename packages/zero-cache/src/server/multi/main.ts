import 'dotenv/config'; // Imports ENV variables from .env
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
  const processes = new ProcessManager(lc);

  const {port, heartbeatMonitorPort} = config;
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
      env: {['ZERO_PORT']: String(port)},
    });
  }

  // Start the first tenant at (port + 1 + 2) unless explicitly
  // overridden by its own ZERO_PORT ...
  let tenantPort = port + 1;
  const tenants = config.tenants.map(tenant => ({
    ...tenant,
    worker: childWorker('./server/main.ts', {
      ...baseEnv, // defaults
      ['ZERO_TENANT_ID']: tenant.id,
      ['ZERO_PORT']: String((tenantPort += 2)), // and bump the port by 2 thereafter.
      ...tenant.env, // overrides
    }),
  }));

  for (const tenant of tenants) {
    processes.addWorker(tenant.worker, 'user-facing', tenant.id);
  }

  const s = tenants.length > 1 ? 's' : '';
  lc.info?.(`waiting for zero-cache${s} to be ready ...`);
  if ((await orTimeout(processes.allWorkersReady(), 30_000)) === 'timed-out') {
    lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
  } else {
    lc.info?.(`zero-cache${s} ready (${Date.now() - startMs} ms)`);
  }

  const mainServices: Service[] = [
    new HeartbeatMonitor(lc, {port: heartbeatMonitorPort ?? port + 2}),
  ];
  if (multiMode) {
    mainServices.push(new TenantDispatcher(lc, tenants, {port}));
  }

  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(lc, process, ...mainServices);
  } catch (err) {
    processes.logErrorAndExit(err);
  }
}

if (!singleProcessMode()) {
  void exitAfter(() => runWorker(parentWorker, process.env));
}
