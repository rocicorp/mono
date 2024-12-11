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
  runUntilKilled,
  Terminator,
} from '../life-cycle.js';
import {createLogContext} from '../logging.js';
import {getMultiZeroConfig} from './config.js';
import {TenantDispatcher} from './tenant-dispatcher.js';

export default async function runWorker(
  parent: Worker | null,
  env: NodeJS.ProcessEnv,
): Promise<void> {
  const startMs = Date.now();
  const config = getMultiZeroConfig(env);
  const lc = createLogContext(config, {worker: 'multi'});

  const {port, heartbeatMonitorPort} = config;

  let tenantPort = port;
  const tenants = config.tenants.map(tenant => ({
    ...tenant,
    worker: childWorker('./server/main.ts', {
      // defaults
      ['ZERO_TENANT_ID']: tenant.id,
      ['ZERO_PORT']: String((tenantPort += 3)),
      ['ZERO_LOG_LEVEL']: config.log.level,
      ['ZERO_LOG_FORMAT']: config.log.format,
      ...tenant.env,
    }),
  }));

  const terminator = new Terminator(lc);
  for (const tenant of tenants) {
    terminator.addWorker(tenant.worker, 'user-facing', tenant.id);
  }

  lc.info?.('waiting for tenants to be ready ...');
  if ((await orTimeout(terminator.allWorkersReady(), 30_000)) === 'timed-out') {
    lc.info?.(`timed out waiting for readiness (${Date.now() - startMs} ms)`);
  } else {
    lc.info?.(`all tenants ready (${Date.now() - startMs} ms)`);
  }

  const mainServices: Service[] = [
    new TenantDispatcher(lc, tenants, {port}),
    new HeartbeatMonitor(lc, {port: heartbeatMonitorPort ?? port + 2}),
  ];

  parent?.send(['ready', {ready: true}]);

  try {
    await runUntilKilled(lc, process, ...mainServices);
  } catch (err) {
    terminator.logErrorAndExit(err);
  }
}

if (!singleProcessMode()) {
  void exitAfter(() => runWorker(parentWorker, process.env));
}
