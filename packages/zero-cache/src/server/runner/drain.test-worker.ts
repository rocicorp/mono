import {LogContext, consoleLogSink} from '@rocicorp/logger';
import {must} from '../../../../shared/src/must.ts';
import {normalizeZeroConfig} from '../../config/normalize.ts';
import {getZeroConfig} from '../../config/zero-config.ts';
import {ProcessManager, runUntilKilled} from '../../services/life-cycle.ts';
import type {ActivityBasedService} from '../../services/service.ts';
import {ConnectionContextManagerImpl} from '../../services/view-syncer/connection-context-manager.ts';
import type {ViewSyncer} from '../../services/view-syncer/view-syncer.ts';
import {
  forkChildWorker,
  parentWorker,
  type Message,
} from '../../types/processes.ts';
import {Syncer} from '../../workers/syncer.ts';
import {WorkerDispatcher} from '../worker-dispatcher.ts';
import {ZeroDispatcher} from './zero-dispatcher.ts';

/**
 * A process tree that mirrors production: a `runner` (as in run-worker.ts)
 * with a `dispatcher` child (as in main.ts) with two `syncer` children. Only
 * the database-dependent view-syncer is faked; the processes, signals, IPC
 * socket transfers, dispatchers, Syncers, and WebSockets are real.
 */
export type Status = {
  event: 'created' | 'stopping' | 'exited' | 'release';
  index: number;
  pid?: number | undefined;
};

type StatusMessage = Message<Status>;
const parent = must(parentWorker);
const role = process.argv[2];
const index = Number(process.argv[3] ?? 0);
const lc = new LogContext('error', undefined, consoleLogSink);
const config = normalizeZeroConfig(
  lc,
  getZeroConfig({
    argv: [],
    env: {
      ZERO_UPSTREAM_DB: 'postgresql://unused/unused',
      ZERO_AFFINITY_COOKIE_NAME: 'zero-affinity',
      ZERO_TASK_ID: 'drain-test',
      ZERO_PORT: '0',
    },
  }),
  {},
  'drain-test',
);

function status(event: Status['event']) {
  parent.send<StatusMessage>(['status', {event, index, pid: process.pid}]);
}

async function runSyncer() {
  const released = Promise.withResolvers<void>();
  parent.onMessageType<StatusMessage>('status', message => {
    if (message.event === 'release') released.resolve();
  });
  const syncer = new Syncer(
    lc,
    config,
    (id, _sub, coordinator) => {
      status('created');
      // Holds the drain open until the test sends 'release'.
      const stopped = Promise.withResolvers<void>();
      return {
        id,
        connContextManager: new ConnectionContextManagerImpl(lc),
        keepalive: () => true,
        run: () => stopped.promise,
        stop: async () => {
          status('stopping');
          await released.promise;
          stopped.resolve();
          if (coordinator.shouldDrain()) coordinator.drainNextIn(0);
        },
        queryCount: 0,
        rowCount: 0,
        hydrationTimeMs: 0,
        createdAtMs: Date.now(),
        servedVersion: null,
        servingLagEligible: true,
        pipelineHashes: () => [],
        clientSchemaKey: undefined,
      } as unknown as ViewSyncer & ActivityBasedService;
    },
    undefined,
    undefined,
    parent,
    undefined,
  );
  const running = runUntilKilled(lc, parent, syncer);
  parent.send(['ready', {}]);
  await running;
}

async function run() {
  if (role === 'syncer') {
    return runSyncer();
  }

  const processes = new ProcessManager(lc, parent);
  const workers = Array.from({length: role === 'runner' ? 1 : 2}, (_, i) => {
    const child = forkChildWorker(
      new URL(import.meta.url),
      process.env,
      role === 'runner' ? 'dispatcher' : 'syncer',
      String(i),
    );
    child.onMessageType<StatusMessage>('status', msg =>
      parent.send<StatusMessage>(['status', msg]),
    );
    if (role === 'dispatcher') {
      child.once('close', () =>
        parent.send<StatusMessage>([
          'status',
          {event: 'exited', index: i, pid: child.pid},
        ]),
      );
    }
    processes.addWorker(child, 'user-facing', `${role}-child-${i}`);
    return child;
  });
  parent.onMessageType<StatusMessage>('status', message => {
    for (const child of workers) child.send<StatusMessage>(['status', message]);
  });
  await processes.allWorkersReady();

  if (role === 'dispatcher') {
    const dispatcher = new WorkerDispatcher(
      lc,
      config.taskID,
      parent,
      workers,
      undefined,
      undefined,
    );
    const running = runUntilKilled(lc, parent, dispatcher);
    parent.send(['ready', {}]);
    try {
      await running;
    } finally {
      await processes.shutdown();
    }
    return;
  }

  class Dispatcher extends ZeroDispatcher {
    override async start() {
      const url = await super.start();
      parent.send(['ready', {url}]);
      return url;
    }
  }
  const dispatcher = new Dispatcher(
    config,
    lc,
    {port: 0, keepaliveTimeoutMs: undefined},
    () => Promise.resolve(workers[0]),
    undefined,
    () => processes.shutdown(),
  );
  try {
    await runUntilKilled(lc, parent, dispatcher);
  } finally {
    await processes.shutdown();
  }
}

void run().then(
  () => process.exit(0),
  error => {
    lc.error?.('drain test worker failed', error);
    process.exit(1);
  },
);
