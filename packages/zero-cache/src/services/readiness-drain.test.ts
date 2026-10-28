import {EventEmitter} from 'node:events';
import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {normalizeZeroConfig} from '../config/normalize.ts';
import {getZeroConfig} from '../config/zero-config.ts';
import {ZeroDispatcher} from '../server/runner/zero-dispatcher.ts';
import {setSingleProcessMode, type Subprocess} from '../types/processes.ts';
import {HttpService} from './http-service.ts';
import {ProcessManager} from './life-cycle.ts';

const lc = new LogContext('error');

beforeEach(() => setSingleProcessMode(true));
afterEach(() => {
  setSingleProcessMode(false);
  vi.restoreAllMocks();
});

function subprocess(): Subprocess & {kill: ReturnType<typeof vi.fn>} {
  return Object.assign(new EventEmitter(), {
    send: vi.fn(() => true),
    kill: vi.fn(),
  });
}

test('withdrawing readiness preserves liveness and traffic handling', async () => {
  const service = new HttpService(
    'test',
    lc,
    {port: 0, keepaliveTimeoutMs: undefined},
    fastify => {
      fastify.get('/traffic', () => 'still serving');
    },
  );
  const url = await service.start();
  try {
    expect((await fetch(url)).status).toBe(200);
    service.failReadinessChecks();
    expect((await fetch(url)).status).toBe(503);
    expect((await fetch(`${url}/keepalive`)).status).toBe(200);
    expect(await (await fetch(`${url}/traffic`)).text()).toBe('still serving');
    service.failHealthChecks();
    expect((await fetch(`${url}/keepalive`)).status).toBe(503);
  } finally {
    await service.stop();
  }
});

test('the dispatcher withdraws readiness throughout its shutdown delay', async () => {
  const config = normalizeZeroConfig(
    lc,
    getZeroConfig({
      argv: [],
      env: {
        ZERO_UPSTREAM_DB: 'postgresql://unused/unused',
        ZERO_SHUTDOWN_DELAY_MS: '500',
      },
    }),
    {},
    'test-task',
  );
  const dispatcher = new ZeroDispatcher(
    config,
    lc,
    {port: 0, keepaliveTimeoutMs: undefined},
    () => Promise.reject(new Error('no worker needed for health checks')),
  );
  const url = await dispatcher.start();
  try {
    const drain = dispatcher.drain();
    expect(dispatcher.drain()).toBe(drain);
    let drained = false;
    void drain.then(() => (drained = true));
    expect((await fetch(url)).status).toBe(503);
    expect((await fetch(`${url}/keepalive`)).status).toBe(200);
    expect(drained).toBe(false);
    await drain;
  } finally {
    await dispatcher.stop();
  }
});

test('workers drain only after readiness withdrawal completes', async () => {
  const parent = new EventEmitter();
  const preparation = Promise.withResolvers<void>();
  const beforeDrain = vi.fn(() => preparation.promise);
  const manager = new ProcessManager(lc, parent, beforeDrain);
  const userFacing = subprocess();
  const supporting = subprocess();
  manager.addSubprocess(userFacing, 'user-facing', 'syncer');
  manager.addSubprocess(supporting, 'supporting', 'replicator');

  parent.emit('SIGTERM');
  parent.emit('SIGTERM');
  parent.emit('SIGINT');
  expect(beforeDrain).toHaveBeenCalledTimes(1);
  expect(userFacing.kill).not.toHaveBeenCalled();
  expect(supporting.kill).not.toHaveBeenCalled();

  preparation.resolve();
  await preparation.promise;
  expect(userFacing.kill).toHaveBeenCalledExactlyOnceWith('SIGTERM');
  expect(supporting.kill).not.toHaveBeenCalled();
  userFacing.emit('close', 0, null);
  await manager.shutdown();
  await vi.waitFor(() =>
    expect(supporting.kill).toHaveBeenCalledExactlyOnceWith('SIGTERM'),
  );
});

test('forceful shutdown bypasses pending readiness preparation', async () => {
  const parent = new EventEmitter();
  const preparation = Promise.withResolvers<void>();
  const manager = new ProcessManager(lc, parent, () => preparation.promise);
  const worker = subprocess();
  manager.addSubprocess(worker, 'user-facing', 'syncer');

  parent.emit('SIGTERM');
  expect(worker.kill).not.toHaveBeenCalled();
  parent.emit('SIGQUIT');
  await manager.shutdown();
  await vi.waitFor(() =>
    expect(worker.kill).toHaveBeenCalledExactlyOnceWith('SIGQUIT'),
  );
  preparation.resolve();
  await preparation.promise;
  expect(worker.kill).toHaveBeenCalledTimes(1);
});

test('shutdown delay defaults to zero and rejects negative values', () => {
  const env = {ZERO_UPSTREAM_DB: 'postgresql://unused/unused'};
  expect(getZeroConfig({argv: [], env}).shutdownDelayMs).toBe(0);
  expect(() =>
    getZeroConfig({argv: [], env: {...env, ZERO_SHUTDOWN_DELAY_MS: '-1'}}),
  ).toThrow();
});
