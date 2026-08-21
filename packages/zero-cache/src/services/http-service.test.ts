import {resolver} from '@rocicorp/resolver';
import type {FastifyInstance} from 'fastify';
import {afterEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {HttpService} from './http-service.ts';

const lc = createSilentLogContext();

let service: HttpService | undefined;

afterEach(async () => {
  await service?.stop();
  service = undefined;
});

function startService(readinessGate?: Promise<void>) {
  service = new HttpService(
    'test-service',
    lc,
    {port: 0, keepaliveTimeoutMs: undefined, readinessGate},
    (_fastify: FastifyInstance) => {},
  );
  return service.start();
}

describe.each(['/', '/keepalive'])('%s', path => {
  test('responds OK once ready by default', async () => {
    const address = await startService();

    const res = await fetch(address + path);
    expect(res.status).toBe(200);
    expect(await res.text()).toBe('OK');
  });

  test('never responds to a request made before the readiness gate resolves', async () => {
    const {promise: readinessGate, resolve} = resolver<void>();
    const address = await startService(readinessGate);

    // The handler sends no response at all while not ready, so a request
    // in flight before readiness hangs forever, even after the gate later
    // resolves (the handler already ran and won't be invoked again).
    const controller = new AbortController();
    const pending = fetch(address + path, {signal: controller.signal});
    let settled = false;
    void pending.then(
      () => (settled = true),
      () => (settled = true),
    );

    await new Promise(r => setTimeout(r, 50));
    expect(settled).toBe(false);

    resolve();
    await new Promise(r => setTimeout(r, 50));
    expect(settled).toBe(false);
    controller.abort();

    // A new request made after the gate resolves succeeds normally.
    const res = await fetch(address + path);
    expect(await res.text()).toBe('OK');
  });
});
