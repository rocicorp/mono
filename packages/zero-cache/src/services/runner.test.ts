import {resolver} from '@rocicorp/resolver';
import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {sleep} from '../../../shared/src/sleep.ts';
import {ServiceRunner} from './runner.ts';
import type {Service} from './service.ts';

describe('services/runner', () => {
  class TestService implements Service {
    readonly id: string;
    resolver = resolver<void>();
    valid = true;

    constructor(id: string) {
      this.id = id;
    }

    run(): Promise<void> {
      return this.resolver.promise;
    }

    // oxlint-disable-next-line require-await
    async stop(): Promise<void> {
      this.resolver.resolve();
    }
  }

  const runner = new ServiceRunner<TestService>(
    createSilentLogContext(),
    (id: string) => new TestService(id),
    (s: TestService) => s.valid,
  );

  test('caching', () => {
    const s1 = runner.getService('foo');
    const s2 = runner.getService('bar');
    const s3 = runner.getService('foo');

    expect(s1).toBe(s3);
    expect(s1).not.toBe(s2);
  });

  test('stopped', async () => {
    const s1 = runner.getService('foo');
    s1.resolver.resolve();

    await sleep(1);
    const s2 = runner.getService('foo');
    expect(s1).not.toBe(s2);
  });

  test('fails', async () => {
    const s1 = runner.getService('foo');
    s1.resolver.reject('foo');

    await sleep(1);
    const s2 = runner.getService('foo');
    expect(s1).not.toBe(s2);
  });

  test('validity', () => {
    const s1 = runner.getService('foo');
    s1.valid = false;

    const s2 = runner.getService('foo');
    expect(s1).not.toBe(s2);
  });

  // Models the zombie ViewSyncer scenario: a service whose run() blocks on
  // an unresolved initialization promise should be cleaned up when that
  // promise is rejected (e.g. when all clients disconnect before
  // initConnection completes).
  test('zombie cleanup on rejected initialization', async () => {
    const initialized = resolver<void>();
    let runCompleted = false;

    const zombieRunner = new ServiceRunner<TestService>(
      createSilentLogContext(),
      (id: string) => {
        const service = new TestService(id);
        // Override run() to block on the initialization promise,
        // modeling ViewSyncerService.run() blocking on readyState().
        service.run = async () => {
          try {
            await initialized.promise;
          } catch {
            // Rejection means shutdown before initialization.
          }
          runCompleted = true;
        };
        return service;
      },
      () => true,
    );

    // Service is created and run() starts, blocking on initialized.promise
    zombieRunner.getService('zombie');
    expect(zombieRunner.size).toBe(1);

    // Without the fix, rejecting the initialization promise would never
    // happen in the idle-shutdown path, leaving the service as a zombie.
    // With the fix, the shutdown path rejects #initialized, which
    // unblocks run() and allows cleanup.
    initialized.reject('shut down before initialization completed');
    await sleep(1);

    expect(runCompleted).toBe(true);
    expect(zombieRunner.size).toBe(0);
  });
});
