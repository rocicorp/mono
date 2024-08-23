import {resolver} from '@rocicorp/resolver';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {sleep} from 'shared/src/sleep.js';
import {describe, expect, test} from 'vitest';
import {ServiceRunner} from './runner.js';
import {Service} from './service.js';

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

    // eslint-disable-next-line require-await
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
});
