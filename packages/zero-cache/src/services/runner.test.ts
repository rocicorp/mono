/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
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
