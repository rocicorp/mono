/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {expect, test, vi} from 'vitest';
import {AbortError} from '../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {RunningState} from './running-state.ts';

const lc = createSilentLogContext();

test('cancelOnStop', () => {
  const state = new RunningState('foo-service');

  const cancelable1 = {cancel: vi.fn()};
  const cancelable2 = {cancel: vi.fn()};
  const cancelable3 = {cancel: vi.fn()};

  state.cancelOnStop(cancelable1);
  const unregister = state.cancelOnStop(cancelable2);
  state.cancelOnStop(cancelable3);

  unregister();
  state.stop(lc);

  expect(cancelable1.cancel).toHaveBeenCalledOnce();
  expect(cancelable2.cancel).not.toHaveBeenCalled();
  expect(cancelable3.cancel).toHaveBeenCalledOnce();
});

test('backoff', () => {
  const mockSleep = vi
    .fn()
    .mockImplementation(() => [Promise.resolve(), Promise.resolve()]);
  const state = new RunningState(
    'foo-service',
    {initialRetryDelay: 1000, maxRetryDelay: 13_000},
    setTimeout,
    mockSleep,
  );

  for (let i = 0; i < 8; i++) {
    void state.backoff(lc, 'any error');
  }
  void state.resetBackoff();
  void state.backoff(lc, 'any error');
  void state.backoff(lc, 'any error');

  expect(mockSleep.mock.calls.map(call => call[0])).toEqual([
    1000, 2000, 4000, 8000, 13_000, 13_000, 13_000, 13_000, 1000, 2000,
  ]);
});

test('cancel backoff on stop', async () => {
  const state = new RunningState('foo-service', {initialRetryDelay: 100_000});

  const timeout = state.backoff(lc, 'any error');
  state.stop(lc);
  await timeout;
});

test('backoff on AbortError', async () => {
  const state = new RunningState('foo-service', {initialRetryDelay: 100_000});
  await state.backoff(lc, new AbortError());
  expect(state.shouldRun()).toBe(false);
});
