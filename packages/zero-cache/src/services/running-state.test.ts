import {AbortError} from 'shared/src/abort-error.js';
import {createSilentLogContext} from 'shared/src/logging-test-utils.js';
import {expect, test, vi} from 'vitest';
import {RunningState} from './running-state.js';

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
    mockSleep,
  );

  for (let i = 0; i < 8; i++) {
    void state.backoff(lc);
  }
  void state.resetBackoff();
  void state.backoff(lc);
  void state.backoff(lc);

  expect(mockSleep.mock.calls.map(call => call[0])).toEqual([
    1000, 2000, 4000, 8000, 13_000, 13_000, 13_000, 13_000, 1000, 2000,
  ]);
});

test('cancel backoff on stop', async () => {
  const state = new RunningState('foo-service', {initialRetryDelay: 100_000});

  const timeout = state.backoff(lc);
  state.stop(lc);
  await timeout;
});

test('backoff on AbortError', async () => {
  const state = new RunningState('foo-service', {initialRetryDelay: 100_000});
  await state.backoff(lc, new AbortError());
  expect(state.shouldRun()).toBe(false);
});
