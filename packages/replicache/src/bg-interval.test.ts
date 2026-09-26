import {LogContext} from '@rocicorp/logger';
import {resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, expect, test, vi} from 'vitest';
import {TestLogSink} from '../../shared/src/logging-test-utils.ts';
import {initBgIntervalProcess} from './bg-interval.ts';
import {IDBNotFoundError} from './kv/idb-store.ts';
import {StorageFailureError} from './storage-failure.ts';

beforeEach(() => {
  vi.useFakeTimers();
});

afterEach(() => {
  vi.useRealTimers();
  vi.restoreAllMocks();
});

test('initBgIntervalProcess starts interval that executes process with delayMs between each execution', async () => {
  let processCallCount = 0;
  const process = async () => {
    processCallCount++;
    await vi.advanceTimersByTimeAsync(50);
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).toBe(0);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(2);
  await vi.advanceTimersByTimeAsync(100);
  await vi.advanceTimersByTimeAsync(100);
  await vi.advanceTimersByTimeAsync(100);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(6);
});

test('initBgIntervalProcess starts interval that executes process with delayMs at 100 on even process call count and 50 on odd process call count', async () => {
  let processCallCount = 0;
  const process = async () => {
    processCallCount++;
    await vi.advanceTimersByTimeAsync(50);
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => {
      if (processCallCount % 2 === 0) {
        return 100;
      }
      return 50;
    },
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).toBe(0);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  await vi.advanceTimersByTimeAsync(50);
  expect(processCallCount).toBe(2);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(3);
  await vi.advanceTimersByTimeAsync(50);
  expect(processCallCount).toBe(4);
  await vi.advanceTimersByTimeAsync(50);
  expect(processCallCount).toBe(4);
  await vi.advanceTimersByTimeAsync(50);
  expect(processCallCount).toBe(5);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(6);
});

test('calling function returned by initBgIntervalProcess, stops interval', async () => {
  let processCallCount = 0;
  const process = () => {
    processCallCount++;
    return Promise.resolve();
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    new LogContext(),
    controller.signal,
  );

  expect(processCallCount).toBe(0);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  controller.abort();
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  await vi.advanceTimersByTimeAsync(400);
  expect(processCallCount).toBe(1);
});

test('error thrown during process (before stop is called) is logged to error', async () => {
  const lc = new LogContext();
  const errorStub = vi.spyOn(console, 'error').mockImplementation(() => {});
  const process = () => Promise.reject('TestErrorBeforeStop');
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  await vi.advanceTimersByTimeAsync(100);
  expect(errorStub).toHaveBeenCalledExactlyOnceWith(
    'bgIntervalProcess=testProcess',
    'Error running.',
    'TestErrorBeforeStop',
  );
});

test('IDBNotFoundError thrown during process is logged to info, not error', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('info', undefined, testLogSink);
  const idbError = new IDBNotFoundError('test db missing');
  const process = () => Promise.reject(idbError);
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  await vi.advanceTimersByTimeAsync(100);
  controller.abort();
  expect(testLogSink.messages).toEqual([
    [
      'info',
      {bgIntervalProcess: 'testProcess'},
      ['IndexedDB was deleted externally.', idbError],
    ],
  ]);
});

test('a storage failure thrown during process is logged to warn, reported, and stops the process', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, testLogSink);
  const failure = new StorageFailureError('io-error', 'disk I/O error');
  let processCallCount = 0;
  const process = () => {
    processCallCount++;
    return Promise.reject(failure);
  };
  const reported: StorageFailureError[] = [];
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
    f => reported.push(f),
  );
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  expect(reported).toEqual([failure]);
  expect(testLogSink.messages).toEqual([
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Starting']],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Running']],
    [
      'warn',
      {bgIntervalProcess: 'testProcess'},
      ['Storage failed; stopping.', failure],
    ],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Stopping']],
  ]);

  // Not run again: the store fails the same way at every interval.
  await vi.advanceTimersByTimeAsync(500);
  expect(processCallCount).toBe(1);
  expect(reported).toHaveLength(1);
});

test('a storage failure wrapped in another error is still recognized, with no callback attached', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('warn', undefined, testLogSink);
  const failure = new StorageFailureError('cannot-open', 'unable to open');
  const wrapped = new Error('Transaction failed', {cause: failure});
  let processCallCount = 0;
  const process = () => {
    processCallCount++;
    return Promise.reject(wrapped);
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  await vi.advanceTimersByTimeAsync(300);
  expect(processCallCount).toBe(1);
  expect(testLogSink.messages).toEqual([
    [
      'warn',
      {bgIntervalProcess: 'testProcess'},
      ['Storage failed; stopping.', wrapped],
    ],
  ]);
});

test('error thrown during process (after stop is called) is logged to debug', async () => {
  const testLogSink = new TestLogSink();
  const lc = new LogContext('debug', undefined, testLogSink);

  let processCallCount = 0;
  const processResolver = resolver();
  const process = () => {
    processCallCount++;
    return processResolver.promise;
  };
  const controller = new AbortController();
  initBgIntervalProcess(
    'testProcess',
    process,
    () => 100,
    lc,
    controller.signal,
  );
  expect(processCallCount).toBe(0);
  await vi.advanceTimersByTimeAsync(100);
  expect(processCallCount).toBe(1);
  controller.abort();
  processResolver.reject('TestErrorAfterStop');
  try {
    await processResolver.promise;
  } catch (e) {
    expect(e).toBe('TestErrorAfterStop');
  }
  expect(testLogSink.messages).toEqual([
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Starting']],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Running']],
    [
      'debug',
      {bgIntervalProcess: 'testProcess'},
      ['Error running most likely due to close.', 'TestErrorAfterStop'],
    ],
    ['debug', {bgIntervalProcess: 'testProcess'}, ['Stopping']],
  ]);
});
