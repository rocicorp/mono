import {beforeEach, expect, test, vi} from 'vitest';
import {AbortError} from './abort-error.ts';
import {sleep, sleepWithAbort} from './sleep.ts';

beforeEach(() => {
  vi.useFakeTimers({now: 0});
  return () => vi.useRealTimers();
});

test('sleep', async () => {
  let callCount = 0;
  const p = (async () => {
    await sleep(100);
    callCount++;
  })();
  await vi.advanceTimersByTimeAsync(99);
  expect(Date.now()).toEqual(99);
  expect(callCount).toEqual(0);

  await vi.advanceTimersByTimeAsync(1);
  expect(callCount).toEqual(1);
  expect(Date.now()).toEqual(100);

  await vi.advanceTimersByTimeAsync(100);
  expect(callCount).toEqual(1);
  expect(Date.now()).toEqual(200);

  await p;
  expect(Date.now()).toEqual(200);
});

test('sleep abort', async () => {
  const controller = new AbortController();
  const p = sleep(100, controller.signal);
  controller.abort();
  let e;
  try {
    expect(Date.now()).toEqual(0);
    await p;
  } catch (err) {
    e = err;
  }
  expect(Date.now()).toEqual(0);

  expect(e).toBeInstanceOf(AbortError);

  await vi.advanceTimersByTimeAsync(100);
  expect(Date.now()).toEqual(100);

  await vi.advanceTimersByTimeAsync(100);
  expect(Date.now()).toEqual(200);
});

test('sleepWithAbort', async () => {
  let okResolved = false;
  let abortedResolved = false;
  const controller = new AbortController();
  const [p, abortedP] = sleepWithAbort(100, controller.signal);
  void p.then(() => {
    okResolved = true;
  });
  void abortedP.then(() => {
    abortedResolved = true;
  });

  await vi.advanceTimersByTimeAsync(50);
  controller.abort();
  expect(okResolved).toEqual(false);
  expect(abortedResolved).toEqual(false);
  expect(Date.now()).toEqual(50);

  await vi.advanceTimersByTimeAsync(0);
  expect(okResolved).toEqual(false);
  expect(abortedResolved).toEqual(true);
  expect(Date.now()).toEqual(50);

  await vi.advanceTimersByTimeAsync(50);
  expect(okResolved).toEqual(false);
});

test.each([100, 0])(
  'sleep with abort signal already aborted ms=%s',
  async ms => {
    const controller = new AbortController();
    controller.abort();
    let e;
    try {
      await sleep(ms, controller.signal);
    } catch (err) {
      e = err;
    }
    expect(e).toBeInstanceOf(AbortError);
  },
);
