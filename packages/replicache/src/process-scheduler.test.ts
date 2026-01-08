import {resolver, type Resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../shared/src/abort-error.ts';
import {ProcessScheduler} from './process-scheduler.ts';
import {expectPromiseToReject} from './test-util.ts';

describe('ProcessScheduler', () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  async function aFewMicrotasks(num = 10) {
    for (let i = 0; i < num; i++) {
      await Promise.resolve();
    }
  }

  test('runs process on idle with specified idleTimeoutMs', async () => {
    let testProcessCallCount = 0;
    // oxlint-disable-next-line require-await
    const testProcess = async () => {
      testProcessCallCount++;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolver = resolver();
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      return requestIdleResolver.promise;
    };
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      new AbortController().signal,
      requestIdle,
    );
    const result = scheduler.schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    await result;
    expect(testProcessCallCount).toBe(1);
  });

  test('rejects if process rejects', async () => {
    let testProcessCallCount = 0;
    let testProcessError;
    // oxlint-disable-next-line require-await
    const testProcess = async () => {
      testProcessCallCount++;
      testProcessError = new Error('testProcess error');
      throw testProcessError;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolver = resolver();
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      return requestIdleResolver.promise;
    };
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      new AbortController().signal,
      requestIdle,
    );
    const result = scheduler.schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    let expectedE;
    try {
      await result;
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).toBe(testProcessError);
    expect(testProcessCallCount).toBe(1);
  });

  test('runs immediately bypassing throttle and idle when run() is called', async () => {
    let testProcessCallCount = 0;
    const testProcess = async () => {
      testProcessCallCount++;
    };
    // requestIdle that never resolves to prove it's skipped
    const requestIdle = () => new Promise<void>(() => {});

    // throttleMs to prove it's skipped
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      5000,
      new AbortController().signal,
      requestIdle,
    );

    const result = scheduler.run();
    await aFewMicrotasks();

    // Should run immediately
    expect(testProcessCallCount).toBe(1);
    await result;
  });

  // Scenario 1: run() call while a schedule() call has a process running
  test('run() waits for currently running process (scheduled)', async () => {
    const processResolvers: Resolver<void>[] = [];
    const testProcess = async () => {
      const r = resolver();
      processResolvers.push(r);
      await r.promise;
    };
    const requestIdle = (_ms: number) => Promise.resolve();

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      0,
      new AbortController().signal,
      requestIdle,
    );

    // Start a scheduled process
    const schedulePromise = scheduler.schedule();
    await aFewMicrotasks();

    // It should be running now (since idle resolves immediately)
    expect(processResolvers.length).toBe(1);

    // Call run()
    const runPromise = scheduler.run();
    await aFewMicrotasks();

    // Should wait for first process to finish
    expect(processResolvers.length).toBe(1);

    // Finish first process
    processResolvers[0].resolve();
    await schedulePromise;
    await aFewMicrotasks();

    // Now run() should start
    expect(processResolvers.length).toBe(2);

    // Finish second process
    processResolvers[1].resolve();
    await runPromise;
  });

  // Scenario 2: run() call while a schedule() call is waiting on throttle
  test('run() preempts schedule() waiting on throttle', async () => {
    let callCount = 0;
    const testProcess = async () => {
      callCount++;
    };
    const requestIdle = (_ms: number) => Promise.resolve();

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      5000, // Very long throttle
      new AbortController().signal,
      requestIdle,
    );

    // Initial run to start throttle timer
    await scheduler.run();
    expect(callCount).toBe(1);

    // Schedule next run - should wait on throttle
    const schedulePromise = scheduler.schedule();
    await aFewMicrotasks();
    expect(callCount).toBe(1); // Still waiting

    // Call run() - should preempt
    const runPromise = scheduler.run();
    await aFewMicrotasks();

    // Run() should execute immediately and fulfill BOTH promises
    expect(callCount).toBe(2);

    await runPromise;
    await schedulePromise;
  });

  // Scenario 3: run() call while a schedule() call is waiting on idle
  test('run() preempts schedule() waiting on idle', async () => {
    let callCount = 0;
    const testProcess = async () => {
      callCount++;
    };
    const requestIdleResolver = resolver();
    const requestIdle = (_ms: number) => requestIdleResolver.promise;

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      0,
      new AbortController().signal,
      requestIdle,
    );

    // Schedule run - should wait on idle
    const schedulePromise = scheduler.schedule();
    await aFewMicrotasks();
    expect(callCount).toBe(0);

    // Call run() - should preempt
    const runPromise = scheduler.run();
    await aFewMicrotasks();

    // Run() should execute immediately (ignoring idle) and fulfill BOTH
    expect(callCount).toBe(1);

    await runPromise;
    await schedulePromise;
  });

  // Scenario 4: run() call while a schedule() call has a process running and another schedule() call is queued
  test('run() chains correctly when schedule() running and another schedule() queued', async () => {
    const processResolvers: Resolver<void>[] = [];
    const testProcess = async () => {
      const r = resolver();
      processResolvers.push(r);
      await r.promise;
    };
    const requestIdle = (_ms: number) => Promise.resolve();

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      0,
      new AbortController().signal,
      requestIdle,
    );

    // 1. Start scheduled process A
    const scheduleA = scheduler.schedule();
    await aFewMicrotasks();
    expect(processResolvers.length).toBe(1); // A running

    // 2. Queue schedule B (waits for A)
    const scheduleB = scheduler.schedule();
    await aFewMicrotasks();

    // 3. Call run() C (steals B, waits for A)
    const runC = scheduler.run();
    await aFewMicrotasks();

    // A is still running. C is waiting for A. B is stolen by C.
    expect(processResolvers.length).toBe(1);

    // Finish A
    processResolvers[0].resolve();
    await scheduleA;
    await aFewMicrotasks();

    // C should act immediately (since start of C's run logic), creating process 2
    expect(processResolvers.length).toBe(2);

    // Finish C (which fulfills B)
    processResolvers[1].resolve();
    await runC;
    await scheduleB;
  });

  // Scenario 5: schedule() call while a run() call is running
  test('schedule() waits for currently running run()', async () => {
    const processResolvers: Resolver<void>[] = [];
    const testProcess = async () => {
      const r = resolver();
      processResolvers.push(r);
      await r.promise;
    };
    const requestIdle = (_ms: number) => Promise.resolve();

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      0,
      new AbortController().signal,
      requestIdle,
    );

    // Start run() A
    const runA = scheduler.run();
    await aFewMicrotasks();
    expect(processResolvers.length).toBe(1); // A running

    // Schedule B (waits for A)
    const scheduleB = scheduler.schedule();
    await aFewMicrotasks();

    // Finish A
    processResolvers[0].resolve();
    await runA;
    await aFewMicrotasks();

    // B starts
    expect(processResolvers.length).toBe(2);

    // Finish B
    processResolvers[1].resolve();
    await scheduleB;
  });

  // Scenario 6: schedule() call while a run() call is waiting on a process run from a previous schedule() call
  test('schedule() queues correctly while run() is waiting on previous schedule', async () => {
    const processResolvers: Resolver<void>[] = [];
    const testProcess = async () => {
      const r = resolver();
      processResolvers.push(r);
      await r.promise;
    };
    const requestIdle = (_ms: number) => Promise.resolve();

    const scheduler = new ProcessScheduler(
      testProcess,
      100,
      0,
      new AbortController().signal,
      requestIdle,
    );

    // 1. Start scheduled A
    const scheduleA = scheduler.schedule();
    await aFewMicrotasks();
    expect(processResolvers.length).toBe(1); // A running

    // 2. Call run() B (waits for A)
    const runB = scheduler.run();
    await aFewMicrotasks();

    // 3. Schedule C (waits for B? No, queues via scheduleInternal logic)
    // scheduleInternal for C will await "runPromise" which is now B's promise
    const scheduleC = scheduler.schedule();
    await aFewMicrotasks();

    // Finish A
    processResolvers[0].resolve();
    await scheduleA;
    await aFewMicrotasks();

    // B starts
    expect(processResolvers.length).toBe(2);

    // Finish B
    processResolvers[1].resolve();
    await runB;
    await aFewMicrotasks();

    // C starts
    expect(processResolvers.length).toBe(3);

    // Finish C
    processResolvers[2].resolve();
    await scheduleC;
  });

  test('multiple calls to schedule while process is running are fullfilled by one process run', async () => {
    let testProcessCallCount = 0;
    const testProcessResolvers: Resolver<void>[] = [];
    const testProcess = () => {
      testProcessCallCount++;
      const r = resolver();
      testProcessResolvers.push(r);
      return r.promise;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolvers: Resolver<void>[] = [];
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      const r = resolver();
      requestIdleResolvers.push(r);
      return r.promise;
    };
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      new AbortController().signal,
      requestIdle,
    );
    const resolved: number[] = [];
    let scheduleCallCount = 0;
    function schedule() {
      const result = scheduler.schedule();
      const scheduleOrder = ++scheduleCallCount;
      void result.then(() => resolved.push(scheduleOrder));
      return result;
    }

    const result1 = schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).toBe(result2);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).not.toBe(result3);
    expect(result3).toBe(result4);
    expect(testProcessCallCount).toBe(1);
    testProcessResolvers[0].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2]);
    await result1;
    await result2;

    expect(requestIdleCalls.length).toBe(2);
    expect(requestIdleCalls[1]).toBe(1234);
    // schedule during second scheduled process idle
    const result5 = schedule();
    expect(result4).toBe(result5);
    expect(testProcessCallCount).toBe(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(2);
    // schedule during second process run
    const result6 = schedule();
    const result7 = schedule();
    expect(result5).not.toBe(result6);
    expect(result6).toBe(result7);
    expect(testProcessCallCount).toBe(2);
    testProcessResolvers[1].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2, 3, 4, 5]);
    await result3;
    await result4;
    await result5;

    expect(requestIdleCalls.length).toBe(3);
    expect(requestIdleCalls[2]).toBe(1234);
    expect(testProcessCallCount).toBe(2);
    requestIdleResolvers[2].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(3);
    testProcessResolvers[2].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2, 3, 4, 5, 6, 7]);
    await result6;
    await result7;
  });

  test('rejects if process rejects with multiple debounced calls', async () => {
    let testProcessCallCount = 0;
    const testProcessResolvers: Resolver<void>[] = [];
    const testProcess = () => {
      testProcessCallCount++;
      const r = resolver();
      testProcessResolvers.push(r);
      return r.promise;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolvers: Resolver<void>[] = [];
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      const r = resolver();
      requestIdleResolvers.push(r);
      return r.promise;
    };
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      new AbortController().signal,
      requestIdle,
    );
    const rejected: number[] = [];
    let scheduleCallCount = 0;
    function schedule() {
      const result = scheduler.schedule();
      const scheduleOrder = ++scheduleCallCount;
      void result.catch(() => rejected.push(scheduleOrder));
      return result;
    }

    const result1 = schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).toBe(result2);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).not.toBe(result3);
    expect(result3).toBe(result4);
    expect(testProcessCallCount).toBe(1);
    const testProcessError1 = new Error('testProcess error 1');
    testProcessResolvers[0].reject(testProcessError1);
    await aFewMicrotasks();
    expect(rejected).toEqual([1, 2]);
    (await expectPromiseToReject(result1)).toBe(testProcessError1);
    (await expectPromiseToReject(result2)).toBe(testProcessError1);

    expect(requestIdleCalls.length).toBe(2);
    expect(requestIdleCalls[1]).toBe(1234);
    // schedule during second scheduled process idle
    const result5 = schedule();
    expect(result4).toBe(result5);
    expect(testProcessCallCount).toBe(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(2);
    // schedule during second process run
    const result6 = schedule();
    const result7 = schedule();
    expect(result5).not.toBe(result6);
    expect(result6).toBe(result7);
    expect(testProcessCallCount).toBe(2);
    const testProcessError2 = new Error('testProcess error 2');
    testProcessResolvers[1].reject(testProcessError2);
    await aFewMicrotasks();
    expect(rejected).toEqual([1, 2, 3, 4, 5]);
    (await expectPromiseToReject(result3)).toBe(testProcessError2);
    (await expectPromiseToReject(result4)).toBe(testProcessError2);
    (await expectPromiseToReject(result5)).toBe(testProcessError2);
  });

  test('process runs are throttled so that the process runs at most once every throttleMs', async () => {
    let testProcessCallCount = 0;
    const testProcessResolvers: Resolver<void>[] = [];
    const testProcess = () => {
      testProcessCallCount++;
      const r = resolver();
      testProcessResolvers.push(r);
      return r.promise;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolvers: Resolver<void>[] = [];
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      const r = resolver();
      requestIdleResolvers.push(r);
      return r.promise;
    };
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      250,
      new AbortController().signal,
      requestIdle,
    );
    const resolved: number[] = [];
    let scheduleCallCount = 0;
    function schedule() {
      const result = scheduler.schedule();
      const scheduleOrder = ++scheduleCallCount;
      void result.then(() => resolved.push(scheduleOrder));
      return result;
    }

    const result1 = schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).toBe(result2);
    // make idle take 100 ms
    await vi.advanceTimersByTimeAsync(100);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).not.toBe(result3);
    expect(result3).toBe(result4);
    expect(testProcessCallCount).toBe(1);
    // make process take 200ms
    await vi.advanceTimersByTimeAsync(200);
    testProcessResolvers[0].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2]);
    await result1;
    await result2;

    // not called yet because 250ms hasn't elapsed since last
    // process run started (100 ms idle doesn't count, only 200ms run does)
    expect(requestIdleCalls.length).toBe(1);
    // schedule during second scheduled process throttle
    const result5 = schedule();
    expect(result4).toBe(result5);
    await vi.advanceTimersByTimeAsync(50);
    await aFewMicrotasks();
    // now 250ms has elapsed
    expect(requestIdleCalls.length).toBe(2);
    expect(requestIdleCalls[1]).toBe(1234);
    // schedule during second scheduled process idle
    const result6 = schedule();
    expect(result5).toBe(result6);
    expect(testProcessCallCount).toBe(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(2);
    // schedule during second process run
    const result7 = schedule();
    const result8 = schedule();
    expect(result6).not.toBe(result7);
    expect(result7).toBe(result8);
    expect(testProcessCallCount).toBe(2);
    // make second process run take 250ms
    await vi.advanceTimersByTimeAsync(250);
    testProcessResolvers[1].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2, 3, 4, 5, 6]);
    await result3;
    await result4;
    await result5;
    await result6;

    // already 3 because 250ms has elapsed since
    // last process run started (250ms run time)
    expect(requestIdleCalls.length).toBe(3);
    expect(requestIdleCalls[2]).toBe(1234);
    expect(testProcessCallCount).toBe(2);
    requestIdleResolvers[2].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(3);
    testProcessResolvers[2].resolve();
    await aFewMicrotasks();
    expect(resolved).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    await result7;
    await result8;
  });

  test('rejects with AbortError if AbortSignal is already aborted', async () => {
    let testProcessCallCount = 0;
    // oxlint-disable-next-line require-await
    const testProcess = async () => {
      testProcessCallCount++;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolver = resolver();
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      return requestIdleResolver.promise;
    };
    const abortController = new AbortController();
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      abortController.signal,
      requestIdle,
    );
    abortController.abort();
    (await expectPromiseToReject(scheduler.schedule())).toBeInstanceOf(
      AbortError,
    );
    expect(testProcessCallCount).toBe(0);
  });

  test('rejects with AbortError when running', async () => {
    let testProcessCallCount = 0;
    const testProcessResolver = resolver();
    const testProcess = () => {
      testProcessCallCount++;
      return testProcessResolver.promise;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolver = resolver();
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      return requestIdleResolver.promise;
    };
    const abortController = new AbortController();
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      abortController.signal,
      requestIdle,
    );
    const result = scheduler.schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    abortController.abort();
    (await expectPromiseToReject(result)).toBeInstanceOf(AbortError);
  });

  test('rejects with AbortError both running and waiting', async () => {
    let testProcessCallCount = 0;
    const testProcessResolver = resolver();
    const testProcess = () => {
      testProcessCallCount++;
      return testProcessResolver.promise;
    };
    const requestIdleCalls: number[] = [];
    const requestIdleResolver = resolver();
    const requestIdle = (idleTimeoutMs: number) => {
      requestIdleCalls.push(idleTimeoutMs);
      return requestIdleResolver.promise;
    };
    const abortController = new AbortController();
    const scheduler = new ProcessScheduler(
      testProcess,
      1234,
      0,
      abortController.signal,
      requestIdle,
    );
    const result1 = scheduler.schedule();
    const result2 = scheduler.schedule();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(0);
    expect(requestIdleCalls.length).toBe(1);
    expect(requestIdleCalls[0]).toBe(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).toBe(1);
    abortController.abort();
    (await expectPromiseToReject(result1)).toBeInstanceOf(AbortError);
    (await expectPromiseToReject(result2)).toBeInstanceOf(AbortError);
  });
});
