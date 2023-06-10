import {resolver, Resolver} from '@rocicorp/resolver';
import {expect} from 'chai';
import {AbortError} from 'shared/abort-error.js';
import sinon, {SinonFakeTimers, useFakeTimers} from 'sinon';
import {ProcessScheduler} from './process-scheduler.js';
import {expectPromiseToReject} from './test-util.js';

suite('ProcessScheduler', () => {
  let clock: SinonFakeTimers;
  setup(() => {
    clock = useFakeTimers();
  });

  teardown(() => {
    clock.restore();
    sinon.restore();
  });

  async function aFewMicrotasks(num = 10) {
    for (let i = 0; i < num; i++) {
      await Promise.resolve();
    }
  }

  test('runs process on idle with specified idleTimeoutMs', async () => {
    let testProcessCallCount = 0;
    // eslint-disable-next-line require-await
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
    expect(testProcessCallCount).to.equal(0);
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    await result;
    expect(testProcessCallCount).to.equal(1);
  });

  test('rejects if process rejects', async () => {
    let testProcessCallCount = 0;
    let testProcessError;
    // eslint-disable-next-line require-await
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
    expect(testProcessCallCount).to.equal(0);
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    let expectedE;
    try {
      await result;
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).to.equal(testProcessError);
    expect(testProcessCallCount).to.equal(1);
  });

  test('rejects if process rejects', async () => {
    let testProcessCallCount = 0;
    let testProcessError;
    // eslint-disable-next-line require-await
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
    expect(testProcessCallCount).to.equal(0);
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    let expectedE;
    try {
      await result;
    } catch (e) {
      expectedE = e;
    }
    expect(expectedE).to.equal(testProcessError);
    expect(testProcessCallCount).to.equal(1);
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
    expect(testProcessCallCount).to.equal(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).to.equal(result2);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).to.not.equal(result3);
    expect(result3).to.equal(result4);
    expect(testProcessCallCount).to.equal(1);
    testProcessResolvers[0].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2]);
    await result1;
    await result2;

    expect(requestIdleCalls.length).to.equal(2);
    expect(requestIdleCalls[1]).to.equal(1234);
    // schedule during second scheduled process idle
    const result5 = schedule();
    expect(result4).to.equal(result5);
    expect(testProcessCallCount).to.equal(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(2);
    // schedule during second process run
    const result6 = schedule();
    const result7 = schedule();
    expect(result5).to.not.equal(result6);
    expect(result6).to.equal(result7);
    expect(testProcessCallCount).to.equal(2);
    testProcessResolvers[1].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2, 3, 4, 5]);
    await result3;
    await result4;
    await result5;

    expect(requestIdleCalls.length).to.equal(3);
    expect(requestIdleCalls[2]).to.equal(1234);
    expect(testProcessCallCount).to.equal(2);
    requestIdleResolvers[2].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(3);
    testProcessResolvers[2].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2, 3, 4, 5, 6, 7]);
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
    expect(testProcessCallCount).to.equal(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).to.equal(result2);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).to.not.equal(result3);
    expect(result3).to.equal(result4);
    expect(testProcessCallCount).to.equal(1);
    const testProcessError1 = new Error('testProcess error 1');
    testProcessResolvers[0].reject(testProcessError1);
    await aFewMicrotasks();
    expect(rejected).to.deep.equal([1, 2]);
    (await expectPromiseToReject(result1)).to.equal(testProcessError1);
    (await expectPromiseToReject(result2)).to.equal(testProcessError1);

    expect(requestIdleCalls.length).to.equal(2);
    expect(requestIdleCalls[1]).to.equal(1234);
    // schedule during second scheduled process idle
    const result5 = schedule();
    expect(result4).to.equal(result5);
    expect(testProcessCallCount).to.equal(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(2);
    // schedule during second process run
    const result6 = schedule();
    const result7 = schedule();
    expect(result5).to.not.equal(result6);
    expect(result6).to.equal(result7);
    expect(testProcessCallCount).to.equal(2);
    const testProcessError2 = new Error('testProcess error 2');
    testProcessResolvers[1].reject(testProcessError2);
    await aFewMicrotasks();
    expect(rejected).to.deep.equal([1, 2, 3, 4, 5]);
    (await expectPromiseToReject(result3)).to.equal(testProcessError2);
    (await expectPromiseToReject(result4)).to.equal(testProcessError2);
    (await expectPromiseToReject(result5)).to.equal(testProcessError2);
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
    expect(testProcessCallCount).to.equal(0);
    await aFewMicrotasks();
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    // schedule during first scheduled process idle
    const result2 = schedule();
    expect(result1).to.equal(result2);
    // make idle take 100 ms
    await clock.tickAsync(100);
    requestIdleResolvers[0].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    // schedule during first scheduled process run
    const result3 = schedule();
    const result4 = schedule();
    expect(result1).to.not.equal(result3);
    expect(result3).to.equal(result4);
    expect(testProcessCallCount).to.equal(1);
    // make process take 200ms
    await clock.tickAsync(200);
    testProcessResolvers[0].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2]);
    await result1;
    await result2;

    // not called yet because 250ms hasn't elapsed since last
    // process run started (100 ms idle doesn't count, only 200ms run does)
    expect(requestIdleCalls.length).to.equal(1);
    // schedule during second scheduled process throttle
    const result5 = schedule();
    expect(result4).to.equal(result5);
    await clock.tickAsync(50);
    await aFewMicrotasks();
    // now 250ms has elapsed
    expect(requestIdleCalls.length).to.equal(2);
    expect(requestIdleCalls[1]).to.equal(1234);
    // schedule during second scheduled process idle
    const result6 = schedule();
    expect(result5).to.equal(result6);
    expect(testProcessCallCount).to.equal(1);
    requestIdleResolvers[1].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(2);
    // schedule during second process run
    const result7 = schedule();
    const result8 = schedule();
    expect(result6).to.not.equal(result7);
    expect(result7).to.equal(result8);
    expect(testProcessCallCount).to.equal(2);
    // make second process run take 250ms
    await clock.tickAsync(250);
    testProcessResolvers[1].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2, 3, 4, 5, 6]);
    await result3;
    await result4;
    await result5;
    await result6;

    // already 3 because 250ms has elapsed since
    // last process run started (250ms run time)
    expect(requestIdleCalls.length).to.equal(3);
    expect(requestIdleCalls[2]).to.equal(1234);
    expect(testProcessCallCount).to.equal(2);
    requestIdleResolvers[2].resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(3);
    testProcessResolvers[2].resolve();
    await aFewMicrotasks();
    expect(resolved).to.deep.equal([1, 2, 3, 4, 5, 6, 7, 8]);
    await result7;
    await result8;
  });

  test('rejects with AbortError if AbortSignal is already aborted', async () => {
    let testProcessCallCount = 0;
    // eslint-disable-next-line require-await
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
    (await expectPromiseToReject(scheduler.schedule())).to.be.instanceOf(
      AbortError,
    );
    expect(testProcessCallCount).to.equal(0);
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
    expect(testProcessCallCount).to.equal(0);
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    abortController.abort();
    (await expectPromiseToReject(result)).to.be.instanceOf(AbortError);
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
    expect(testProcessCallCount).to.equal(0);
    expect(requestIdleCalls.length).to.equal(1);
    expect(requestIdleCalls[0]).to.equal(1234);
    requestIdleResolver.resolve();
    await aFewMicrotasks();
    expect(testProcessCallCount).to.equal(1);
    abortController.abort();
    (await expectPromiseToReject(result1)).to.be.instanceOf(AbortError);
    (await expectPromiseToReject(result2)).to.be.instanceOf(AbortError);
  });
});
