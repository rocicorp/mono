import {describe, test, expect} from '@jest/globals';
import {LoggingLock} from './lock.js';
import {LogContext} from '@rocicorp/logger';
import {TestLogSink, createSilentLogContext} from './test-utils.js';
import {must} from 'shared/src/must.js';
import {sleep} from './sleep.js';

describe('LoggingLock', () => {
  test('logs lock-acquired and lock-held timings', async () => {
    const lock = new LoggingLock();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const inLock = new Signal();
    const releaseLock = new Signal();
    void lock.withLock(createSilentLogContext(), 'first', async () => {
      inLock.notify();
      await releaseLock.notification();
    });

    await inLock.notification();
    setTimeout(() => releaseLock.notify(), 2);
    await lock.withLock(lc, 'logic', async () => {
      await sleep(1); // Must be >0ms to result in logging
    });
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toEqual({
      function: 'logic',
      timing: 'lock-acquired',
    });
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][1]).toEqual({
      function: 'logic',
      timing: 'lock-held',
    });
  });

  test('logs at info level above threshold', async () => {
    const lock = new LoggingLock();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const inLock = new Signal();
    const releaseLock = new Signal();
    void lock.withLock(createSilentLogContext(), 'first', async () => {
      inLock.notify();
      await releaseLock.notification();
    });

    await inLock.notification();
    setTimeout(() => releaseLock.notify(), 2);
    await lock.withLock(
      lc,
      'logic',
      async () => {
        await sleep(2); // Must be >0ms to result in logging
      },
      1, // Log at INFO if held for more than 1 ms
    );
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toEqual({
      function: 'logic',
      timing: 'lock-acquired',
    });
    expect(sink.messages[1][0]).toBe('info');
    expect(sink.messages[1][1]).toEqual({
      function: 'logic',
      timing: 'lock-held',
    });
  });

  test('logs multiple waiters', async () => {
    const lock = new LoggingLock();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const inLock = new Signal();
    const releaseFirstLock = new Signal();
    void lock.withLock(createSilentLogContext(), 'slow', async () => {
      inLock.notify();
      await releaseFirstLock.notification();
    });

    await inLock.notification();

    const releaseSecondLock = new Signal();
    const waiters: Promise<void>[] = [];
    const pushWaiter = () => {
      waiters.push(
        lock.withLock(lc, `logic`, async () => {
          await releaseSecondLock.notification();
        }),
      );
    };

    pushWaiter();
    pushWaiter();

    await sleep(1);
    await lc.flush();

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toEqual({
      function: 'logic',
    });
    expect(sink.messages[0][2][0]).toMatch(
      'logic waiting for slow with 1 other waiter(s): logic,logic',
    );

    pushWaiter();
    await sleep(2);
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][1]).toEqual({
      function: 'logic',
    });
    expect(sink.messages[1][2][0]).toMatch(
      'logic waiting for slow with 2 other waiter(s): logic,logic,logic',
    );

    releaseFirstLock.notify();
    releaseSecondLock.notify();

    await Promise.all(waiters);
  });
});

class Signal {
  #promise: Promise<void>;
  #resolve: undefined | ((value: void | PromiseLike<void>) => void) = undefined;

  constructor() {
    this.#promise = new Promise(resolve => {
      this.#resolve = resolve;
    });
  }

  notification(): Promise<void> {
    return this.#promise;
  }

  notify() {
    must(this.#resolve)();
  }
}
