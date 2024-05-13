import {describe, test, expect, jest} from '@jest/globals';
import {LoggingLock} from './lock.js';
import {LogContext} from '@rocicorp/logger';
import {
  TestLogSink,
  createSilentLogContext,
} from 'shared/src/logging-test-utils.js';
import {resolver} from './resolver.js';
import {sleep} from './sleep.js';

describe('LoggingLock', () => {
  test('logs nothing for timings above threshold', async () => {
    const lock = new LoggingLock(100 /* ms threshold */);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    await lock.withLock(lc, 'fast', () => {
      // do nothing
    });
    await lc.flush();

    expect(sink.messages).toHaveLength(0);
  });

  test('returns return value of fn', async () => {
    const lock = new LoggingLock(100 /* ms threshold */);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const result = await lock.withLock(lc, 'test fn', async () => {
      await 1;
      return 'test';
    });
    await lc.flush();
    expect(result).toEqual('test');
  });

  test('adds lockHoldID to the LogContext', async () => {
    const lock = new LoggingLock(100 /* ms threshold */);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {foo: 'bar'}, sink);

    await lock.withLock(lc, 'fast', innerLC => {
      innerLC.info?.('should have new context');
    });
    await lc.flush();

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0][0]).toBe('info');
    expect(sink.messages[0][1]).toMatchObject({
      foo: 'bar',
      lockFn: 'fast',
    });
    expect(sink.messages[0][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[0][2][0]).toBe('should have new context');
  });

  test('logs acquired and held lockTiming', async () => {
    const lock = new LoggingLock(-1);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const {promise: hasAcquiredLock, resolve: acquiredLock} = resolver<void>();
    const {promise: canReleaseLock, resolve: releaseLock} = resolver<void>();
    void lock.withLock(createSilentLogContext(), 'first', async () => {
      acquiredLock();
      await canReleaseLock;
    });

    await hasAcquiredLock;
    setTimeout(() => releaseLock(), 1);
    await lock.withLock(lc, 'logic', async () => {
      // do nothing
    });
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toMatchObject({
      lockFn: 'logic',
      lockStage: 'acquired',
    });
    expect(sink.messages[0][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[0][1]).toHaveProperty('lockTiming');
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][1]).toMatchObject({
      lockFn: 'logic',
      lockStage: 'held',
    });
    expect(sink.messages[1][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[1][1]).toHaveProperty('lockTiming');
  });

  test('logs at info level above threshold', async () => {
    const lock = new LoggingLock(-1);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    const {promise: hasAcquiredLock, resolve: acquiredLock} = resolver<void>();
    const {promise: canReleaseLock, resolve: releaseLock} = resolver<void>();
    void lock.withLock(createSilentLogContext(), 'first', async () => {
      acquiredLock();
      await canReleaseLock;
    });

    await hasAcquiredLock;
    setTimeout(() => releaseLock(), 1);
    await lock.withLock(
      lc,
      'logic',
      async () => {
        await sleep(2); // Must be >1ms
      },
      1, // Log at INFO if held for more than 1 ms
    );
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toMatchObject({
      lockFn: 'logic',
      lockStage: 'acquired',
    });
    expect(sink.messages[0][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[0][1]).toHaveProperty('lockTiming');
    expect(sink.messages[1][0]).toBe('info');
    expect(sink.messages[1][1]).toMatchObject({
      lockFn: 'logic',
      lockStage: 'held',
    });
    expect(sink.messages[1][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[1][1]).toHaveProperty('lockTiming');
  });

  test('logs multiple waiters without awaiting flush', async () => {
    const lock = new LoggingLock(1000);
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

    // To ensure that flush() is never `await`ed when logging pre-acquire
    // messages, we replace flush with a never-resolving Promise.
    const {promise: neverFinished} = resolver<void>();
    const flushSpy = jest
      .spyOn(lc, 'flush')
      .mockImplementation(() => neverFinished);
    // Override withContext so it always returns the this instance with the mocked flush().
    jest.spyOn(lc, 'withContext').mockImplementation(() => lc);

    const {promise: hasAcquiredLock, resolve: acquiredLock} = resolver<void>();
    const {promise: canReleaseFirstLock, resolve: releaseFirstLock} =
      resolver<void>();
    void lock.withLock(createSilentLogContext(), 'slow', async () => {
      acquiredLock();
      await canReleaseFirstLock;
    });

    await hasAcquiredLock;

    const {promise: canReleaseSecondLock, resolve: releaseSecondLock} =
      resolver<void>();
    const waiters: Promise<void>[] = [];
    const pushWaiter = () => {
      waiters.push(
        lock.withLock(lc, `logic`, async () => {
          await canReleaseSecondLock;
        }),
      );
    };

    pushWaiter();
    pushWaiter();

    await sleep(1);

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][2][0]).toMatch(
      /logic waiting for slow#[a-z0-9]+ with 1 other waiter\(s\): logic,logic/,
    );

    pushWaiter();
    await sleep(2);

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][2][0]).toMatch(
      /logic waiting for slow#[a-z0-9]+ with 2 other waiter\(s\): logic,logic,logic/,
    );

    // Push 7 more waiters so that the total of 10 waiters triggers an (asynchronous) LogContext flush.
    for (let i = 0; i < 7; i++) {
      pushWaiter();
    }
    await sleep(2);

    // The 9th message should call flush() without awaiting.
    expect(sink.messages).toHaveLength(9);

    releaseFirstLock();
    releaseSecondLock();

    // Ensure that the waiters all complete, which validates that they are never waiting on the
    // never-resolving flush() method. If they are waiting, the test runner should fail with a timeout.
    await Promise.all(waiters);
    expect(flushSpy).toHaveBeenCalledTimes(1);
  });
});
