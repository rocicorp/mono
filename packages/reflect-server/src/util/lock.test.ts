import {describe, test, expect} from '@jest/globals';
import {LoggingLock} from './lock.js';
import {LogContext} from '@rocicorp/logger';
import {TestLogSink, createSilentLogContext} from './test-utils.js';
import {sleep} from './sleep.js';
import {resolver} from './resolver.js';

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
      lockTiming: 'acquired',
    });
    expect(sink.messages[0][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][1]).toMatchObject({
      lockFn: 'logic',
      lockTiming: 'held',
    });
    expect(sink.messages[1][1]).toHaveProperty('lockHoldID');
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
      lockTiming: 'acquired',
    });
    expect(sink.messages[0][1]).toHaveProperty('lockHoldID');
    expect(sink.messages[1][0]).toBe('info');
    expect(sink.messages[1][1]).toMatchObject({
      lockFn: 'logic',
      lockTiming: 'held',
    });
    expect(sink.messages[1][1]).toHaveProperty('lockHoldID');
  });

  test('logs multiple waiters', async () => {
    const lock = new LoggingLock();
    const sink = new TestLogSink();
    const lc = new LogContext('debug', {}, sink);

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
    await lc.flush();

    expect(sink.messages).toHaveLength(1);
    expect(sink.messages[0][0]).toBe('debug');
    expect(sink.messages[0][1]).toEqual({
      lockFn: 'logic',
    });
    expect(sink.messages[0][2][0]).toMatch(
      /logic waiting for slow#[a-z0-9]+ with 1 other waiter\(s\): logic,logic/,
    );

    pushWaiter();
    await sleep(2);
    await lc.flush();

    expect(sink.messages).toHaveLength(2);
    expect(sink.messages[1][0]).toBe('debug');
    expect(sink.messages[1][1]).toEqual({
      lockFn: 'logic',
    });
    expect(sink.messages[1][2][0]).toMatch(
      /logic waiting for slow#[a-z0-9]+ with 2 other waiter\(s\): logic,logic,logic/,
    );

    releaseFirstLock();
    releaseSecondLock();

    await Promise.all(waiters);
  });
});
