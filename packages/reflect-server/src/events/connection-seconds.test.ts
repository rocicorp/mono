import {
  afterEach,
  beforeEach,
  describe,
  expect,
  jest,
  test,
} from '@jest/globals';
import type {LogContext} from '@rocicorp/logger';
import {subscribe, unsubscribe} from 'node:diagnostics_channel';
import {Queue} from 'shared/src/queue.js';
import type {AlarmScheduler} from '../server/alarms.js';
import {createSilentLogContext} from '../util/test-utils.js';
import {
  CONNECTION_CLOSED_FLUSH_INTERVAL_MS,
  ConnectionSecondsReporter,
  REPORTING_INTERVAL_MS,
} from './connection-seconds.js';

describe('connection-seconds', () => {
  const TEST_DIAGNOSTICS_CHANNEL_NAME = 'connection-seconds-test';
  const scheduler = {
    promiseTimeout: jest.fn().mockImplementation(() => Promise.resolve(123)),
    clearTimeout: jest.fn().mockImplementation(() => Promise.resolve()),
  };

  function newReporter(setRoomID: boolean) {
    const reporter = new ConnectionSecondsReporter(
      scheduler as unknown as AlarmScheduler,
      TEST_DIAGNOSTICS_CHANNEL_NAME,
    );
    if (setRoomID) {
      reporter.setRoomID('room-id');
    }
    return reporter;
  }

  let reportQueue: Queue<unknown>;

  function onPublish(message: unknown) {
    void reportQueue.enqueue(message);
  }

  beforeEach(() => {
    jest.useFakeTimers();
    jest.setSystemTime(1000);
    reportQueue = new Queue<unknown>();
    subscribe(TEST_DIAGNOSTICS_CHANNEL_NAME, onPublish);
  });

  afterEach(() => {
    jest.resetAllMocks();
    unsubscribe(TEST_DIAGNOSTICS_CHANNEL_NAME, onPublish);
  });

  test('timeout scheduling', async () => {
    const reporter = newReporter(true);
    expect(scheduler.promiseTimeout).not.toBeCalled;

    await reporter.onConnectionCountChange(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);
    expect(scheduler.promiseTimeout.mock.calls[0][1]).toEqual(
      REPORTING_INTERVAL_MS,
    );
    const flush1 = scheduler.promiseTimeout.mock.calls[0][0] as (
      lc: LogContext,
    ) => Promise<void>;

    await reporter.onConnectionCountChange(3);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);

    jest.advanceTimersByTime(1000);
    expect(scheduler.clearTimeout).toBeCalledTimes(0);
    await reporter.onConnectionCountChange(2);
    // Flush should be rescheduled because a connection was closed.
    expect(scheduler.clearTimeout).toBeCalledTimes(1);
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);
    expect(scheduler.promiseTimeout.mock.calls[1][1]).toEqual(
      CONNECTION_CLOSED_FLUSH_INTERVAL_MS,
    );

    jest.advanceTimersByTime(1000);
    await flush1(createSilentLogContext());
    // Flush should reschedule the timeout since there are open connectinos.
    expect(scheduler.promiseTimeout).toBeCalledTimes(3);
    expect(scheduler.promiseTimeout.mock.calls[2][1]).toEqual(
      REPORTING_INTERVAL_MS,
    );
    const flush2 = scheduler.promiseTimeout.mock.calls[1][0] as (
      lc: LogContext,
    ) => Promise<void>;

    jest.advanceTimersByTime(1000);
    // Setting the connections to zero should schedule an immediate flush
    // so that the elapsed times can be reported before the DO is shut down.
    expect(scheduler.clearTimeout).toBeCalledTimes(1);
    await reporter.onConnectionCountChange(0);

    expect(scheduler.clearTimeout).toBeCalledTimes(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);
    expect(scheduler.promiseTimeout.mock.calls[3][1]).toEqual(
      CONNECTION_CLOSED_FLUSH_INTERVAL_MS,
    );

    jest.advanceTimersByTime(1000);
    // Flush should not reschedule the timeout when there are no more connections
    await flush2(createSilentLogContext());
    expect(scheduler.clearTimeout).toBeCalledTimes(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);
  });

  test('tracks connection seconds', async () => {
    const reporter = newReporter(true);

    await reporter.onConnectionCountChange(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);

    // 1 second with 2 connections
    jest.advanceTimersByTime(1000);
    await reporter.onConnectionCountChange(3);

    // 3 seconds with 3 connections
    jest.advanceTimersByTime(3000);

    // Setting to zero requests an immediate flush.
    expect(scheduler.clearTimeout).toBeCalledTimes(0);
    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);
    expect(scheduler.clearTimeout).toBeCalledTimes(1);

    // 2 seconds with 0 connections
    jest.advanceTimersByTime(2000);
    await reporter.onConnectionCountChange(5);

    // 0.5 seconds with 5 connections
    jest.advanceTimersByTime(500);

    // Flush!
    const flush1 = scheduler.promiseTimeout.mock.calls[0][0] as (
      lc: LogContext,
    ) => Promise<void>;
    await flush1(createSilentLogContext());

    expect(await reportQueue.dequeue()).toEqual({
      roomID: 'room-id',
      period: 6.5,
      elapsed: 13.5, // (1*2) + (3*3) + (0.5*5)
    });

    // setTimeout should have been rescheduled.
    expect(scheduler.promiseTimeout).toBeCalledTimes(3);

    // + 2.5 seconds with 5 connections.
    jest.advanceTimersByTime(2500);

    // Setting to zero requests an earlier flush.
    expect(scheduler.clearTimeout).toBeCalledTimes(1);
    await reporter.onConnectionCountChange(0);
    expect(scheduler.clearTimeout).toBeCalledTimes(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);

    const flush2 = scheduler.promiseTimeout.mock.calls[1][0] as (
      lc: LogContext,
    ) => Promise<void>;
    await flush2(createSilentLogContext());

    expect(await reportQueue.dequeue()).toEqual({
      roomID: 'room-id',
      period: 2.5,
      elapsed: 12.5, // (2.5*5)
    });

    // setTimeout should not have been rescheduled because there are
    // no more connections.
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);

    // But should be rescheduled on the next connection.
    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);
    await reporter.onConnectionCountChange(1);
    expect(scheduler.promiseTimeout).toBeCalledTimes(5);

    expect(scheduler.clearTimeout).toBeCalledTimes(2);
  });

  test('flush skipped without roomID', async () => {
    const reporter = newReporter(false);

    await reporter.onConnectionCountChange(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);

    // 1 second with 2 connections
    jest.advanceTimersByTime(1000);
    await reporter.onConnectionCountChange(3);

    // 3 seconds with 3 connections
    jest.advanceTimersByTime(3000);

    // Setting to zero requests an immediate flush.
    expect(scheduler.clearTimeout).toBeCalledTimes(0);
    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);
    expect(scheduler.clearTimeout).toBeCalledTimes(1);

    // 2 seconds with 0 connections
    jest.advanceTimersByTime(2000);
    await reporter.onConnectionCountChange(5);

    // 0.5 seconds with 5 connections
    jest.advanceTimersByTime(500);

    // Flush!
    const flush1 = scheduler.promiseTimeout.mock.calls[0][0] as (
      lc: LogContext,
    ) => Promise<void>;

    // This flush should be a no-op because the roomID has not yet been
    // set. The data should continue to be aggregated.
    await flush1(createSilentLogContext());

    // setTimeout should have been rescheduled.
    expect(scheduler.promiseTimeout).toBeCalledTimes(3);

    // + 2.5 seconds with 5 connections.
    jest.advanceTimersByTime(2500);

    // Setting to zero requests an earlier flush.
    expect(scheduler.clearTimeout).toBeCalledTimes(1);
    await reporter.onConnectionCountChange(0);
    expect(scheduler.clearTimeout).toBeCalledTimes(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);

    reporter.setRoomID('finally-gets-the-room-id');

    const flush2 = scheduler.promiseTimeout.mock.calls[1][0] as (
      lc: LogContext,
    ) => Promise<void>;
    await flush2(createSilentLogContext());

    // Reporter should flush all of the state now.
    expect(await reportQueue.dequeue()).toEqual({
      roomID: 'finally-gets-the-room-id',
      period: 2.5, // 6.5 + 2.5
      elapsed: 26, // (1*2) + (3*3) + (0.5*5) + (2.5*5)
    });

    // setTimeout should not have been rescheduled because there are
    // no more connections.
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);

    // But should be rescheduled on the next connection.
    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(4);
    await reporter.onConnectionCountChange(1);
    expect(scheduler.promiseTimeout).toBeCalledTimes(5);

    expect(scheduler.clearTimeout).toBeCalledTimes(2);
  });
});
