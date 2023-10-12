import {
  afterEach,
  beforeEach,
  describe,
  expect,
  test,
  jest,
} from '@jest/globals';
import {subscribe, unsubscribe} from 'node:diagnostics_channel';
import type {AlarmScheduler} from '../server/alarms.js';
import {
  ConnectionSecondsReporter,
  REPORTING_INTERVAL_SECONDS,
} from './connection-seconds.js';
import {Queue} from 'shared/src/queue.js';
import type {LogContext} from '@rocicorp/logger';
import {createSilentLogContext} from '../util/test-utils.js';

describe('connection-seconds', () => {
  const TEST_DIAGNOSTICS_CHANNEL_NAME = 'connection-seconds-test';
  const scheduler = {
    promiseTimeout: jest.fn().mockImplementation(() => Promise.resolve(123)),
  };

  function newReporter() {
    return new ConnectionSecondsReporter(
      scheduler as unknown as AlarmScheduler,
      TEST_DIAGNOSTICS_CHANNEL_NAME,
    );
  }

  const reportQueue = new Queue<unknown>();

  function onPublish(message: unknown) {
    void reportQueue.enqueue(message);
  }

  beforeEach(() => {
    jest.useFakeTimers();
    subscribe(TEST_DIAGNOSTICS_CHANNEL_NAME, onPublish);
  });

  afterEach(() => {
    jest.resetAllMocks();
    unsubscribe(TEST_DIAGNOSTICS_CHANNEL_NAME, onPublish);
  });

  test('schedules timeout only once', async () => {
    const reporter = newReporter();
    expect(scheduler.promiseTimeout).not.toBeCalled;

    await reporter.onConnectionCountChange(2);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);
    expect(scheduler.promiseTimeout.mock.calls[0][1]).toEqual(
      REPORTING_INTERVAL_SECONDS * 1000,
    );

    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);

    await reporter.onConnectionCountChange(1);
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);
  });

  test('tracks connection seconds', async () => {
    const reporter = newReporter();

    jest.setSystemTime(1000);
    await reporter.onConnectionCountChange(2);

    // 1 second with 2 connections
    jest.setSystemTime(2000);
    await reporter.onConnectionCountChange(3);

    // 3 seconds with 3 connections
    jest.setSystemTime(5000);
    await reporter.onConnectionCountChange(0);

    // 2 seconds with 0 connections
    jest.setSystemTime(7000);
    await reporter.onConnectionCountChange(5);

    // 0.5 seconds with 5 connections
    jest.setSystemTime(7500);

    // Flush!
    expect(scheduler.promiseTimeout).toBeCalledTimes(1);
    const flush1 = scheduler.promiseTimeout.mock.calls[0][0] as (
      lc: LogContext,
    ) => Promise<void>;
    await flush1(createSilentLogContext());

    expect(await reportQueue.dequeue()).toEqual({
      interval: REPORTING_INTERVAL_SECONDS,
      elapsed: 13.5, // (1*2) + (3*3) + (0.5*5)
    });

    // setTimeout should have been rescheduled.
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);

    // + 2.5 seconds with 5 connections.
    jest.setSystemTime(10000);
    await reporter.onConnectionCountChange(0);

    const flush2 = scheduler.promiseTimeout.mock.calls[1][0] as (
      lc: LogContext,
    ) => Promise<void>;
    await flush2(createSilentLogContext());

    expect(await reportQueue.dequeue()).toEqual({
      interval: REPORTING_INTERVAL_SECONDS,
      elapsed: 12.5, // (2.5*5)
    });

    // setTimeout should not have been rescheduled because there are
    // no more connections.
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);

    // But should be rescheduled on the next connection.
    await reporter.onConnectionCountChange(0);
    expect(scheduler.promiseTimeout).toBeCalledTimes(2);
    await reporter.onConnectionCountChange(1);
    expect(scheduler.promiseTimeout).toBeCalledTimes(3);
  });
});
