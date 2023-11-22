import {describe, expect, jest, test} from '@jest/globals';
import type {AlarmScheduler} from '../server/alarms.js';
import {ConnectionLifetimeReporter} from './connection-lifetimes.js';

describe('connection-lifetimes', () => {
  const scheduler = {
    setTimeout: jest.fn().mockImplementation(() => 123),
  };

  function newReporter() {
    const reporter = new ConnectionLifetimeReporter(
      scheduler as unknown as AlarmScheduler,
    );
    return reporter;
  }

  test('timeout scheduling', () => {
    const reporter = newReporter();
    expect(scheduler.setTimeout).not.toBeCalled;

    void reporter.onConnectionCountChange(2);
    expect(scheduler.setTimeout).not.toBeCalled;

    reporter.onConnectionCountChange(3);
    expect(scheduler.setTimeout).not.toBeCalled;

    reporter.onConnectionCountChange(1);
    expect(scheduler.setTimeout).toBeCalledTimes(1);

    reporter.onConnectionCountChange(2);
    expect(scheduler.setTimeout).toBeCalledTimes(1);

    reporter.onConnectionCountChange(2);
    expect(scheduler.setTimeout).toBeCalledTimes(1);

    reporter.onConnectionCountChange(0);
    expect(scheduler.setTimeout).toBeCalledTimes(2);
  });
});
