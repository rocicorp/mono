import {
  describe,
  beforeEach,
  afterEach,
  expect,
  test,
  jest,
} from '@jest/globals';
import {AlarmManager, AlarmScheduler} from './alarms.js';
import {TestLogSink, createSilentLogContext} from '../util/test-utils.js';
import {LogContext} from '@rocicorp/logger';

describe('alarm timeout tests', () => {
  const STARTING_TIME = 1000;
  let fireTime: null | number = null;
  const lc = createSilentLogContext();

  const storage: DurableObjectStorage = {
    getAlarm: () => Promise.resolve(fireTime),
    setAlarm: (newFireTime: number) => {
      fireTime = newFireTime;
      return Promise.resolve();
    },
    deleteAlarm: () => {
      fireTime = null;
      return Promise.resolve();
    },
  } as DurableObjectStorage;

  let alarmManager: AlarmManager;
  let scheduler: AlarmScheduler;

  beforeEach(() => {
    fireTime = null;
    alarmManager = new AlarmManager(storage);
    scheduler = alarmManager.scheduler;
    jest.useFakeTimers();
    jest.setSystemTime(STARTING_TIME);
  });

  afterEach(() => {
    jest.resetAllMocks();
  });

  async function expectFlushAlarm(at: number, then: number | null) {
    expect(fireTime).toBe(at);
    await alarmManager.fireScheduled(lc);
    expect(fireTime).toBe(then);
  }

  test('fire single timeout', async () => {
    const results: string[] = [];
    scheduler.setTimeout(() => {
      results.push('foo');
    }, 10);

    expect(results).toEqual([]);
    expect(await alarmManager.nextAlarmTime()).toBe(STARTING_TIME + 10);
    expect(fireTime).toBe(STARTING_TIME + 10);

    jest.advanceTimersByTime(9);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 10);

    jest.advanceTimersByTime(1);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['foo']);
    await expectFlushAlarm(STARTING_TIME + 10, null);
  });

  test('fire promised timeout', async () => {
    const results: string[] = [];
    await scheduler.promiseTimeout(() => {
      results.push('foo');
    }, 10);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 10);

    jest.advanceTimersByTime(9);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 10);

    jest.advanceTimersByTime(1);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['foo']);
    await expectFlushAlarm(STARTING_TIME + 10, null);
  });

  test('timeout with args', async () => {
    const results: string[] = [];
    scheduler.setTimeout(
      (_, item1, item2) => {
        results.push(item1, item2);
      },
      10,
      'food',
      'bard',
    );

    jest.advanceTimersByTime(10);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['food', 'bard']);
    await expectFlushAlarm(STARTING_TIME + 10, null);

    await scheduler.promiseTimeout(
      (_, ...items) => {
        results.push(...items);
      },
      20,
      'bazd',
      'bonkd',
    );

    jest.advanceTimersByTime(20);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['food', 'bard', 'bazd', 'bonkd']);
    await expectFlushAlarm(STARTING_TIME + 30, null);
  });

  test('clear single timeout', async () => {
    const results: string[] = [];
    const alarmID = scheduler.setTimeout(() => {
      results.push('foo');
    }, 10);

    expect(results).toEqual([]);
    expect(await alarmManager.nextAlarmTime()).toBe(STARTING_TIME + 10);
    expect(fireTime).toBe(STARTING_TIME + 10);

    await scheduler.clearTimeout(alarmID);
    expect(fireTime).toBe(null);

    jest.advanceTimersByTime(10);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual([]);
    expect(fireTime).toBe(null);
  });

  test('set multiple timeouts', async () => {
    const results: string[] = [];
    scheduler.setTimeout(() => {
      results.push('foo');
    }, 30);
    await scheduler.promiseTimeout(() => {
      results.push('bar');
    }, 20);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 20);

    jest.advanceTimersByTime(20);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['bar']);
    await expectFlushAlarm(STARTING_TIME + 20, STARTING_TIME + 30);

    jest.advanceTimersByTime(10);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['bar', 'foo']);
    await expectFlushAlarm(STARTING_TIME + 30, null);
  });

  test('clear one of many timeouts', async () => {
    const results: string[] = [];
    const alarmID = scheduler.setTimeout(() => {
      results.push('foo');
    }, 20);
    await scheduler.promiseTimeout(() => {
      results.push('bar');
    }, 30);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 20);

    await scheduler.clearTimeout(alarmID);
    expect(fireTime).toBe(STARTING_TIME + 30);
    expect(results).toEqual([]);

    jest.advanceTimersByTime(30);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['bar']);
    await expectFlushAlarm(STARTING_TIME + 30, null);
  });

  test('fire concurrent timeouts', async () => {
    const results: string[] = [];
    scheduler.setTimeout(() => {
      results.push('foo');
    }, 30);
    scheduler.setTimeout(() => {
      results.push('bar');
    }, 30);

    expect(results).toEqual([]);
    expect(await alarmManager.nextAlarmTime()).toBe(STARTING_TIME + 30);
    expect(fireTime).toBe(STARTING_TIME + 30);

    jest.advanceTimersByTime(30);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(expect.arrayContaining(['foo', 'bar']));
    await expectFlushAlarm(STARTING_TIME + 30, null);
  });

  test('timeout errors are isolated and logged', async () => {
    const results: string[] = [];
    await scheduler.promiseTimeout(
      () => Promise.reject('error from async'),
      30,
    );
    await scheduler.promiseTimeout(() => {
      throw 'error from sync';
    }, 30);
    await scheduler.promiseTimeout(() => {
      results.push('bar');
    }, 30);

    expect(results).toEqual([]);
    expect(await alarmManager.nextAlarmTime()).toBe(STARTING_TIME + 30);
    expect(fireTime).toBe(STARTING_TIME + 30);

    jest.advanceTimersByTime(30);

    const logSink = new TestLogSink();
    await alarmManager.fireScheduled(new LogContext('info', {}, logSink));

    expect(results).toEqual(expect.arrayContaining(['bar']));
    expect(logSink.messages).toEqual([
      ['error', {}, ['error from async']],
      ['error', {}, ['error from sync']],
    ]);
    await expectFlushAlarm(STARTING_TIME + 30, null);
  });

  test('log context passed to callback', async () => {
    await scheduler.promiseTimeout(
      (lc, ...args) => {
        lc.info?.('ABC is easy as', ...args);
      },
      10,
      1,
      2,
      3,
    );

    expect(fireTime).toBe(STARTING_TIME + 10);

    jest.advanceTimersByTime(10);

    const logSink = new TestLogSink();
    await alarmManager.fireScheduled(new LogContext('info', {}, logSink));

    expect(logSink.messages).toEqual([
      ['info', {}, ['ABC is easy as', 1, 2, 3]],
    ]);
    await expectFlushAlarm(STARTING_TIME + 10, null);
  });

  test('reschedules if no timeouts to fire', async () => {
    const results: string[] = [];
    await scheduler.promiseTimeout(() => {
      results.push('foo');
    }, 20);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 20);

    // Simulate a race condition in which an alarm for
    // a deleted timeout (at timeout = 10) fires. The AlarmManager
    // should still reschedule to the next alarm.
    fireTime = null;
    jest.advanceTimersByTime(10);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual([]);
    expect(fireTime).toBe(STARTING_TIME + 20);

    jest.advanceTimersByTime(10);
    await alarmManager.fireScheduled(lc);

    expect(results).toEqual(['foo']);
    await expectFlushAlarm(STARTING_TIME + 20, null);
  });
});
