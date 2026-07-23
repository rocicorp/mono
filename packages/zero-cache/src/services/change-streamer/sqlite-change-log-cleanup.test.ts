import {resolver} from '@rocicorp/resolver';
import {afterEach, describe, expect, test, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {SQLiteChangeLogMaintenance} from '../replicator/sqlite-change-log-maintenance.ts';
import type {SQLiteChangeLogPurgeResult} from '../replicator/sqlite-change-log-purger.ts';
import {SQLiteChangeLogCleanupCoordinator} from './sqlite-change-log-cleanup.ts';

const lc = createSilentLogContext();

function purgeResult(
  moreEligible: boolean,
  deletedRows = moreEligible ? 2 : 0,
): SQLiteChangeLogPurgeResult {
  return {
    headWatermark: '0a',
    timeFloor: '0a',
    effectiveFloor: '08',
    deletedRows,
    deletedBeforeWatermark: deletedRows ? '08' : undefined,
    moreEligible,
  };
}

class TestTimers {
  readonly #timers: Array<{
    callback: () => void;
    delay: number;
    cleared: boolean;
  }> = [];

  readonly setTimeout = ((callback: () => void, delay = 0) => {
    const timer = {callback, delay, cleared: false};
    this.#timers.push(timer);
    return timer;
  }) as unknown as typeof setTimeout;

  readonly clearTimeout = ((timer: {cleared: boolean}) => {
    timer.cleared = true;
  }) as unknown as typeof clearTimeout;

  runNext(): number {
    const timer = this.#timers
      .filter(candidate => !candidate.cleared)
      .sort((a, b) => a.delay - b.delay)[0];
    if (!timer) {
      throw new Error('no timer is scheduled');
    }
    timer.cleared = true;
    timer.callback();
    return timer.delay;
  }

  activeDelays(): number[] {
    return this.#timers
      .filter(timer => !timer.cleared)
      .map(timer => timer.delay)
      .sort((a, b) => a - b);
  }
}

const coordinators: SQLiteChangeLogCleanupCoordinator[] = [];

afterEach(async () => {
  await Promise.all(
    coordinators.splice(0).map(coordinator => coordinator.close()),
  );
});

async function flushPromises(): Promise<void> {
  for (let i = 0; i < 5; i++) {
    await Promise.resolve();
  }
}

function createCoordinator({
  request,
  acks = new Set<string>(),
  head = () => '0a',
}: {
  request: (
    maintenance: SQLiteChangeLogMaintenance,
  ) => Promise<SQLiteChangeLogPurgeResult>;
  acks?: Set<string> | undefined;
  head?: (() => string | undefined) | undefined;
}) {
  const timers = new TestTimers();
  const coordinator = new SQLiteChangeLogCleanupCoordinator(lc, {
    retentionMs: 1000,
    maxRows: 100,
    request,
    getAcks: () => acks,
    getHead: head,
    retryDelayMs: 50,
    idleDrainIntervalMs: 500,
    now: () => 10_000,
    setTimeoutFn: timers.setTimeout,
    clearTimeoutFn: timers.clearTimeout,
  });
  coordinators.push(coordinator);
  return {coordinator, timers, acks};
}

describe('change-streamer/sqlite-change-log-cleanup', () => {
  test('coalesces verified targets and recomputes subscriber floor before dispatch', async () => {
    const request = vi.fn(() => Promise.resolve(purgeResult(false)));
    const {coordinator, timers, acks} = createCoordinator({request});

    coordinator.scheduleCleanup('06');
    coordinator.scheduleCleanup('08');
    acks.add('04');
    expect(timers.runNext()).toBe(0);
    await flushPromises();

    expect(request).toHaveBeenCalledTimes(1);
    expect(request).toHaveBeenCalledWith({
      safeFloor: '04',
      requestTimeMs: 10_000,
      retentionMs: 1000,
      maxRows: 100,
    });
  });

  test('catchup registration waits for an in-flight purge and blocks its successor', async () => {
    const first = resolver<SQLiteChangeLogPurgeResult>();
    const request = vi
      .fn<
        (
          maintenance: SQLiteChangeLogMaintenance,
        ) => Promise<SQLiteChangeLogPurgeResult>
      >()
      .mockReturnValueOnce(first.promise)
      .mockResolvedValue(purgeResult(false));
    const {coordinator, timers, acks} = createCoordinator({request});
    coordinator.scheduleCleanup('08');
    timers.runNext();
    await flushPromises();

    let registered = false;
    const registering = coordinator.runWhilePurgeBlocked(() => {
      acks.add('03');
      registered = true;
    });
    await flushPromises();
    expect(registered).toBe(false);

    first.resolve(purgeResult(true));
    await registering;
    expect(registered).toBe(true);
    expect(request).toHaveBeenCalledTimes(1);

    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(request).toHaveBeenLastCalledWith(
      expect.objectContaining({safeFloor: '03'}),
    );
  });

  test('immediately dispatches a newer target queued during an in-flight batch', async () => {
    const first = resolver<SQLiteChangeLogPurgeResult>();
    const request = vi
      .fn<
        (
          maintenance: SQLiteChangeLogMaintenance,
        ) => Promise<SQLiteChangeLogPurgeResult>
      >()
      .mockReturnValueOnce(first.promise)
      .mockResolvedValue(purgeResult(false));
    const {coordinator, timers} = createCoordinator({request});
    coordinator.scheduleCleanup('06');
    timers.runNext();
    await flushPromises();

    coordinator.scheduleCleanup('08');
    first.resolve(purgeResult(false));
    await flushPromises();

    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(request).toHaveBeenLastCalledWith(
      expect.objectContaining({safeFloor: '08'}),
    );
  });

  test('snapshot reservation waits for dispatched work and blocks new batches until release', async () => {
    const first = resolver<SQLiteChangeLogPurgeResult>();
    const request = vi
      .fn<
        (
          maintenance: SQLiteChangeLogMaintenance,
        ) => Promise<SQLiteChangeLogPurgeResult>
      >()
      .mockReturnValueOnce(first.promise)
      .mockResolvedValue(purgeResult(false));
    const {coordinator, timers} = createCoordinator({request});
    coordinator.scheduleCleanup('08');
    timers.runNext();
    await flushPromises();

    let paused = false;
    const pause = coordinator.pauseForSnapshot('view-syncer-1').then(() => {
      paused = true;
    });
    await flushPromises();
    expect(paused).toBe(false);

    first.resolve(purgeResult(true));
    await pause;
    coordinator.scheduleCleanup('0a');
    expect(timers.activeDelays()).toEqual([]);

    coordinator.resumeAfterSnapshot('view-syncer-1');
    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(request).toHaveBeenCalledTimes(2);
  });

  test('keeps purge blocked until every task-scoped reservation ends', async () => {
    const request = vi.fn(() => Promise.resolve(purgeResult(false)));
    const {coordinator, timers} = createCoordinator({request});

    await coordinator.pauseForSnapshot('view-syncer-1');
    await coordinator.pauseForSnapshot('view-syncer-2');
    coordinator.scheduleCleanup('08');
    expect(timers.activeDelays()).toEqual([]);

    coordinator.resumeAfterSnapshot('view-syncer-1');
    expect(timers.activeDelays()).toEqual([]);
    coordinator.resumeAfterSnapshot('view-syncer-2');
    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(request).toHaveBeenCalledTimes(1);
  });

  test('retries failures and periodically drains a quiet source', async () => {
    const request = vi
      .fn<
        (
          maintenance: SQLiteChangeLogMaintenance,
        ) => Promise<SQLiteChangeLogPurgeResult>
      >()
      .mockRejectedValueOnce(new Error('worker unavailable'))
      .mockResolvedValueOnce(purgeResult(true, 2))
      .mockResolvedValue(purgeResult(false));
    const {coordinator, timers} = createCoordinator({request});
    coordinator.scheduleCleanup('08');

    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(timers.runNext()).toBe(50);
    await flushPromises();
    expect(timers.runNext()).toBe(0);
    await flushPromises();
    expect(timers.runNext()).toBe(500);
    await flushPromises();

    expect(request).toHaveBeenCalledTimes(4);
  });
});
