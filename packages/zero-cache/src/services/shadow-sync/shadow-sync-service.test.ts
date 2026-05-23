import {resolver} from '@rocicorp/resolver';
import {createSilentLogContext} from 'shared/src/logging-test-utils.ts';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {ShadowSyncService} from './shadow-sync-service.ts';

const shadowInitialSyncMock = vi.hoisted(() => vi.fn());

vi.mock('../change-source/pg/initial-sync.ts', () => ({
  shadowInitialSync: shadowInitialSyncMock,
}));

const SHARD = {
  appID: '1',
  shardNum: 0,
  publications: [] as readonly string[],
} as const;

const CONTEXT = {foo: 'shadow-sync-schedule-test'};

const INTERVAL_MS = 1000;

describe('ShadowSyncService scheduling', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    shadowInitialSyncMock.mockReset();
    shadowInitialSyncMock.mockResolvedValue(undefined);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  function makeService() {
    return new ShadowSyncService(
      createSilentLogContext(),
      SHARD,
      'postgres://unused',
      CONTEXT,
      {
        intervalMs: INTERVAL_MS,
        sampleRate: 1,
        maxRowsPerTable: 10,
      },
    );
  }

  test('first run waits at least 2/3 of an interval (jitter = 0)', async () => {
    // minFirstRunDelay = floor(1000 * 2 / 3) = 666, jitter = 0.
    vi.spyOn(Math, 'random').mockReturnValue(0);
    const service = makeService();
    const running = service.run();

    await vi.advanceTimersByTimeAsync(665);
    expect(shadowInitialSyncMock).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(shadowInitialSyncMock).toHaveBeenCalledTimes(1);

    await service.stop();
    await running;
  });

  test('first run fires before one full interval (jitter ≈ max)', async () => {
    // minFirstRunDelay = 666, range = 1000 - 666 = 334.
    // Math.floor(0.9999 * 334) = 333, so firstRunDelay = 999.
    vi.spyOn(Math, 'random').mockReturnValue(0.9999);
    const service = makeService();
    const running = service.run();

    await vi.advanceTimersByTimeAsync(998);
    expect(shadowInitialSyncMock).not.toHaveBeenCalled();

    await vi.advanceTimersByTimeAsync(1);
    expect(shadowInitialSyncMock).toHaveBeenCalledTimes(1);

    await service.stop();
    await running;
  });

  test('subsequent runs are spaced by intervalMs after completion', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0); // first run at intervalMs

    // Gate each sync call on an external resolver so we control completion.
    let current = resolver<void>();
    shadowInitialSyncMock.mockImplementation(() => current.promise);

    const service = makeService();
    const running = service.run();

    // Reach the first run.
    await vi.advanceTimersByTimeAsync(INTERVAL_MS);
    expect(shadowInitialSyncMock).toHaveBeenCalledTimes(1);

    // Complete first run. The service now sleeps intervalMs before run #2.
    const first = current;
    current = resolver<void>();
    first.resolve();
    await vi.advanceTimersByTimeAsync(0); // flush the post-completion sleep scheduling

    // Just before the interval elapses — no second run yet.
    await vi.advanceTimersByTimeAsync(INTERVAL_MS - 1);
    expect(shadowInitialSyncMock).toHaveBeenCalledTimes(1);

    // Cross the boundary — second run fires.
    await vi.advanceTimersByTimeAsync(1);
    expect(shadowInitialSyncMock).toHaveBeenCalledTimes(2);

    // Clean up: resolve the in-flight sync, then stop. The service will
    // enter one more sleep(intervalMs) whose abort listener registers after
    // the signal is aborted, so we flush that final timer manually.
    current.resolve();
    const stopped = service.stop();
    await vi.runAllTimersAsync();
    await stopped;
    await running;
  });

  test('stop during the initial sleep exits promptly', async () => {
    vi.spyOn(Math, 'random').mockReturnValue(0.5);
    const service = makeService();
    const running = service.run();

    await vi.advanceTimersByTimeAsync(100);
    expect(shadowInitialSyncMock).not.toHaveBeenCalled();

    await service.stop();
    await running;
    expect(shadowInitialSyncMock).not.toHaveBeenCalled();
  });
});
