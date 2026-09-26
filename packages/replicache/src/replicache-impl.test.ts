import type {LogLevel, LogSink} from '@rocicorp/logger';
import {describe, expect, test, vi} from 'vitest';
import {refresh} from './persist/refresh.ts';
import {ReplicacheImpl} from './replicache-impl.ts';
import type {ReplicacheOptions, ZeroOption} from './replicache-options.ts';
import {initReplicacheTesting, tickAFewTimes} from './test-util.ts';

vi.mock('./persist/refresh.ts', () => ({
  refresh: vi.fn().mockResolvedValue(undefined),
}));

initReplicacheTesting();

describe('ReplicacheImpl', () => {
  test('enableRefresh option controls refresh behavior', async () => {
    const pullURL = 'https://pull.com/rep';
    const name = 'test-enable-refresh';
    const options: ReplicacheOptions<{}> = {
      name,
      pullURL,
    };

    let refreshEnabled = false;
    const impl = new ReplicacheImpl(options, {
      enableRefresh: () => refreshEnabled,
      enablePullAndPushInOpen: false, // Disable auto-pull
    });

    // Initial state
    refreshEnabled = false;

    await impl.runRefresh();

    expect(refresh).not.toHaveBeenCalled();

    refreshEnabled = true;

    await impl.runRefresh();

    expect(refresh).toHaveBeenCalled();

    await impl.close();
  });

  test('a failure during open is reported to the log sinks, not rethrown', async () => {
    // The promise `#open().catch(...)` returns is discarded, so rethrowing
    // from that handler produces an unhandled rejection that no log sink and
    // no application code can observe — while `#ready` stays pending, so the
    // instance is unusable for the rest of its life with nothing reported.
    const name = 'test-open-failure-is-reported';
    const openError = new Error('the store could not be read');

    const records: {level: LogLevel; args: unknown[]}[] = [];
    const logSink: LogSink = {
      log: (level, _context, ...args) => {
        records.push({level, args});
      },
    };

    const rejections: unknown[] = [];
    const onUnhandledRejection = (event: PromiseRejectionEvent) => {
      rejections.push(event.reason);
      // Keep the rejection from failing the run on its own; this test asserts
      // on it instead.
      event.preventDefault();
    };
    globalThis.addEventListener('unhandledrejection', onUnhandledRejection);

    try {
      const impl = new ReplicacheImpl(
        {name, logLevel: 'error', logSinks: [logSink]},
        {
          enablePullAndPushInOpen: false,
          // Zero's IVM initialization is the last thing `#open` awaits before
          // it resolves readiness, and it reads the dag — so it is where a
          // store that cannot answer surfaces. Only `init` runs here, so the
          // rest of the interface is deliberately absent.
          zero: {
            init: () => Promise.reject(openError),
          } as unknown as ZeroOption,
        },
      );

      await tickAFewTimes(vi);

      expect(
        records.some(
          ({level, args}) => level === 'error' && args.includes(openError),
        ),
      ).toBe(true);
      expect(rejections).toEqual([]);

      await impl.close();
    } finally {
      globalThis.removeEventListener(
        'unhandledrejection',
        onUnhandledRejection,
      );
    }
  });
});
