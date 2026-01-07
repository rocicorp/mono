import {describe, expect, test, vi} from 'vitest';
import {ReplicacheImpl} from './replicache-impl.ts';
import {initReplicacheTesting} from './test-util.ts';
import type {ReplicacheOptions} from './replicache-options.ts';
import {consoleLogSink} from '@rocicorp/logger';
import {refresh} from './persist/refresh.ts';

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
      logSinks: [consoleLogSink],
    };

    let refreshEnabled = false;
    const impl = new ReplicacheImpl(options, {
      enableRefresh: () => refreshEnabled,
      enablePullAndPushInOpen: false, // Disable auto-pull
    });

    // Initial state
    refreshEnabled = false;

    await impl.refresh();

    expect(refresh).not.toHaveBeenCalled();

    refreshEnabled = true;

    await impl.refresh();

    expect(refresh).toHaveBeenCalled();

    await impl.close();
  });
});
