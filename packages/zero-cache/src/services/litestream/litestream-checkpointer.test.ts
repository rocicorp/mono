import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {AbortError} from '../../../../shared/src/abort-error.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Database} from '../../../../zqlite/src/db.ts';
import {DbFile} from '../../test/lite.ts';
import type {ForceCheckpointConfig} from '../replicator/write-worker-client.ts';
import {LitestreamCheckpointer} from './litestream-checkpointer.ts';
import type {
  LitestreamController,
  SyncResponse,
} from './litestream-controller.ts';

const lc = createSilentLogContext();

const SYNC_RESPONSE: SyncResponse = {
  status: 'synced_local',
  path: '/data/replica.db',
  txid: 1,
  replicated_txid: 0,
};

// Reads the un-checkpointed WAL frame backlog the same way LitestreamCheckpointer
// does, for asserting on db state directly in tests.
function numWalPages(db: Database): number {
  const [{log, checkpointed}] = db.pragma<{log: number; checkpointed: number}>(
    'wal_checkpoint(NOOP)',
  );
  return log - checkpointed;
}

// A fake LitestreamController whose `sync` is fully controlled by the test:
// litestream's real checkpoint behavior (drain vs. skip during a snapshot) is
// simulated by whether `onSync` itself runs a PASSIVE checkpoint on the same
// connection the checkpointer is watching.
function fakeController(
  onSync: () => Promise<SyncResponse> | SyncResponse = () => SYNC_RESPONSE,
): LitestreamController & {sync: ReturnType<typeof vi.fn>} {
  return {
    // Wrapped in an `async` function (rather than passed directly to vi.fn)
    // so that, like the real controller, sync() always returns a Promise —
    // including turning a synchronous throw (used to simulate a rejected
    // request) into a rejected Promise instead of an actual throw.
    sync: vi.fn(() => Promise.resolve().then(onSync)),
    close: vi.fn(),
  } as unknown as LitestreamController & {sync: ReturnType<typeof vi.fn>};
}

describe('litestream/litestream-checkpointer', () => {
  let dbFile: DbFile;
  let db: Database;

  beforeEach(() => {
    dbFile = new DbFile('litestream-checkpointer');

    // Create the schema on a connection that's closed before the test's `db`
    // opens: SQLite checkpoints the WAL when the last connection to it
    // closes, so this leaves a clean, zero-frame WAL baseline for the
    // page-count assertions below (matching db/wal-checkpoint.test.ts).
    const setup = dbFile.connect(lc);
    setup.pragma('journal_mode = WAL');
    setup.exec('CREATE TABLE foo(id INTEGER PRIMARY KEY)');
    setup.close();

    db = dbFile.connect(lc);
    db.pragma('wal_autocheckpoint = 0');
  });

  afterEach(() => {
    db.close();
    dbFile.delete();
    vi.useRealTimers();
  });

  function insertRows(count: number, startAt = 0) {
    const insert = db.prepare('INSERT INTO foo(id) VALUES(?)');
    for (let i = 0; i < count; i++) {
      insert.run(startAt + i);
    }
  }

  // Simulates a successful litestream PASSIVE checkpoint: seals + drains the WAL.
  function drainWal() {
    db.pragma('wal_checkpoint(PASSIVE)');
  }

  const config = (
    overrides: Partial<ForceCheckpointConfig> = {},
  ): ForceCheckpointConfig => ({
    checkpointThresholdPages: 5,
    maxWalPages: undefined,
    ...overrides,
  });

  test('does not sync when the WAL is under the threshold', async () => {
    const litestream = fakeController();
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config(),
    );

    insertRows(3); // under checkpointThresholdPages (5)
    await checkpointer.maybeCheckpoint();

    expect(litestream.sync).not.toHaveBeenCalled();
  });

  test('syncs and resets the threshold once the WAL drains', async () => {
    const litestream = fakeController(() => {
      drainWal();
      return SYNC_RESPONSE;
    });
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config(),
    );

    insertRows(10); // over the threshold (5)
    await checkpointer.maybeCheckpoint();

    expect(litestream.sync).toHaveBeenCalledTimes(1);
    expect(numWalPages(db)).toBe(0);

    // Below the next threshold (5 more pages): no sync yet.
    insertRows(4, 10);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(1);

    // Crossing it triggers another attempt.
    insertRows(1, 14);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(2);
  });

  test('backs off by a full chunk when the checkpoint does not drain the WAL (e.g. a snapshot is in progress)', async () => {
    // sync() succeeds (litestream is reachable) but its checkpoint is skipped
    // server-side, so the WAL is unaffected — as happens during a snapshot.
    const litestream = fakeController(() => SYNC_RESPONSE);
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config(),
    );

    insertRows(10); // over the threshold (5)
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(1);
    expect(numWalPages(db)).toBe(10); // did not drain

    // nextWalThreshold is now 10 + 5 = 15: growth under that doesn't retry.
    insertRows(4, 10);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(1);

    // Crossing 15 retries.
    insertRows(1, 14);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(2);
  });

  test('backs off the same way when sync() rejects', async () => {
    const litestream = fakeController(() => {
      throw new Error('ECONNREFUSED');
    });
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config(),
    );

    insertRows(10);
    await expect(checkpointer.maybeCheckpoint()).resolves.toBeUndefined();
    expect(litestream.sync).toHaveBeenCalledTimes(1);
    expect(numWalPages(db)).toBe(10);

    insertRows(4, 10);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(1); // still under backed-off threshold
  });

  test('pauses and polls litestream until the WAL drains below the hard cap', async () => {
    vi.useFakeTimers();

    // Skips (simulating an in-progress snapshot) for the first two sync
    // attempts, then drains on the third.
    let attempt = 0;
    const litestream = fakeController(() => {
      attempt++;
      if (attempt >= 3) {
        drainWal();
      }
      return SYNC_RESPONSE;
    });
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config({checkpointThresholdPages: 5, maxWalPages: 15}),
    );

    insertRows(20); // over both the soft threshold (5) and the hard cap (15)

    const done = checkpointer.maybeCheckpoint();
    // 1st attempt happens inside maybeCheckpoint itself (no sleep yet); it
    // fails to drain (20 > 15), entering the poll loop, which sleeps 1s
    // between each subsequent attempt (the 2nd and 3rd).
    await vi.advanceTimersByTimeAsync(2_000);
    await done;

    expect(litestream.sync).toHaveBeenCalledTimes(3);
    expect(numWalPages(db)).toBe(0);
  });

  test('resets the soft threshold from the post-pause WAL size, not the pre-pause peak', async () => {
    vi.useFakeTimers();

    let attempt = 0;
    const litestream = fakeController(() => {
      attempt++;
      if (attempt >= 2) {
        drainWal();
      }
      return SYNC_RESPONSE;
    });
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config({checkpointThresholdPages: 5, maxWalPages: 15}),
    );

    insertRows(20);
    const done = checkpointer.maybeCheckpoint();
    await vi.advanceTimersByTimeAsync(2_000);
    await done;
    expect(numWalPages(db)).toBe(0);

    // If nextWalThreshold had been left at the pre-pause peak (20 + 5 = 25)
    // instead of being derived from the drained size (0 + 5 = 5), this
    // wouldn't trigger a sync.
    insertRows(6, 20);
    await checkpointer.maybeCheckpoint();
    expect(litestream.sync).toHaveBeenCalledTimes(3);
  });

  test('close() releases the underlying controller', () => {
    const litestream = fakeController();
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config(),
    );

    checkpointer.close();

    expect(litestream.close).toHaveBeenCalledTimes(1);
  });

  test('close() interrupts an in-progress pause-until-drained poll', async () => {
    vi.useFakeTimers();

    // Never drains: simulates litestream being stuck mid-snapshot (or down)
    // for longer than the test cares to wait out.
    const litestream = fakeController(() => SYNC_RESPONSE);
    const checkpointer = new LitestreamCheckpointer(
      lc,
      db,
      litestream,
      config({checkpointThresholdPages: 5, maxWalPages: 15}),
    );

    insertRows(20); // over both the soft threshold (5) and the hard cap (15)

    const done = checkpointer.maybeCheckpoint();
    // Flush microtasks so execution reaches the pause loop's first sleep
    // (registering its abort listener) without firing the 1s timer itself.
    await vi.advanceTimersByTimeAsync(0);
    expect(litestream.sync).toHaveBeenCalledTimes(1); // the initial soft attempt

    checkpointer.close();

    // The interrupted sleep resolves immediately rather than waiting out the
    // full 1s poll interval, and the loop exits with an AbortError instead of
    // continuing to poll indefinitely.
    await expect(done).rejects.toThrow(AbortError);
    expect(litestream.close).toHaveBeenCalledTimes(1);
  });
});
