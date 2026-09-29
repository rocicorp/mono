import {mkdtempSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {resolver, type Resolver} from '@rocicorp/resolver';
import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {StatementRunner} from '../../db/statements.ts';
import type {Source} from '../../types/streams.ts';
import type {
  SyncOptions,
  SyncResponse,
} from '../litestream/litestream-controller.ts';
import {
  initReplicationState,
  updateReplicationWatermark,
} from '../replicator/schema/replication-state.ts';
import type {BackedUpWatermark} from './backup-monitor.ts';
import {
  LitestreamSyncRequester,
  type SyncController,
} from './litestream-sync-requester.ts';

const RESPONSE: SyncResponse = {
  status: 'synced',
  path: '/replica.db',
  txid: 1,
  replicated_txid: 1,
};

/** A fake litestream whose sync requests are resolved by the test. */
class FakeLitestream implements SyncController {
  readonly requests: {opts: SyncOptions | undefined; result: Resolver<void>}[] =
    [];
  closed = false;

  sync(opts?: SyncOptions): Promise<SyncResponse> {
    const result = resolver<void>();
    this.requests.push({opts, result});
    return result.promise.then(() => RESPONSE);
  }

  close() {
    this.closed = true;
  }
}

describe('change-streamer/litestream-sync-requester', () => {
  let replicaFile: string;
  let litestream: FakeLitestream;
  let requester: LitestreamSyncRequester;
  let source: Source<BackedUpWatermark>;
  let pushed: BackedUpWatermark[];

  function setLocalWatermark(stateVersion: string, writeTimeMs: number) {
    const db = new Database(createSilentLogContext(), replicaFile);
    try {
      updateReplicationWatermark(
        new StatementRunner(db),
        stateVersion,
        writeTimeMs,
      );
    } finally {
      db.close();
    }
  }

  beforeEach(() => {
    const dir = mkdtempSync(join(tmpdir(), 'litestream-sync-requester-test-'));
    replicaFile = join(dir, 'replica.db');
    const db = new Database(createSilentLogContext(), replicaFile);
    initReplicationState(db, ['zero_pub'], '00');
    db.close();
    setLocalWatermark('01', 1000);

    litestream = new FakeLitestream();
    requester = new LitestreamSyncRequester(
      createSilentLogContext(),
      replicaFile,
      {intervalMs: 60 * 60 * 1000}, // ticks are driven manually
      litestream,
    );
    // start() issues the first request immediately.
    source = requester.start();
    pushed = [];
    void (async () => {
      for await (const backedUp of source) {
        pushed.push(backedUp);
      }
    })();
  });

  afterEach(() => {
    source.cancel();
  });

  /** Resolves the outstanding request and waits for its tick to settle. */
  async function completeRequest(i: number) {
    litestream.requests[i].result.resolve();
    await requester.tick();
  }

  test('waits for the upload with a server-side timeout', () => {
    expect(litestream.requests).toHaveLength(1);
    expect(litestream.requests[0].opts).toMatchObject({
      wait: true,
      serverTimeoutSeconds: 60,
    });
    const {timeoutMs, serverTimeoutSeconds} = litestream.requests[0].opts ?? {};
    expect(timeoutMs).toBeGreaterThan((serverTimeoutSeconds ?? 0) * 1000);
  });

  test('publishes the watermark read before the request once it succeeds', async () => {
    await completeRequest(0);

    expect(pushed).toEqual([
      {watermark: '01', writeTimeMs: 1000, backupTimeMs: expect.any(Number)},
    ]);
  });

  test('does not attribute a later write to an in-flight request', async () => {
    setLocalWatermark('02', 2000);
    await completeRequest(0);
    expect(pushed.map(b => b.watermark)).toEqual(['01']);

    // The next tick requests a sync covering the later write.
    void requester.tick();
    expect(litestream.requests).toHaveLength(2);
    await completeRequest(1);
    expect(pushed.map(b => b.watermark)).toEqual(['01', '02']);
  });

  test('keeps at most one request outstanding', () => {
    setLocalWatermark('02', 2000);
    void requester.tick();
    void requester.tick();

    expect(litestream.requests).toHaveLength(1);
  });

  test('keeps requesting syncs while the backup is caught up', async () => {
    await completeRequest(0);
    expect(pushed.map(b => b.watermark)).toEqual(['01']);

    // Changes that don't advance the watermark still need to be synced.
    void requester.tick();
    expect(litestream.requests).toHaveLength(2);
    await completeRequest(1);

    // ... but the unchanged watermark is not published again.
    expect(pushed.map(b => b.watermark)).toEqual(['01']);

    setLocalWatermark('02', 2000);
    void requester.tick();
    expect(litestream.requests).toHaveLength(3);
    await completeRequest(2);
    expect(pushed.map(b => b.watermark)).toEqual(['01', '02']);
  });

  test('retries a failed request without publishing', async () => {
    litestream.requests[0].result.reject(
      new Error('context deadline exceeded'),
    );
    await requester.tick();
    expect(pushed).toEqual([]);

    void requester.tick();
    expect(litestream.requests).toHaveLength(2);
    await completeRequest(1);
    expect(pushed.map(b => b.watermark)).toEqual(['01']);
  });

  test('closes the litestream connection on cancel', () => {
    source.cancel();
    expect(litestream.closed).toBe(true);
  });
});
