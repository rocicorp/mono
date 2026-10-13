import {afterEach, beforeEach, describe, expect, vi} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {ErrorKind} from '../../../../zero-protocol/src/error-kind.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRConfigDrivenUpdater} from './cvr.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';
import {
  ISSUES_QUERY,
  nextPoke,
  ON_FAILURE,
  permissionsAll,
  serviceID,
  setup,
  SHARD,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {SyncContext, ViewSyncerService} from './view-syncer.ts';

const OTHER_TASK_ID = 'other-task';

describe('view-syncer/stale-cached-cvr', () => {
  const lc = createSilentLogContext();

  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  let stateChanges: Subscription<ReplicaState>;
  let vs: ViewSyncerService;
  let viewSyncerDone: Promise<void>;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
  ) => Queue<Downstream>;
  let clearMocks: () => void;

  const SYNC_CONTEXT: SyncContext = {
    clientID: 'foo',
    profileID: 'p0000g00000003203',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    httpCookie: undefined,
    origin: undefined,
    userID: 'user-1',
    auth: undefined,
  };

  beforeEach<PgTest>(async ({testDBs}) => {
    ({
      replicaDbFile,
      cvrDB,
      upstreamDb,
      stateChanges,
      vs,
      viewSyncerDone,
      connect,
      clearMocks,
    } = await setup(testDBs, 'view_syncer_stale_cvr_test', permissionsAll));

    return async () => {
      vi.useRealTimers();
      clearMocks();
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * Stands in for a second view-syncer that the client group was rehomed to:
   * it loads the CVR under its own task ID (taking ownership) and commits a
   * config change, as it would when the client sends `changeDesiredQueries`.
   */
  async function advanceCVRFromAnotherViewSyncer(deletedQueryHash: string) {
    const now = Date.now();
    const store = new CVRStore(
      lc,
      cvrDB,
      SHARD,
      OTHER_TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const cvr = await store.load(lc, now);
    const updater = new CVRConfigDrivenUpdater(store, cvr, SHARD);
    updater.deleteDesiredQueries(SYNC_CONTEXT.clientID, [deletedQueryHash]);
    const {cvr: updated} = await updater.flush(
      lc,
      now,
      now,
      ttlClockFromNumber(now),
    );
    return updated.version;
  }

  test('reloads the CVR when a reconnecting client is ahead of the cached copy', async () => {
    // The client connects and is fully caught up with this view-syncer.
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: USERS_QUERY},
    ]);
    await nextPoke(client1);
    stateChanges.push({state: 'version-ready'});
    const [, , ...rest] = await nextPoke(client1);
    const pokeEnd = rest.at(-1);
    expect(pokeEnd?.[0]).toBe('pokeEnd');
    expect(pokeEnd?.[1]).toMatchObject({cookie: '01'});

    // The client group is served elsewhere for a while (e.g. the client's
    // network changed and its next connection landed on another
    // view-syncer). That view-syncer commits a config change and pokes the
    // client up to the new version. This view-syncer still holds the group,
    // with its CVR cached at the version it last wrote: the first socket is
    // half-open as far as the server knows, so the group is not shut down.
    const advanced = await advanceCVRFromAnotherViewSyncer('query-hash2');
    expect(advanced).toEqual({stateVersion: '01', configVersion: 1});

    // The client's next connection lands back on this view-syncer, with the
    // cookie the other view-syncer poked it to.
    const client2 = connect(
      {...SYNC_CONTEXT, wsID: 'ws2', baseCookie: '01:01'},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );

    // The cookie is valid: it is the version the CVR is durably at. Before the
    // fix this connection failed with InvalidConnectionRequestBaseCookie
    // ("CVR is at version 01"), judged against the stale cached copy, and the
    // client discarded its local state for a full resync.
    const [first] = await nextPoke(client2);
    expect(first).toEqual([
      'pokeStart',
      expect.objectContaining({baseCookie: '01:01'}),
    ]);
  });

  test('still rejects a client that is ahead of the reloaded CVR', async () => {
    const client1 = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    await nextPoke(client1);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client1);

    // No other view-syncer has written: the cached CVR is current, so the
    // reload finds the same version and the cookie is genuinely invalid.
    const client2 = connect(
      {...SYNC_CONTEXT, wsID: 'ws2', baseCookie: '01:05'},
      [{op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY}],
    );
    await expect(client2.dequeue()).rejects.toMatchObject({
      errorBody: {
        kind: ErrorKind.InvalidConnectionRequestBaseCookie,
        message: 'CVR is at version 01',
      },
    });
  });
});
