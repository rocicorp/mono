import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockInstance,
  vi,
} from 'vitest';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import type {FakeReplicator} from '../replicator/test-utils.ts';
import {
  ALL_ISSUES_QUERY,
  ISSUES_QUERY_WITH_OWNER,
  messages,
  nextPoke,
  permissionsAll,
  setup,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {
  type SyncContext,
  TimeSliceTimer,
  type ViewSyncerService,
} from './view-syncer.ts';

describe('view-syncer/yield-during-advance', () => {
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  let vs: ViewSyncerService;
  let replicator: FakeReplicator;
  let stateChanges: Subscription<ReplicaState>;
  let viewSyncerDone: Promise<void>;
  let connect: (
    ctx: SyncContext,
    desiredQueriesPatch: UpQueriesPatch,
  ) => Queue<Downstream>;

  const CLIENT_ID = 'client1';

  const SYNC_CONTEXT: SyncContext = {
    clientID: CLIENT_ID,
    profileID: 'p0000g00000003203',
    wsID: 'ws1',
    baseCookie: null,
    protocolVersion: PROTOCOL_VERSION,
    schemaVersion: 2,
    tokenData: undefined,
    httpCookie: undefined,
  };

  afterEach(() => {
    vi.restoreAllMocks();
  });

  beforeEach<PgTest>(async ({testDBs}) => {
    ({
      replicaDbFile,
      cvrDB,
      upstreamDb,
      vs,
      replicator,
      viewSyncerDone,
      connect,
      stateChanges,
    } = await setup(testDBs, 'view_syncer_yield_advance_test', permissionsAll));

    return async () => {
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, upstreamDb);
      replicaDbFile.delete();
    };
  });

  function expectYieldMessage(spy: MockInstance, message: string) {
    for (const call of spy.mock.calls) {
      expect(call[0]).toBe(message);
    }
  }

  test('yields during advance when time slice is exceeded, even when no rows are read', async () => {
    // 1. Connect and hydrate initial state
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ALL_ISSUES_QUERY},
    ]);

    await nextPoke(client);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);

    // 2. Setup mock for TimeSliceTimer
    const yieldSpy = vi.spyOn(TimeSliceTimer.prototype, 'yieldProcess');

    let elapsedLapCallCountSinceYield = 0;
    let lastYieldCallCount = yieldSpy.mock.calls.length;
    let expectYieldBeforeNext = false;
    const elapsedLapSpy = vi
      .spyOn(TimeSliceTimer.prototype, 'elapsedLap')
      .mockImplementation(function (this: TimeSliceTimer) {
        if (expectYieldBeforeNext) {
          expect(yieldSpy).toBeCalledTimes(lastYieldCallCount + 1);
        }
        if (lastYieldCallCount !== yieldSpy.mock.calls.length) {
          lastYieldCallCount = yieldSpy.mock.calls.length;
          elapsedLapCallCountSinceYield = 0;
        }
        elapsedLapCallCountSinceYield++;
        const elapsed = elapsedLapCallCountSinceYield * 100;
        expectYieldBeforeNext = elapsed > YIELD_THRESHOLD_MS;
        return elapsed;
      });

    expect(elapsedLapSpy).toBeCalledTimes(0);

    // 3. Insert rows into replica via replicator to trigger changes
    // Inserting 9 rows should results in 9 calls to elapsedLap, once per
    // advance diff.
    replicator.processTransaction(
      '02',
      messages.insert('issues', {
        id: '11',
        title: 't11',
        owner: 'u11',
        big: 11,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '12',
        title: 't12',
        owner: 'u12',
        big: 12,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '13',
        title: 't13',
        owner: 'u13',
        big: 13,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '14',
        title: 't14',
        owner: 'u14',
        big: 14,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '15',
        title: 't15',
        owner: 'u15',
        big: 15,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '16',
        title: 't16',
        owner: 'u16',
        big: 16,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '17',
        title: 't17',
        owner: 'u17',
        big: 17,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '18',
        title: 't18',
        owner: 'u18',
        big: 18,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '19',
        title: 't19',
        owner: 'u19',
        big: 19,
        _0_version: '02',
      }),
    );

    // 4. Trigger advance
    stateChanges.push({state: 'version-ready'});

    // 5. Wait for the poke resulting from the advance
    await nextPoke(client);

    // 6. Verify yields
    // 9 elapsedLap calls, once per advance diff, no rows read so no
    // elapseLap calls from TableSource
    expect(elapsedLapSpy).toHaveBeenCalledTimes(9);
    // once for every 3 elapsedLap calls
    expect(yieldSpy).toHaveBeenCalledTimes(3);
    expectYieldMessage(yieldSpy, 'yield in processChanges');
  });

  test('yields during advance when time slice is exceeded when yield come from TableSource row reads', async () => {
    // 1. Connect and hydrate initial state
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY_WITH_OWNER},
    ]);

    await nextPoke(client);
    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);

    // 2. Insert 6 issue rows all with owner '200'
    replicator.processTransaction(
      '02',
      messages.insert('issues', {
        id: '11',
        title: 't11',
        owner: '200',
        big: 11,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '12',
        title: 't12',
        owner: '200',
        big: 12,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '13',
        title: 't13',
        owner: '200',
        big: 13,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '14',
        title: 't14',
        owner: '200',
        big: 14,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '15',
        title: 't15',
        owner: '200',
        big: 15,
        _0_version: '02',
      }),
      messages.insert('issues', {
        id: '16',
        title: 't16',
        owner: '200',
        big: 16,
        _0_version: '02',
      }),
    );

    stateChanges.push({state: 'version-ready'});
    await nextPoke(client);

    // 3 Setup mock for TimeSliceTimer
    const yieldSpy = vi.spyOn(TimeSliceTimer.prototype, 'yieldProcess');

    let elapsedLapCallCountSinceYield = 0;
    let lastYieldCallCount = yieldSpy.mock.calls.length;
    let expectYieldBeforeNext = false;
    const elapsedLapSpy = vi
      .spyOn(TimeSliceTimer.prototype, 'elapsedLap')
      .mockImplementation(function (this: TimeSliceTimer) {
        if (expectYieldBeforeNext) {
          expect(yieldSpy).toBeCalledTimes(lastYieldCallCount + 1);
        }
        if (lastYieldCallCount !== yieldSpy.mock.calls.length) {
          lastYieldCallCount = yieldSpy.mock.calls.length;
          elapsedLapCallCountSinceYield = 0;
        }
        elapsedLapCallCountSinceYield++;
        const elapsed = elapsedLapCallCountSinceYield * 100;
        expectYieldBeforeNext = elapsed > YIELD_THRESHOLD_MS;
        return elapsed;
      });

    expect(elapsedLapSpy).toBeCalledTimes(0);

    // 3. Insert a user row with id '200' to trigger a push
    // that will read the 6 issues rows with owner '200'
    replicator.processTransaction(
      '03',
      messages.insert('users', {
        id: '200',
        name: 'NewUser',
        _0_version: '03',
      }),
    );

    // 4. Trigger advance
    stateChanges.push({state: 'version-ready'});

    // 5. Wait for the poke resulting from the advance
    await nextPoke(client);

    // 6. Verify yields
    // 6 rows + 1 for the advance diff
    expect(elapsedLapSpy).toHaveBeenCalledTimes(7);
    // once for every 3 elapsedLap calls
    expect(yieldSpy).toHaveBeenCalledTimes(2);
    expectYieldMessage(yieldSpy, 'yield in processChanges');
  });
});
