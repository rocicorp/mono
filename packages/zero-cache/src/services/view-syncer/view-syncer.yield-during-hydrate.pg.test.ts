import {
  afterEach,
  beforeEach,
  describe,
  expect,
  type MockInstance,
  vi,
} from 'vitest';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {DbFile} from '../../test/lite.ts';
import type {PostgresDB} from '../../types/pg.ts';
import type {Subscription} from '../../types/subscription.ts';
import type {ReplicaState} from '../replicator/replicator.ts';
import {CVRStore} from './cvr-store.ts';
import {CVRConfigDrivenUpdater, CVRQueryDrivenUpdater} from './cvr.ts';
import {ttlClockFromNumber} from './ttl-clock.ts';
import {
  ISSUES_QUERY,
  ISSUES_QUERY_WITH_RELATED,
  nextPoke,
  ON_FAILURE,
  permissionsAll,
  REPLICA_VERSION,
  serviceID,
  setup,
  SHARD,
  TASK_ID,
  USERS_QUERY,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {
  type SyncContext,
  TimeSliceTimer,
  type ViewSyncerService,
} from './view-syncer.ts';

describe('view-syncer/yield-during-hydrate', () => {
  let replicaDbFile: DbFile;
  let cvrDB: PostgresDB;
  let upstreamDb: PostgresDB;
  let vs: ViewSyncerService;
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
    tokenData: undefined,
    httpCookie: undefined,
    origin: undefined,
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
      viewSyncerDone,
      connect,
      stateChanges,
    } = await setup(testDBs, 'view_syncer_yield_test', permissionsAll));

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

  test('yields during hydration when time slice is exceeded', async () => {
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

    // Connect and add queries.  These queries will read a total of 9 rows.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY_WITH_RELATED},
    ]);

    await nextPoke(client);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client);

    // Verify that elapsedLapSpy was called once for each row
    expect(elapsedLapSpy).toHaveBeenCalledTimes(9);
    expect(yieldSpy).toHaveBeenCalledTimes(3);
    expectYieldMessage(yieldSpy, 'yield in processChanges');
  });

  test('yields during hydration of aded query when time slice is exceeded', async () => {
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

    // Connect and add queries.  These queries will read a total of 9 rows.
    const client = connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'query-hash1', ast: ISSUES_QUERY},
      {op: 'put', hash: 'query-hash2', ast: ISSUES_QUERY_WITH_RELATED},
    ]);

    await nextPoke(client);

    stateChanges.push({state: 'version-ready'});

    await nextPoke(client);

    // Verify that elapsedLapSpy was called once for each row
    expect(elapsedLapSpy).toHaveBeenCalledTimes(9);
    // Verify we yield on every third row
    expect(yieldSpy).toHaveBeenCalledTimes(3);
    expectYieldMessage(yieldSpy, 'yield in processChanges');

    // This query will read 3 rows
    await vs.changeDesiredQueries(SYNC_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {op: 'put', hash: 'query-hash3', ast: USERS_QUERY},
        ],
      },
    ]);

    await nextPoke(client);

    // Verify that elapsedLapSpy was called once for each row
    expect(elapsedLapSpy).toHaveBeenCalledTimes(12);
    // Verify we yield on every third row
    expect(yieldSpy).toHaveBeenCalledTimes(4);
    expectYieldMessage(yieldSpy, 'yield in processChanges');
  });

  test('yields during hydration of unchanged queries on client connectt when time slice is exceeded', async () => {
    const lc = createSilentLogContext();
    const cvrStore = new CVRStore(
      lc,
      cvrDB,
      upstreamDb,
      SHARD,
      TASK_ID,
      serviceID,
      ON_FAILURE,
    );
    const now = Date.now();
    const ttlClock = ttlClockFromNumber(now);
    const configUpdater = new CVRConfigDrivenUpdater(
      cvrStore,
      await cvrStore.load(lc, now),
      SHARD,
    );
    configUpdater.putDesiredQueries(CLIENT_ID, [
      {hash: 'query-hash1', ast: ISSUES_QUERY},
    ]);
    const {cvr: updated} = await configUpdater.flush(lc, now, now, ttlClock);
    const updater = new CVRQueryDrivenUpdater(
      cvrStore,
      updated,
      REPLICA_VERSION,
      REPLICA_VERSION,
    );
    updater.trackQueries(
      lc,
      [
        {id: 'query-hash1', transformationHash: 'gojujxnrngdx'},
        {id: 'lmids', transformationHash: '2jf1ycuha0k5e'},
        {id: 'mutationResults', transformationHash: 'lcbd7o1qvz9g'},
      ],
      [],
    );
    await updater.flush(lc, now, now, ttlClock);

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

    const emptyQuery = {
      table: 'issues',
      where: {
        type: 'or',
        conditions: [],
      },
    } as const;
    const client1 = connect({...SYNC_CONTEXT, baseCookie: '01'}, [
      {op: 'put', hash: 'query-empty', ast: emptyQuery},
    ]);

    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01",
            "pokeID": "01:01",
          },
        ],
        [
          "pokePart",
          {
            "desiredQueriesPatches": {
              "client1": [
                {
                  "hash": "query-empty",
                  "op": "put",
                },
              ],
            },
            "pokeID": "01:01",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01:01",
            "pokeID": "01:01",
          },
        ],
      ]
    `);

    stateChanges.push({state: 'version-ready'});

    expect(await nextPoke(client1)).toMatchInlineSnapshot(`
      [
        [
          "pokeStart",
          {
            "baseCookie": "01:01",
            "pokeID": "01:02",
          },
        ],
        [
          "pokePart",
          {
            "gotQueriesPatch": [
              {
                "hash": "query-empty",
                "op": "put",
              },
            ],
            "pokeID": "01:02",
          },
        ],
        [
          "pokeEnd",
          {
            "cookie": "01:02",
            "pokeID": "01:02",
          },
        ],
      ]
    `);

    // Verify that elapsedLapSpy was called once for each row
    expect(elapsedLapSpy).toHaveBeenCalledTimes(5);
    // Verify we yield on each third row
    expect(yieldSpy).toHaveBeenCalledTimes(1);
    expectYieldMessage(yieldSpy, 'yield in hydrateUnchangedQueries');
  });
});
