import {afterEach, expect, vi} from 'vitest';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {type PgTest, test} from '../../test/db.ts';
import {
  addQuery,
  expectDesiredDel,
  expectDesiredPut,
  expectGotPut,
  inactivateQuery,
  ISSUES_QUERY,
  nextPoke,
  nextPokeParts,
  permissionsAll,
  restartViewSyncer,
  serviceID,
  setup,
  USERS_QUERY,
} from './view-syncer-test-util.ts';
import type {SyncContext} from './view-syncer.ts';

const SYNC_CONTEXT: SyncContext = {
  clientID: 'foo',
  profileID: 'p0000g00000003203',
  wsID: 'ws1',
  baseCookie: null,
  protocolVersion: PROTOCOL_VERSION,
  httpCookie: undefined,
  origin: undefined,
  userID: 'bar',
  auth: undefined,
};

afterEach(() => {
  vi.useRealTimers();
});

test<PgTest>('stops inactive hydration at a query boundary and permits a re-request', async ({
  testDBs,
}) => {
  const initial = await setup(
    testDBs,
    'view_syncer_hydration_budget_test',
    permissionsAll,
    {hydrationBudgetMs: 4},
  );
  const client = initial.connect(SYNC_CONTEXT, [
    {op: 'put', hash: 'shorter-ttl', ast: ISSUES_QUERY, ttl: 30_000},
    {op: 'put', hash: 'longer-ttl', ast: USERS_QUERY, ttl: 60_000},
  ]);

  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    await nextPoke(client); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(client); // initial hydration

    await inactivateQuery(initial.vs, SYNC_CONTEXT, 'shorter-ttl');
    await expectDesiredDel(client, 'foo', 'shorter-ttl');
    await inactivateQuery(initial.vs, SYNC_CONTEXT, 'longer-ttl');
    await expectDesiredDel(client, 'foo', 'longer-ttl');

    await initial.vs.stop();
    await initial.viewSyncerDone;

    // Only the budget asks this clock for time. The fourth boundary check is
    // immediately before the second inactive query, so the first query is
    // retained and the second is evicted.
    let monotonicNow = 0;
    restarted = restartViewSyncer({
      databaseStorage: initial.databaseStorage,
      replicaDbFile: initial.replicaDbFile,
      cvrDB: initial.cvrDB,
      config: initial.config,
      customQueryTransformer: initial.customQueryTransformer,
      setTimeoutFn: initial.setTimeoutFn,
      monotonicClock: () => monotonicNow++,
    });
    const restartedContext = {
      ...SYNC_CONTEXT,
      clientID: 'bar',
      wsID: 'ws2',
    };
    const restartedClient = restarted.connect(restartedContext, []);
    restarted.stateChanges.push({state: 'version-ready'});
    const budgetPoke = await nextPokeParts(restartedClient);
    expect(budgetPoke[0].gotQueriesPatch).toContainEqual({
      hash: 'shorter-ttl',
      op: 'del',
    });

    await vi.waitFor(
      async () => {
        const queries = await initial.cvrDB`
          SELECT "queryHash", deleted
            FROM "this_app_2/cvr".queries
           WHERE "clientGroupID" = ${serviceID}
             AND "queryHash" IN ('shorter-ttl', 'longer-ttl')
           ORDER BY "queryHash"`;
        expect(queries).toEqual([
          {queryHash: 'longer-ttl', deleted: false},
          {queryHash: 'shorter-ttl', deleted: true},
        ]);
      },
      {timeout: 10_000, interval: 20},
    );
    expect(
      await initial.cvrDB`
        SELECT 1
          FROM "this_app_2/cvr".rows
         WHERE "clientGroupID" = ${serviceID}
           AND "refCounts" ? 'shorter-ttl'`,
    ).toEqual([]);
    expect(restarted.vs.pipelineHashes().some(q => q.internal)).toBe(true);
    expect(restarted.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      1,
    );

    await addQuery(
      restarted.vs,
      restartedContext,
      'shorter-ttl',
      ISSUES_QUERY,
      30_000,
    );
    await expectDesiredPut(restartedClient, 'bar', 'shorter-ttl');
    await expectGotPut(restartedClient, 'shorter-ttl');

    await vi.waitFor(async () => {
      const queries = await initial.cvrDB`
        SELECT deleted
          FROM "this_app_2/cvr".queries
         WHERE "clientGroupID" = ${serviceID}
           AND "queryHash" = 'shorter-ttl'`;
      expect(queries).toEqual([{deleted: false}]);
    });
  } finally {
    initial.clearMocks();
    if (restarted) {
      await restarted.vs.stop();
      await restarted.viewSyncerDone;
    } else {
      await initial.vs.stop();
      await initial.viewSyncerDone;
    }
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
