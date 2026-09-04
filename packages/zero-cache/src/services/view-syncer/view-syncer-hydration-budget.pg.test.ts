import {afterEach, expect, vi} from 'vitest';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {MonotonicClock} from './hydration-budget.ts';
import {
  addQuery,
  expectDesiredDel,
  expectDesiredPut,
  expectGotPut,
  inactivateQuery,
  ISSUES_QUERY,
  ISSUES_QUERY_WITH_RELATED,
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

const RESTARTED_CONTEXT = {...SYNC_CONTEXT, clientID: 'bar', wsID: 'ws2'};

/**
 * Queries used by every test in this file:
 *
 * - `active` stays desired by client `foo`, so it is always required.
 * - `later-expiry` and `earlier-expiry` are inactivated, so they are optional
 *   and ordered `later-expiry` first (optional hydration runs from latest to
 *   earliest effective expiration).
 *
 * `active` and `later-expiry` both match issues '1' and '2', which lets the
 * tests check that evicting `later-expiry` does not strip row references still
 * held by `active`.
 */
const ACTIVE = 'active';
const LATER_EXPIRY = 'later-expiry';
const EARLIER_EXPIRY = 'earlier-expiry';

/**
 * A clock that never advances, so the budget is never spent no matter how many
 * times it is read.
 */
const NEVER_EXHAUSTED: MonotonicClock = () => 0;

/**
 * A clock that jumps a full hour on every read, so any budget is spent at the
 * first check after the budget is created, no matter how many times it is read.
 */
function alwaysExhausted(): MonotonicClock {
  let now = 0;
  return () => (now += 3_600_000);
}

/**
 * A clock that advances `stepMs` per read. `HydrationBudget` reads it once when
 * it is created and once per `exhausted()` call, and `exhausted()` is called
 * exactly once immediately before each optional query. So with `stepMs` of 100
 * and a budget of 150ms the first optional query runs (elapsed 100) and the
 * second is evicted (elapsed 200).
 */
function steppingClock(stepMs: number): MonotonicClock {
  let now = 0;
  return () => {
    const value = now;
    now += stepMs;
    return value;
  };
}

afterEach(() => {
  vi.useRealTimers();
});

/**
 * Hydrates all three queries, then inactivates the two optional ones so that a
 * restarted view-syncer sees them as gotten-but-inactive hydration candidates.
 */
async function seedInactiveQueries(
  testDBs: PgTest['testDBs'],
  dbName: string,
  hydrationBudgetMs: number,
) {
  const initial = await setup(testDBs, dbName, permissionsAll, {
    hydrationBudgetMs,
  });
  const client = initial.connect(SYNC_CONTEXT, [
    {op: 'put', hash: ACTIVE, ast: ISSUES_QUERY, ttl: 60_000},
    {
      op: 'put',
      hash: LATER_EXPIRY,
      ast: ISSUES_QUERY_WITH_RELATED,
      ttl: 60_000,
    },
    {op: 'put', hash: EARLIER_EXPIRY, ast: USERS_QUERY, ttl: 30_000},
  ]);

  await nextPoke(client); // desired queries
  initial.stateChanges.push({state: 'version-ready'});
  await nextPoke(client); // initial hydration

  // Inactivated in this order so that LATER_EXPIRY (ttl 60s) keeps a later
  // effective expiration than EARLIER_EXPIRY (ttl 30s).
  await inactivateQuery(initial.vs, SYNC_CONTEXT, LATER_EXPIRY);
  await expectDesiredDel(client, 'foo', LATER_EXPIRY);
  await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
  await expectDesiredDel(client, 'foo', EARLIER_EXPIRY);

  await initial.vs.stop();
  await initial.viewSyncerDone;
  return initial;
}

function restartWithClock(
  initial: Awaited<ReturnType<typeof seedInactiveQueries>>,
  monotonicClock: MonotonicClock | undefined,
) {
  return restartViewSyncer({
    databaseStorage: initial.databaseStorage,
    replicaDbFile: initial.replicaDbFile,
    cvrDB: initial.cvrDB,
    config: initial.config,
    customQueryTransformer: initial.customQueryTransformer,
    setTimeoutFn: initial.setTimeoutFn,
    monotonicClock,
  });
}

/** Query hashes that are not deleted in the persisted CVR. */
async function liveQueries(
  initial: Awaited<ReturnType<typeof seedInactiveQueries>>,
): Promise<string[]> {
  const rows = await initial.cvrDB<{queryHash: string}[]>`
    SELECT "queryHash"
      FROM "this_app_2/cvr".queries
     WHERE "clientGroupID" = ${serviceID}
       AND deleted = false
       AND "queryHash" IN (${ACTIVE}, ${LATER_EXPIRY}, ${EARLIER_EXPIRY})
     ORDER BY "queryHash"`;
  return rows.map(({queryHash}) => queryHash);
}

/** Row keys in the persisted CVR that still reference `queryHash`. */
function rowsReferencing(
  initial: Awaited<ReturnType<typeof seedInactiveQueries>>,
  queryHash: string,
): Promise<{table: string; rowKey: unknown}[]> {
  return initial.cvrDB<{table: string; rowKey: unknown}[]>`
    SELECT "table", "rowKey"
      FROM "this_app_2/cvr".rows
     WHERE "clientGroupID" = ${serviceID}
       AND "refCounts" ? ${queryHash}
     ORDER BY "table", "rowKey"`;
}

test<PgTest>('evicts every optional query when the budget is spent before optional hydration', async ({
  testDBs,
}) => {
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_spent',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    restarted = restartWithClock(initial, alwaysExhausted());
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});

    const poke = await nextPokeParts(client);
    expect(poke[0].gotQueriesPatch).toEqual(
      expect.arrayContaining([
        {hash: LATER_EXPIRY, op: 'del'},
        {hash: EARLIER_EXPIRY, op: 'del'},
      ]),
    );

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([ACTIVE]);
    });

    // The required query is still hydrated, as are the internal queries.
    // (pipelineHashes() reports transformation hashes, so identity is checked
    // against the persisted CVR above.)
    expect(restarted.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      1,
    );
    expect(restarted.vs.pipelineHashes().some(q => q.internal)).toBe(true);

    // The evicted queries hold no row references...
    expect(await rowsReferencing(initial, LATER_EXPIRY)).toEqual([]);
    expect(await rowsReferencing(initial, EARLIER_EXPIRY)).toEqual([]);
    // ...but rows they shared with the required query survive.
    const activeRows = await rowsReferencing(initial, ACTIVE);
    expect(activeRows.map(({rowKey}) => rowKey)).toEqual(
      expect.arrayContaining([{id: '1'}, {id: '2'}]),
    );
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('retains every optional query when the budget is not spent', async ({
  testDBs,
}) => {
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_unspent',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    restarted = restartWithClock(initial, NEVER_EXHAUSTED);
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([
        ACTIVE,
        EARLIER_EXPIRY,
        LATER_EXPIRY,
      ]);
    });
    expect(restarted.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      3,
    );
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('a disabled budget never evicts an optional query', async ({
  testDBs,
}) => {
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_disabled',
    0,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    // Even a clock that would exhaust any enabled budget changes nothing.
    restarted = restartWithClock(initial, alwaysExhausted());
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([
        ACTIVE,
        EARLIER_EXPIRY,
        LATER_EXPIRY,
      ]);
    });
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('stops optional hydration at a query boundary and permits a re-request', async ({
  testDBs,
}) => {
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_boundary',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    restarted = restartWithClock(initial, steppingClock(100));
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});

    const poke = await nextPokeParts(client);
    expect(poke[0].gotQueriesPatch).toContainEqual({
      hash: EARLIER_EXPIRY,
      op: 'del',
    });

    // The query with the later expiration started before the budget was spent
    // and therefore ran to completion; only the unstarted one was evicted.
    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([ACTIVE, LATER_EXPIRY]);
    });
    expect(await rowsReferencing(initial, EARLIER_EXPIRY)).toEqual([]);
    expect(
      (await rowsReferencing(initial, LATER_EXPIRY)).length,
    ).toBeGreaterThan(0);

    // A re-request hydrates the evicted query again, now as an active query.
    await addQuery(
      restarted.vs,
      RESTARTED_CONTEXT,
      EARLIER_EXPIRY,
      USERS_QUERY,
      30_000,
    );
    await expectDesiredPut(client, 'bar', EARLIER_EXPIRY);
    await expectGotPut(client, EARLIER_EXPIRY);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([
        ACTIVE,
        EARLIER_EXPIRY,
        LATER_EXPIRY,
      ]);
    });
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
