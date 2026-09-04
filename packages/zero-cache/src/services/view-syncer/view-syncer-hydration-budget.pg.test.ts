import {afterEach, expect, vi} from 'vitest';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
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
  const lastPokes = await nextPoke(client);
  expect(
    lastPokes
      .filter(msg => msg[0] === 'pokePart')
      .flatMap(([, body]) => body.desiredQueriesPatches?.foo ?? []),
  ).toEqual([{op: 'del', hash: EARLIER_EXPIRY}]);
  // The cookie this client last saw, for the reconnect test below.
  const lastCookie = lastPokeCookie(lastPokes);

  await initial.vs.stop();
  await initial.viewSyncerDone;
  return {...initial, lastCookie};
}

/** The cookie of the final `pokeEnd` in a downstream sequence. */
function lastPokeCookie(pokes: Downstream[]): string | null {
  for (let i = pokes.length - 1; i >= 0; i--) {
    const msg = pokes[i];
    if (msg[0] === 'pokeEnd') {
      return msg[1].cookie ?? null;
    }
  }
  return null;
}

type Harness = Awaited<ReturnType<typeof setup>>;

function restartWithClock(
  initial: Harness,
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
async function liveQueries(initial: Harness): Promise<string[]> {
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
  initial: Harness,
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

test<PgTest>('an active query is never evicted, even when another client inactivated it', async ({
  testDBs,
}) => {
  // `shared` is inactivated by client `foo` but still desired by client
  // `keeper`, so the client group must keep treating it as required.
  const initial = await setup(
    testDBs,
    'vs_hydration_budget_multi_client',
    permissionsAll,
    {hydrationBudgetMs: 150},
  );
  const KEEPER_CONTEXT = {...SYNC_CONTEXT, clientID: 'keeper', wsID: 'ws-k'};
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    const foo = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'shared', ast: ISSUES_QUERY, ttl: 60_000},
      {op: 'put', hash: EARLIER_EXPIRY, ast: USERS_QUERY, ttl: 30_000},
    ]);
    await nextPoke(foo); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(foo); // initial hydration

    const keeper = initial.connect(KEEPER_CONTEXT, [
      {op: 'put', hash: 'shared', ast: ISSUES_QUERY, ttl: 60_000},
    ]);
    await nextPoke(keeper); // catchup
    await nextPoke(foo); // keeper's desired queries

    // `foo` gives up both queries, but `keeper` still desires `shared`.
    await inactivateQuery(initial.vs, SYNC_CONTEXT, 'shared');
    await expectDesiredDel(foo, 'foo', 'shared');
    await nextPoke(keeper);
    await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
    await expectDesiredDel(foo, 'foo', EARLIER_EXPIRY);
    await nextPoke(keeper);

    await initial.vs.stop();
    await initial.viewSyncerDone;

    restarted = restartWithClock(initial, alwaysExhausted());
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      const rows = await initial.cvrDB<{queryHash: string}[]>`
        SELECT "queryHash"
          FROM "this_app_2/cvr".queries
         WHERE "clientGroupID" = ${serviceID}
           AND deleted = false
           AND "queryHash" IN ('shared', ${EARLIER_EXPIRY})`;
      // The fully-inactive query is evicted; the one `keeper` still wants is
      // required and survives a completely spent budget.
      expect(rows.map(({queryHash}) => queryHash)).toEqual(['shared']);
    });
    expect((await rowsReferencing(initial, 'shared')).length).toBeGreaterThan(
      0,
    );
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('a never-gotten inactive query is hydrated when the budget is disabled', async ({
  testDBs,
}) => {
  // Inactivated before the first hydration, so it is inactive with no gotten
  // state. With the budget disabled this must behave as it always has: the
  // query is hydrated and kept for the rest of its TTL.
  const initial = await setup(
    testDBs,
    'vs_hydration_budget_never_gotten_off',
    permissionsAll,
    {hydrationBudgetMs: 0},
  );
  try {
    const client = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: EARLIER_EXPIRY, ast: USERS_QUERY, ttl: 30_000},
    ]);
    await nextPoke(client); // desired queries; no hydration yet
    await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
    await expectDesiredDel(client, 'foo', EARLIER_EXPIRY);

    initial.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([EARLIER_EXPIRY]);
    });
    expect(
      (await rowsReferencing(initial, EARLIER_EXPIRY)).length,
    ).toBeGreaterThan(0);
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('a never-gotten inactive query is dropped when the budget is enabled', async ({
  testDBs,
}) => {
  const initial = await setup(
    testDBs,
    'vs_hydration_budget_never_gotten_on',
    permissionsAll,
    {hydrationBudgetMs: 150},
  );
  try {
    const client = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: EARLIER_EXPIRY, ast: USERS_QUERY, ttl: 30_000},
    ]);
    await nextPoke(client);
    await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
    await expectDesiredDel(client, 'foo', EARLIER_EXPIRY);

    initial.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([]);
    });
    expect(await rowsReferencing(initial, EARLIER_EXPIRY)).toEqual([]);
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('required and optional custom queries are transformed in one round trip', async ({
  testDBs,
}) => {
  // A fresh connection re-transforms every custom query for authorization.
  // Required and optional queries must go out in a single request: splitting
  // them would double the round trips to the user's query endpoint.
  const initial = await setup(
    testDBs,
    'vs_hydration_budget_one_batch',
    permissionsAll,
    {hydrationBudgetMs: 150, queryFetchMode: 'empty-validation'},
  );
  try {
    initial.queryFetch.respond([
      {id: 'custom-active', name: 'named-active', ast: ISSUES_QUERY},
      {id: 'custom-inactive-1', name: 'named-inactive-1', ast: USERS_QUERY},
      {
        id: 'custom-inactive-2',
        name: 'named-inactive-2',
        ast: ISSUES_QUERY_WITH_RELATED,
      },
    ]);
    const foo = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'custom-active', name: 'named-active', args: []},
      {
        op: 'put',
        hash: 'custom-inactive-1',
        name: 'named-inactive-1',
        args: [],
      },
      {
        op: 'put',
        hash: 'custom-inactive-2',
        name: 'named-inactive-2',
        args: [],
      },
    ]);
    await nextPoke(foo);
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(foo);

    await inactivateQuery(initial.vs, SYNC_CONTEXT, 'custom-inactive-1');
    await expectDesiredDel(foo, 'foo', 'custom-inactive-1');
    await inactivateQuery(initial.vs, SYNC_CONTEXT, 'custom-inactive-2');
    await expectDesiredDel(foo, 'foo', 'custom-inactive-2');

    // A second connection re-transforms all custom queries for authorization:
    // one required (still desired by `foo`) and two optional. Spy on the
    // transformer rather than on fetch, so the assertion counts the batches the
    // view-syncer forms rather than whichever of them the transform cache
    // happens to serve.
    const transformer = initial.customQueryTransformer;
    expect(transformer).toBeDefined();
    using transformSpy = vi.spyOn(transformer!, 'transform');

    const bar = initial.connect(RESTARTED_CONTEXT, []);
    await nextPoke(bar);

    expect(transformSpy).toHaveBeenCalledTimes(1);
    expect(Array.from(transformSpy.mock.calls[0][1], q => q.id).sort()).toEqual(
      ['custom-active', 'custom-inactive-1', 'custom-inactive-2'],
    );
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('a client reconnecting at its old cookie is told the evicted query and its rows are gone', async ({
  testDBs,
}) => {
  // The client already holds the evicted query's rows, so the server has to
  // take them away. This is the same server-initiated removal that natural TTL
  // expiration performs.
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_reconnect',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    restarted = restartWithClock(initial, alwaysExhausted());
    // Reconnect as the original client at the cookie it last saw. A null cookie
    // would make this a fresh sync and the test would not be exercising
    // server-initiated removal against a client that holds the rows.
    expect(initial.lastCookie).toBeTypeOf('string');
    const client = restarted.connect(
      {...SYNC_CONTEXT, baseCookie: initial.lastCookie, wsID: 'ws-reconnect'},
      [],
    );
    restarted.stateChanges.push({state: 'version-ready'});

    const parts = await nextPokeParts(client);
    const gotPatches = parts.flatMap(p => p.gotQueriesPatch ?? []);
    expect(gotPatches).toEqual(
      expect.arrayContaining([
        {hash: LATER_EXPIRY, op: 'del'},
        {hash: EARLIER_EXPIRY, op: 'del'},
      ]),
    );

    // Rows that only the evicted queries referenced are deleted at the client;
    // rows still held by the required query are not.
    const rowPatches = parts.flatMap(p => p.rowsPatch ?? []);
    const deletedTables = rowPatches
      .filter(p => p.op === 'del')
      .map(p => p.tableName);
    expect(deletedTables).toContain('labels');
    expect(deletedTables).toContain('users');
    expect(
      rowPatches.filter(p => p.op === 'del' && p.tableName === 'issues'),
    ).toEqual([]);
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
