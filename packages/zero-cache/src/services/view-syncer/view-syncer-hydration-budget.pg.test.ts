import {afterEach, expect, vi} from 'vitest';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {MonotonicClock} from './hydration-budget.ts';
import {
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
 * A clock that spends a full hour inside each of the two transform round trips
 * of a restart pass and no time at all anywhere else.
 *
 * The readings follow the pass's read order: construction, then the start and
 * end of each `excluding()` round trip, then one read per optional query
 * boundary. Both hours therefore fall between an `excluding()` pair and are
 * discounted, leaving zero elapsed hydration time.
 */
function slowTransforms(): MonotonicClock {
  const hour = 3_600_000;
  const readings = [0, 0, hour, hour, 2 * hour];
  let i = 0;
  return () => readings[i++] ?? 2 * hour;
}

/**
 * A clock that advances `stepMs` per read. `HydrationBudget` reads it once when
 * it is created, twice per `excluding()` call, and once per `exhausted()` call;
 * `exhausted()` runs exactly once immediately before each optional query. An
 * `excluding()` call discounts the step between its own two reads, so each of
 * them nets out to one step of elapsed time.
 *
 * A restart pass makes two excluded transform round trips (one in
 * `#hydrateUnchangedQueries`, one in `#syncQueryPipelineSet`), so the first
 * `exhausted()` sees `3 * stepMs` and the second `4 * stepMs`. With `stepMs` of
 * 40 and a budget of 150ms the first optional query runs (elapsed 120) and the
 * second is evicted (elapsed 160).
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
 * Custom (named) queries are the production path; plain-AST client queries are
 * deprecated. Tests default to `'custom'` and cover `'legacy'` only as a
 * regression guard.
 */
type QueryKind = 'custom' | 'legacy';

/** The transform responses backing the three custom queries. */
const TRANSFORMED = [
  {id: ACTIVE, name: 'named-active', ast: ISSUES_QUERY},
  {id: LATER_EXPIRY, name: 'named-later', ast: ISSUES_QUERY_WITH_RELATED},
  {id: EARLIER_EXPIRY, name: 'named-earlier', ast: USERS_QUERY},
] as const;

function desiredQueries(kind: QueryKind): UpQueriesPatch {
  if (kind === 'legacy') {
    return TRANSFORMED.map(({id, ast}) => ({
      op: 'put' as const,
      hash: id,
      ast,
      ttl: id === EARLIER_EXPIRY ? 30_000 : 60_000,
    }));
  }
  return TRANSFORMED.map(({id, name}) => ({
    op: 'put' as const,
    hash: id,
    name,
    args: [],
    ttl: id === EARLIER_EXPIRY ? 30_000 : 60_000,
  }));
}

async function setupHarness(
  testDBs: PgTest['testDBs'],
  dbName: string,
  hydrationBudgetMs: number,
  kind: QueryKind,
) {
  const initial = await setup(testDBs, dbName, permissionsAll, {
    hydrationBudgetMs,
    ...(kind === 'custom' && {queryFetchMode: 'empty-validation' as const}),
  });
  if (kind === 'custom') {
    // The transformer is asked for whichever subset a pass needs; responding
    // with all three is fine because unrequested ids are ignored.
    initial.queryFetch.respond([...TRANSFORMED]);
  }
  return initial;
}

/**
 * Hydrates all three queries, then inactivates the two optional ones so that a
 * restarted view-syncer sees them as gotten-but-inactive hydration candidates.
 */
async function seedInactiveQueries(
  testDBs: PgTest['testDBs'],
  dbName: string,
  hydrationBudgetMs: number,
  kind: QueryKind = 'custom',
) {
  const initial = await setupHarness(testDBs, dbName, hydrationBudgetMs, kind);
  const client = initial.connect(SYNC_CONTEXT, desiredQueries(kind));

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

  if (kind === 'custom') {
    // Guard against a silent fallback to the legacy path: these queries must
    // really be going through the custom-query transformer.
    expect(initial.queryFetch.transformCalls.length).toBeGreaterThan(0);
    const names = await initial.cvrDB<{queryName: string | null}[]>`
      SELECT "queryName" FROM "this_app_2/cvr".queries
       WHERE "clientGroupID" = ${serviceID}
         AND "queryHash" IN (${ACTIVE}, ${LATER_EXPIRY}, ${EARLIER_EXPIRY})`;
    expect(names.map(({queryName}) => queryName).sort()).toEqual([
      'named-active',
      'named-earlier',
      'named-later',
    ]);
  }

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

/**
 * The persisted CVR state of `queryHash`: whether its record survives, whether
 * it has been gotten, and the per-client desired rows that carry its remaining
 * TTL.
 */
async function persistedQueryState(initial: Harness, queryHash: string) {
  const [query] = await initial.cvrDB<{transformationHash: string | null}[]>`
    SELECT "transformationHash"
      FROM "this_app_2/cvr".queries
     WHERE "clientGroupID" = ${serviceID}
       AND "queryHash" = ${queryHash}
       AND deleted = false`;
  const desires = await initial.cvrDB<
    {inactivatedAtMs: number | null; ttlMs: number | null}[]
  >`
    SELECT "inactivatedAtMs", "ttlMs"
      FROM "this_app_2/cvr".desires
     WHERE "clientGroupID" = ${serviceID}
       AND "queryHash" = ${queryHash}
     ORDER BY "clientID"`;
  const transformationHash = query?.transformationHash ?? null;
  return {
    present: query !== undefined,
    gotten: transformationHash !== null,
    desires: desires.map(({inactivatedAtMs, ttlMs}) => ({
      inactivated: inactivatedAtMs !== null,
      ttlMs,
    })),
  };
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

test<PgTest>('transform round trips do not spend the budget', async ({
  testDBs,
}) => {
  // The budget bounds hydration, not the latency of the user's query endpoint.
  // Two hour-long transform round trips must leave a 150ms budget untouched:
  // evicting inactive queries cannot recover remote latency, so charging it to
  // the budget would evict every optional query on every pass of any
  // deployment whose endpoint is slower than the budget.
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_slow_transform',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    restarted = restartWithClock(initial, slowTransforms());
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
    restarted = restartWithClock(initial, steppingClock(40));
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

    // A re-request of the same named query hydrates it again, now as an
    // active query.
    await restarted.vs.changeDesiredQueries(RESTARTED_CONTEXT, [
      'changeDesiredQueries',
      {
        desiredQueriesPatch: [
          {
            op: 'put',
            hash: EARLIER_EXPIRY,
            name: 'named-earlier',
            args: [],
            ttl: 30_000,
          },
        ],
      },
    ]);
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
  const initial = await setupHarness(
    testDBs,
    'vs_hydration_budget_multi_client',
    150,
    'custom',
  );
  const KEEPER_CONTEXT = {...SYNC_CONTEXT, clientID: 'keeper', wsID: 'ws-k'};
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    initial.queryFetch.respond([
      {id: 'shared', name: 'named-shared', ast: ISSUES_QUERY},
      {id: EARLIER_EXPIRY, name: 'named-earlier', ast: USERS_QUERY},
    ]);
    const foo = initial.connect(SYNC_CONTEXT, [
      {op: 'put', hash: 'shared', name: 'named-shared', args: [], ttl: 60_000},
      {
        op: 'put',
        hash: EARLIER_EXPIRY,
        name: 'named-earlier',
        args: [],
        ttl: 30_000,
      },
    ]);
    await nextPoke(foo); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(foo); // initial hydration

    const keeper = initial.connect(KEEPER_CONTEXT, [
      {op: 'put', hash: 'shared', name: 'named-shared', args: [], ttl: 60_000},
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
  const initial = await setupHarness(
    testDBs,
    'vs_hydration_budget_never_gotten_off',
    0,
    'custom',
  );
  try {
    const client = initial.connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: EARLIER_EXPIRY,
        name: 'named-earlier',
        args: [],
        ttl: 30_000,
      },
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

test<PgTest>('a never-gotten inactive query is left intact when the budget is enabled', async ({
  testDBs,
}) => {
  // A query inactivated before it was ever gotten cannot be an optional
  // hydration candidate: a budget eviction converts the query to a removal
  // after trackQueries(), which for a never-gotten query would mean retracting
  // a 'put' with a 'del' in the same poke. So it is skipped -- but skipping
  // must not destroy it. Its record, its desired row and its remaining TTL all
  // survive for a later pass, or a re-desiring client, to pick up.
  const initial = await setupHarness(
    testDBs,
    'vs_hydration_budget_never_gotten_on',
    150,
    'custom',
  );
  try {
    const client = initial.connect(SYNC_CONTEXT, [
      {
        op: 'put',
        hash: EARLIER_EXPIRY,
        name: 'named-earlier',
        args: [],
        ttl: 30_000,
      },
    ]);
    await nextPoke(client);
    await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
    await expectDesiredDel(client, 'foo', EARLIER_EXPIRY);

    initial.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      // Not hydrated: the budget is enabled, so it was never a candidate.
      expect(await rowsReferencing(initial, EARLIER_EXPIRY)).toEqual([]);
      // Not dropped either: the record survives, still not gotten, with the
      // client's inactivation and its 30s TTL intact.
      expect(await liveQueries(initial)).toEqual([EARLIER_EXPIRY]);
      expect(await persistedQueryState(initial, EARLIER_EXPIRY)).toEqual({
        present: true,
        gotten: false,
        desires: [{inactivated: true, ttlMs: 30_000}],
      });
    });
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
  const initial = await setupHarness(
    testDBs,
    'vs_hydration_budget_one_batch',
    150,
    'custom',
  );
  try {
    const foo = initial.connect(SYNC_CONTEXT, desiredQueries('custom'));
    await nextPoke(foo);
    initial.stateChanges.push({state: 'version-ready'});
    await nextPoke(foo);

    await inactivateQuery(initial.vs, SYNC_CONTEXT, LATER_EXPIRY);
    await expectDesiredDel(foo, 'foo', LATER_EXPIRY);
    await inactivateQuery(initial.vs, SYNC_CONTEXT, EARLIER_EXPIRY);
    await expectDesiredDel(foo, 'foo', EARLIER_EXPIRY);

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
      [ACTIVE, EARLIER_EXPIRY, LATER_EXPIRY],
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

test<PgTest>('an optional custom query that fails to transform is removed, not evicted', async ({
  testDBs,
}) => {
  // A per-query transform error and a budget eviction both end in a query
  // removal, and both can land in the same pass. The errored query must be
  // removed even though the budget still has time for it.
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_transform_error',
    150,
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    // LATER_EXPIRY (the first optional query) now fails to transform.
    initial.queryFetch.respond([
      {id: ACTIVE, name: 'named-active', ast: ISSUES_QUERY},
      {
        error: 'app',
        id: LATER_EXPIRY,
        name: 'named-later',
        message: 'not authorized',
      },
      {id: EARLIER_EXPIRY, name: 'named-earlier', ast: USERS_QUERY},
    ]);

    restarted = restartWithClock(initial, NEVER_EXHAUSTED);
    const client = restarted.connect(RESTARTED_CONTEXT, []);
    restarted.stateChanges.push({state: 'version-ready'});
    await nextPokeParts(client);

    await vi.waitFor(async () => {
      // The errored query is gone; the other optional query still hydrates
      // because the budget was never spent.
      expect(await liveQueries(initial)).toEqual([ACTIVE, EARLIER_EXPIRY]);
    });
    expect(await rowsReferencing(initial, LATER_EXPIRY)).toEqual([]);
    expect((await rowsReferencing(initial, ACTIVE)).length).toBeGreaterThan(0);
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('legacy client queries still follow the budget', async ({
  testDBs,
}) => {
  // Plain-AST client queries are deprecated in favour of custom queries, but
  // they must keep working while they exist.
  const initial = await seedInactiveQueries(
    testDBs,
    'vs_hydration_budget_legacy',
    150,
    'legacy',
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
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
