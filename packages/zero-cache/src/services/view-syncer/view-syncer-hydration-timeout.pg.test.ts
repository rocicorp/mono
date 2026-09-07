import {afterEach, expect, vi} from 'vitest';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {PokePartBody} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {MonotonicClock} from './hydration-budget.ts';
import {DEFAULT_CIRCUIT_BREAKER_OPEN_MS} from './hydration-circuit-breaker.ts';
import {
  addQuery,
  ALL_ISSUES_QUERY,
  ISSUES_QUERY,
  nextPoke,
  permissionsAll,
  restartViewSyncer,
  serviceID,
  setup,
  USERS_QUERY,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {type SyncContext, TimeSliceTimer} from './view-syncer.ts';

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

/**
 * Queries used by the tests in this file:
 *
 * - `FAST` matches issues '1' through '4'.
 * - `SLOW` matches all five issues, so it shares '1' through '4' with `FAST`
 *   and holds '5' exclusively.
 * - `FAST2` matches the three users.
 *
 * Hydration is made to yield after every row, and each row is made to cost
 * {@link SLOW_ROW_MS} of processing time, so with a timeout of
 * {@link TIMEOUT_MS} only the five-row `SLOW` query trips the breaker.
 */
const FAST = 'fast';
const SLOW = 'slow';
const FAST2 = 'fast2';

const TIMEOUT_MS = 450;
const SLOW_ROW_MS = 100;
const FAST_ROW_MS = 50;

const DESIRED: UpQueriesPatch = [
  {op: 'put', hash: FAST, ast: ISSUES_QUERY},
  {op: 'put', hash: SLOW, ast: ALL_ISSUES_QUERY},
  {op: 'put', hash: FAST2, ast: USERS_QUERY},
];

afterEach(() => {
  vi.restoreAllMocks();
});

/** A settable clock, used to expire the breaker's cooldown. */
function settableClock(): MonotonicClock & {set(ms: number): void} {
  let now = 0;
  const clock = () => now;
  clock.set = (ms: number) => {
    now = ms;
  };
  return clock;
}

/**
 * Makes every hydrated row yield and cost `rowMs` of processing time, as
 * measured by the timer the view-syncer consults at each yield.
 *
 * Returns the number of rows hydrated so far, for asserting that a rejected
 * query was not hydrated at all.
 */
function slowRows(rowMsRef: {rowMs: number}): () => number {
  let rowsThisQuery = 0;
  let totalRows = 0;
  const start = TimeSliceTimer.prototype.startWithoutYielding;
  vi.spyOn(TimeSliceTimer.prototype, 'startWithoutYielding').mockImplementation(
    function (this: TimeSliceTimer) {
      rowsThisQuery = 0;
      return start.call(this);
    },
  );
  vi.spyOn(TimeSliceTimer.prototype, 'elapsedLap').mockImplementation(() => {
    rowsThisQuery++;
    totalRows++;
    return YIELD_THRESHOLD_MS + 1;
  });
  vi.spyOn(TimeSliceTimer.prototype, 'totalElapsed').mockImplementation(
    () => rowsThisQuery * rowMsRef.rowMs,
  );
  return () => totalRows;
}

type Harness = Awaited<ReturnType<typeof setup>>;

/** Query hashes that are not deleted in the persisted CVR. */
async function liveQueries(initial: Harness): Promise<string[]> {
  const rows = await initial.cvrDB<{queryHash: string}[]>`
    SELECT "queryHash"
      FROM "this_app_2/cvr".queries
     WHERE "clientGroupID" = ${serviceID}
       AND deleted = false
       AND "queryHash" IN (${FAST}, ${SLOW}, ${FAST2})
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

/** Row keys in the persisted CVR that are still referenced by any query. */
async function liveRowKeys(initial: Harness): Promise<unknown[]> {
  const rows = await initial.cvrDB<{rowKey: unknown}[]>`
    SELECT "rowKey"
      FROM "this_app_2/cvr".rows
     WHERE "clientGroupID" = ${serviceID}
       AND "table" = 'issues'
       AND "refCounts" IS NOT NULL
     ORDER BY "rowKey"`;
  return rows.map(({rowKey}) => rowKey);
}

type Client = ReturnType<Harness['connect']>;

/** Whether `messages` ends inside a poke that has not been ended yet. */
function inOpenPoke(messages: Downstream[]): boolean {
  const starts = messages.filter(([type]) => type === 'pokeStart').length;
  const ends = messages.filter(([type]) => type === 'pokeEnd').length;
  return starts > ends;
}

/**
 * Dequeues messages until `done` holds and the current poke, if any, has
 * ended. A hydration timeout sends its `transformError` directly to the
 * client, so it can land on either side of a poke's end, and a rejected query
 * can span two pokes (the desired-queries poke and the removal poke).
 */
async function collectUntil(
  client: Client,
  done: (messages: Downstream[]) => boolean,
  timeoutMs = 5000,
): Promise<Downstream[]> {
  const messages: Downstream[] = [];
  const deadline = Date.now() + timeoutMs;
  const timedOut = 'nothing' as unknown as Downstream;
  while (!done(messages) || inOpenPoke(messages)) {
    const remaining = deadline - Date.now();
    const msg =
      remaining > 0 ? await client.dequeue(timedOut, remaining) : timedOut;
    if (msg === timedOut) {
      throw new Error(
        `Timed out waiting for messages. Received: ${JSON.stringify(messages)}`,
      );
    }
    messages.push(msg);
  }
  return messages;
}

function hasGot(op: 'put' | 'del', hash: string) {
  return (messages: Downstream[]) =>
    gotQueriesPatch(messages).some(
      p => p.op !== 'clear' && p.op === op && p.hash === hash,
    );
}

function hasTransformError(messages: Downstream[]): boolean {
  return transformErrors(messages).length > 0;
}

const slowEvicted = (messages: Downstream[]) =>
  hasGot('del', SLOW)(messages) && hasTransformError(messages);

function pokeParts(messages: Downstream[]): PokePartBody[] {
  return messages
    .filter(msg => msg[0] === 'pokePart')
    .map(([, body]) => body as PokePartBody);
}

function gotQueriesPatch(messages: Downstream[]) {
  return pokeParts(messages).flatMap(part => part.gotQueriesPatch ?? []);
}

function transformErrors(messages: Downstream[]) {
  return messages
    .filter(msg => msg[0] === 'transformError')
    .flatMap(([, errors]) => errors);
}

/** The issue ids the client holds after applying every row patch in order. */
function issueIDsAfter(messages: Downstream[]): string[] {
  const ids = new Set<string>();
  for (const part of pokeParts(messages)) {
    for (const patch of part.rowsPatch ?? []) {
      if (patch.op === 'put' && patch.tableName === 'issues') {
        ids.add(patch.value.id as string);
      } else if (patch.op === 'del' && patch.tableName === 'issues') {
        ids.delete(patch.id.id as string);
      }
    }
  }
  return [...ids].toSorted();
}

const HYDRATION_TIMEOUT_ERROR = {
  error: 'app',
  id: SLOW,
  name: 'legacy',
  message: expect.stringContaining(`${TIMEOUT_MS}ms`),
  details: {kind: 'HydrationTimeout', timeoutMs: TIMEOUT_MS},
};

test<PgTest>('aborts a slow hydration, evicts the query, and errors it to the client', async ({
  testDBs,
}) => {
  const clock = settableClock();
  const initial = await setup(
    testDBs,
    'vs_hydration_timeout_abort',
    permissionsAll,
    {queryHydrationTimeoutMs: TIMEOUT_MS, monotonicClock: clock},
  );
  const rowMs = {rowMs: SLOW_ROW_MS};
  const hydratedRows = slowRows(rowMs);
  try {
    const client = initial.connect(SYNC_CONTEXT, DESIRED);
    await nextPoke(client); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    const messages = await collectUntil(client, slowEvicted);

    // The slow query is announced as gotten before its hydration starts and
    // retracted in the same poke once it is aborted.
    const patches = gotQueriesPatch(messages);
    expect(patches).toEqual(
      expect.arrayContaining([
        {op: 'put', hash: FAST},
        {op: 'put', hash: FAST2},
        {op: 'put', hash: SLOW},
        {op: 'del', hash: SLOW},
      ]),
    );
    const indexOf = (op: 'put' | 'del') =>
      patches.findIndex(
        p => p.op !== 'clear' && p.op === op && p.hash === SLOW,
      );
    expect(indexOf('put')).toBeLessThan(indexOf('del'));
    // Issue '5' is held only by the slow query, so its put is canceled. The
    // issues it shares with the fast query survive.
    expect(issueIDsAfter(messages)).toEqual(['1', '2', '3', '4']);
    expect(transformErrors(messages)).toEqual([HYDRATION_TIMEOUT_ERROR]);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([FAST, FAST2]);
    });
    expect(await rowsReferencing(initial, SLOW)).toEqual([]);
    expect(await liveRowKeys(initial)).toEqual([
      {id: '1'},
      {id: '2'},
      {id: '3'},
      {id: '4'},
    ]);
    expect(initial.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      2,
    );

    // While the breaker is open, re-requesting the query rejects it without
    // hydrating a single row.
    const rowsBefore = hydratedRows();
    await addQuery(initial.vs, SYNC_CONTEXT, SLOW, ALL_ISSUES_QUERY);
    const rejected = await collectUntil(client, slowEvicted);
    expect(transformErrors(rejected)).toEqual([HYDRATION_TIMEOUT_ERROR]);
    expect(gotQueriesPatch(rejected)).not.toContainEqual({
      op: 'put',
      hash: SLOW,
    });
    expect(hydratedRows()).toBe(rowsBefore);
    expect(await liveQueries(initial)).toEqual([FAST, FAST2]);

    // Once the cooldown has passed the query is tried again, and this time it
    // fits within the timeout.
    clock.set(DEFAULT_CIRCUIT_BREAKER_OPEN_MS);
    rowMs.rowMs = FAST_ROW_MS;
    await addQuery(initial.vs, SYNC_CONTEXT, SLOW, ALL_ISSUES_QUERY);
    const retried = await collectUntil(client, hasGot('put', SLOW));
    expect(transformErrors(retried)).toEqual([]);
    expect(gotQueriesPatch(retried)).toEqual([{op: 'put', hash: SLOW}]);
    expect(issueIDsAfter(retried)).toEqual(['5']);
    expect(hydratedRows()).toBeGreaterThan(rowsBefore);
    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([FAST, FAST2, SLOW]);
    });
    expect(initial.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      3,
    );
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('aborts a slow rehydration of an unchanged query on restart', async ({
  testDBs,
}) => {
  const initial = await setup(
    testDBs,
    'vs_hydration_timeout_restart',
    permissionsAll,
    {queryHydrationTimeoutMs: TIMEOUT_MS},
  );
  let restarted: ReturnType<typeof restartViewSyncer> | undefined;
  try {
    // Hydrate everything at full speed first.
    const client = initial.connect(SYNC_CONTEXT, DESIRED);
    await nextPoke(client); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    const messages = await collectUntil(client, hasGot('put', SLOW));
    expect(transformErrors(messages)).toEqual([]);
    expect(issueIDsAfter(messages)).toEqual(['1', '2', '3', '4', '5']);
    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([FAST, FAST2, SLOW]);
    });
    await initial.vs.stop();
    await initial.viewSyncerDone;

    // On restart the gotten queries are rehydrated as unchanged queries. The
    // slow one is aborted there, and the following pipeline sync removes it.
    slowRows({rowMs: SLOW_ROW_MS});
    restarted = restartViewSyncer({
      databaseStorage: initial.databaseStorage,
      replicaDbFile: initial.replicaDbFile,
      cvrDB: initial.cvrDB,
      config: initial.config,
      customQueryTransformer: initial.customQueryTransformer,
      setTimeoutFn: initial.setTimeoutFn,
    });
    const reconnected = restarted.connect({...SYNC_CONTEXT, wsID: 'ws2'}, []);
    restarted.stateChanges.push({state: 'version-ready'});
    const afterRestart = await collectUntil(reconnected, slowEvicted);

    // The fresh connection is caught up with the surviving gotten queries.
    const patches = gotQueriesPatch(afterRestart);
    expect(patches).toContainEqual({op: 'del', hash: SLOW});
    expect(patches).not.toContainEqual({op: 'put', hash: SLOW});
    expect(transformErrors(afterRestart)).toEqual([HYDRATION_TIMEOUT_ERROR]);
    // The issue that only the slow query held is gone from the client.
    expect(issueIDsAfter(afterRestart)).toEqual(['1', '2', '3', '4']);

    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([FAST, FAST2]);
    });
    expect(await rowsReferencing(initial, SLOW)).toEqual([]);
    expect(await liveRowKeys(initial)).toEqual([
      {id: '1'},
      {id: '2'},
      {id: '3'},
      {id: '4'},
    ]);
    expect(restarted.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      2,
    );
  } finally {
    initial.clearMocks();
    await (restarted ?? initial).vs.stop();
    await (restarted ?? initial).viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});

test<PgTest>('a disabled timeout never aborts a hydration', async ({
  testDBs,
}) => {
  const initial = await setup(
    testDBs,
    'vs_hydration_timeout_disabled',
    permissionsAll,
  );
  slowRows({rowMs: SLOW_ROW_MS});
  try {
    const client = initial.connect(SYNC_CONTEXT, DESIRED);
    await nextPoke(client); // desired queries
    initial.stateChanges.push({state: 'version-ready'});
    const messages = await collectUntil(client, m =>
      [FAST, SLOW, FAST2].every(hash => hasGot('put', hash)(m)),
    );

    expect(transformErrors(messages)).toEqual([]);
    expect(gotQueriesPatch(messages)).not.toContainEqual({
      op: 'del',
      hash: SLOW,
    });
    expect(issueIDsAfter(messages)).toEqual(['1', '2', '3', '4', '5']);
    await vi.waitFor(async () => {
      expect(await liveQueries(initial)).toEqual([FAST, FAST2, SLOW]);
    });
    expect(initial.vs.pipelineHashes().filter(q => !q.internal)).toHaveLength(
      3,
    );
  } finally {
    initial.clearMocks();
    await initial.vs.stop();
    await initial.viewSyncerDone;
    await testDBs.drop(initial.cvrDB, initial.upstreamDb);
    initial.replicaDbFile.delete();
  }
});
