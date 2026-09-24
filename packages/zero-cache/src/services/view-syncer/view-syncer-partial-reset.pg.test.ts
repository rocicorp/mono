import {LogContext} from '@rocicorp/logger';
import {afterEach, expect, vi} from 'vitest';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {
  PokeEndBody,
  PokePartBody,
} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {UpQueriesPatch} from '../../../../zero-protocol/src/queries-patch.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Stream} from '../../../../zql/src/ivm/stream.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import type {HashedTransformResponse} from '../../custom-queries/transform-query.ts';
import {
  getOrCreateCounter,
  getOrCreateValueHistogram,
} from '../../observability/metrics.ts';
import {type PgTest, test} from '../../test/db.ts';
import type {ConnectionValidation} from './connection-context-manager.ts';
import {CVRStore} from './cvr-store.ts';
import {PipelineDriver} from './pipeline-driver.ts';
import {
  COMMENTS_QUERY,
  ISSUES_QUERY_WITH_OWNER,
  messages,
  nextPoke,
  permissionsAll,
  serviceID,
  setup,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {type SyncContext, TimeSliceTimer} from './view-syncer.ts';

/**
 * View-syncer tests for partial pipeline resets
 * (designs/003_per_pipeline_reset.md): a pipeline that goes over its own
 * advancement budget is dropped during the advancement and rebuilt at the new
 * version, in the same poke as the advancement of the other queries.
 *
 * Processing time is simulated with a fake clock, `now`, which advances only
 * when a pipeline fetches a row from a table that the current phase makes
 * expensive (see `costs`). The view-syncer's timers read it through
 * `TimeSliceTimer.totalElapsed`, so the time of a fetch is charged to the
 * pipeline the driver is working on at that moment.
 *
 * - `EXPENSIVE` (issues with their owners) is hydrated for free, so its
 *   advancement budget is the 50 ms minimum. Renaming user '101' fetches the
 *   three issues that '101' owns, which the advancement phase makes cost
 *   30 ms each, so it is dropped before the third.
 * - `CHEAP` (comments) costs 1000 ms per row to hydrate, which gives the
 *   client group a large budget.
 */
const EXPENSIVE = 'expensive';
const CHEAP = 'cheap';

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

const DESIRED: UpQueriesPatch = [
  {op: 'put', hash: EXPENSIVE, ast: ISSUES_QUERY_WITH_OWNER},
  {op: 'put', hash: CHEAP, ast: COMMENTS_QUERY},
];

type Phase = 'hydrate' | 'advance' | 'rebuild';
type Costs = Readonly<Record<Phase, Readonly<Record<string, number>>>>;

const DEFAULT_COSTS: Costs = {
  hydrate: {comments: 1000},
  advance: {issues: 30},
  rebuild: {},
};

afterEach(() => {
  vi.restoreAllMocks();
});

type Harness = Awaited<ReturnType<typeof setup>>;
type Client = ReturnType<Harness['connect']>;

type Clock = {
  phase: Phase;
  costs: Costs;
};

/**
 * Installs the fake clock (see the file comment). Must be called before any
 * pipeline is built. Detects the rebuild of dropped pipelines, and records
 * the query IDs that are rebuilt.
 */
function fakeClock(costs: Costs = DEFAULT_COSTS): Clock & {
  rebuilt: string[];
} {
  const clock = {phase: 'hydrate' as Phase, costs, rebuilt: [] as string[]};
  let now = 0;

  const starts = new WeakMap<TimeSliceTimer, number>();
  const startWithoutYielding = TimeSliceTimer.prototype.startWithoutYielding;
  vi.spyOn(TimeSliceTimer.prototype, 'startWithoutYielding').mockImplementation(
    function (this: TimeSliceTimer) {
      starts.set(this, now);
      return startWithoutYielding.call(this);
    },
  );
  vi.spyOn(TimeSliceTimer.prototype, 'totalElapsed').mockImplementation(
    function (this: TimeSliceTimer) {
      return now - (starts.get(this) ?? now);
    },
  );

  function* charged(
    table: string,
    nodes: Stream<Node | 'yield'>,
  ): Stream<Node | 'yield'> {
    for (const node of nodes) {
      if (node !== 'yield') {
        now += clock.costs[clock.phase][table] ?? 0;
      }
      yield node;
    }
  }
  const connect = TableSource.prototype.connect;
  vi.spyOn(TableSource.prototype, 'connect').mockImplementation(function (
    this: TableSource,
    ...args: Parameters<TableSource['connect']>
  ) {
    const input = connect.apply(this, args);
    const {tableName} = input.getSchema();
    const fetch = input.fetch;
    vi.spyOn(input, 'fetch').mockImplementation(req =>
      charged(tableName, fetch(req)),
    );
    return input;
  });

  const addQuery = PipelineDriver.prototype.addQuery;
  vi.spyOn(PipelineDriver.prototype, 'addQuery').mockImplementation(function (
    this: PipelineDriver,
    ...args: Parameters<PipelineDriver['addQuery']>
  ) {
    if (args[5] === 'advancement-reset') {
      clock.phase = 'rebuild';
      clock.rebuilt.push(args[1]);
    }
    return addQuery.apply(this, args);
  });
  return clock;
}

/** Whether `messages` ends inside a poke that has not been ended yet. */
function inOpenPoke(messages: Downstream[]): boolean {
  const starts = messages.filter(([type]) => type === 'pokeStart').length;
  const ends = messages.filter(([type]) => type === 'pokeEnd').length;
  return starts > ends;
}

/**
 * Dequeues messages until `done` holds and the current poke, if any, has
 * ended.
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

function pokeEnds(messages: Downstream[]): PokeEndBody[] {
  return messages
    .filter(msg => msg[0] === 'pokeEnd')
    .map(([, body]) => body as PokeEndBody);
}

/** Whether `messages` include a poke that ended without being cancelled. */
function appliedPoke(messages: Downstream[]): boolean {
  return pokeEnds(messages).some(end => !end.cancel);
}

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

type ClientState = Map<string, Map<string, Record<string, unknown>>>;

/**
 * Applies the row patches of the pokes in `messages` to `state`, as a client
 * does: the parts of a cancelled poke are discarded.
 */
function applyPokes(state: ClientState, messages: Downstream[]): ClientState {
  let parts: PokePartBody[] = [];
  for (const [type, body] of messages) {
    if (type === 'pokeStart') {
      parts = [];
    } else if (type === 'pokePart') {
      parts.push(body as PokePartBody);
    } else if (type === 'pokeEnd') {
      if ((body as PokeEndBody).cancel) {
        continue;
      }
      for (const part of parts) {
        for (const patch of part.rowsPatch ?? []) {
          switch (patch.op) {
            case 'put': {
              let rows = state.get(patch.tableName);
              if (!rows) {
                rows = new Map();
                state.set(patch.tableName, rows);
              }
              rows.set(String(patch.value.id), patch.value);
              break;
            }
            case 'del':
              state.get(patch.tableName)?.delete(String(patch.id.id));
              break;
            default:
              throw new Error(`unexpected patch ${JSON.stringify(patch)}`);
          }
        }
      }
    }
  }
  return state;
}

/** A readable summary of a client's rows. */
function summarize(state: ClientState): Record<string, string[]> {
  const summary: Record<string, string[]> = {};
  for (const [table, rows] of [...state].toSorted(([a], [b]) =>
    a.localeCompare(b),
  )) {
    if (rows.size === 0) {
      continue;
    }
    summary[table] = Array.from(rows.values(), row =>
      table === 'issues'
        ? `${String(row.id)}:${String(row.title)}`
        : table === 'users'
          ? `${String(row.id)}:${String(row.name)}`
          : String(row.id),
    ).toSorted();
  }
  return summary;
}

/** The state of every query at the version of the advancement. */
const FINAL_STATE = {
  comments: ['1', '2', '3'],
  issues: [
    '1:parent issue foo',
    '2:parent issue bar',
    '3:foo2',
    '4:bar',
    '5:not matched',
  ],
  users: ['100:Alice', '101:Bobby', '102:Candice'],
};

/** The comments only, as when `EXPENSIVE` is removed. */
const CHEAP_ONLY_STATE = {comments: ['1', '2', '3']};

/**
 * The advancement: an edit that `EXPENSIVE` outputs (before it is dropped),
 * the rename that drops it, and a comment for `CHEAP`.
 */
function advance(initial: Harness, clock: Clock) {
  clock.phase = 'advance';
  initial.replicator.processTransaction(
    '101',
    messages.update('issues', {
      id: '3',
      title: 'foo2',
      owner: '102',
      parent: '1',
      big: 123,
    }),
    messages.update('users', {id: '101', name: 'Bobby'}),
    messages.insert('comments', {id: '3', issueID: '1', text: 'comment 3'}),
  );
  initial.stateChanges.push({state: 'version-ready'});
}

/** Connects the client and returns its state and messages once hydrated. */
async function hydrate(initial: Harness, desired = DESIRED) {
  const client = initial.connect(SYNC_CONTEXT, desired);
  const state: ClientState = new Map();
  const messages = await nextPoke(client); // desired queries
  initial.stateChanges.push({state: 'version-ready'});
  messages.push(
    ...(await collectUntil(
      client,
      ms => gotQueriesPatch(ms).length >= desired.length,
    )),
  );
  applyPokes(state, messages);
  const cookie = pokeEnds(messages).at(-1)?.cookie;
  return {client, state, cookie};
}

/** The refCounts of the rows of the given tables in the persisted CVR. */
async function cvrRows(
  initial: Harness,
): Promise<Record<string, Record<string, unknown> | null>> {
  const rows = await initial.cvrDB<
    {table: string; rowKey: {id: string}; refCounts: Record<string, unknown>}[]
  >`
    SELECT "table", "rowKey", "refCounts"
      FROM "this_app_2/cvr".rows
     WHERE "clientGroupID" = ${serviceID}
       AND "table" IN ('issues', 'users', 'comments')
     ORDER BY "table", "rowKey"`;
  return Object.fromEntries(
    rows.map(({table, rowKey, refCounts}) => [
      `${table}/${rowKey.id}`,
      refCounts,
    ]),
  );
}

async function rowSetSignature(initial: Harness, queryHash: string) {
  const [row] = await initial.cvrDB<{rowSetSignature: string | null}[]>`
    SELECT "rowSetSignature"
      FROM "this_app_2/cvr".queries
     WHERE "clientGroupID" = ${serviceID} AND "queryHash" = ${queryHash}`;
  return row?.rowSetSignature;
}

async function cvrVersion(initial: Harness): Promise<string> {
  const [{version}] = await initial.cvrDB<{version: string}[]>`
    SELECT "version" FROM "this_app_2/cvr".instances
     WHERE "clientGroupID" = ${serviceID}`;
  return version;
}

const FINAL_CVR_ROWS = {
  'comments/1': {[CHEAP]: 1},
  'comments/2': {[CHEAP]: 1},
  'comments/3': {[CHEAP]: 1},
  'issues/1': {[EXPENSIVE]: 1},
  'issues/2': {[EXPENSIVE]: 1},
  'issues/3': {[EXPENSIVE]: 1},
  'issues/4': {[EXPENSIVE]: 1},
  'issues/5': {[EXPENSIVE]: 1},
  'users/100': {[EXPENSIVE]: 1},
  'users/101': {[EXPENSIVE]: 3},
  'users/102': {[EXPENSIVE]: 1},
};

async function teardown(testDBs: PgTest['testDBs'], initial: Harness) {
  initial.clearMocks();
  await initial.vs.stop();
  await initial.viewSyncerDone;
  await testDBs.drop(initial.cvrDB, initial.upstreamDb);
  initial.replicaDbFile.delete();
}

const PARTIAL_RESET_REASONS = new Set([
  'slow-change',
  'projected-overrun',
  'timeout',
]);

/**
 * The `add` calls of the partial reset counter. In tests, instruments may
 * share one no-op instance, so its calls are told apart by their attributes.
 */
function partialResetAdds(add: {mock: {calls: unknown[][]}}) {
  return add.mock.calls.filter(([, attrs]) =>
    PARTIAL_RESET_REASONS.has(
      (attrs as {reason?: string} | undefined)?.reason ?? '',
    ),
  );
}

function infoLogs(logSink: TestLogSink) {
  return logSink.messages.filter(([level]) => level === 'info');
}

test<PgTest>('the dropped query is rebuilt in the same poke as the advancement', async ({
  testDBs,
}) => {
  const logSink = new TestLogSink();
  const lc = new LogContext('info', {}, logSink);
  const clock = fakeClock();
  const initial = await setup(
    testDBs,
    'vs_partial_reset_same_poke',
    permissionsAll,
    {partialPipelineReset: true, lc},
  );
  const partialResets = vi.spyOn(
    getOrCreateCounter('sync', 'pipeline-partial-resets', ''),
    'add',
  );
  const partialResetSizes = vi.spyOn(
    getOrCreateValueHistogram('sync', 'pipeline-partial-reset-size', {
      description: '',
      bucketBoundaries: [],
    }),
    'record',
  );
  try {
    const {client, state} = await hydrate(initial);
    expect(summarize(state)).toEqual({
      ...FINAL_STATE,
      comments: ['1', '2'],
      issues: FINAL_STATE.issues.map(i => (i === '3:foo2' ? '3:foo' : i)),
      users: ['100:Alice', '101:Bob', '102:Candice'],
    });
    await vi.waitFor(async () =>
      expect(await rowSetSignature(initial, EXPENSIVE)).toBeTruthy(),
    );
    const signatureBefore = await rowSetSignature(initial, EXPENSIVE);

    advance(initial, clock);
    const poke = await nextPoke(client);

    // The rename dropped `EXPENSIVE`, which was rebuilt without a reset of
    // the group.
    expect(clock.rebuilt).toEqual([EXPENSIVE]);
    expect(
      infoLogs(logSink).filter(
        ([, , args]) =>
          typeof args[0] === 'string' &&
          args[0].startsWith('resetting pipeline'),
      ),
    ).toEqual([
      [
        'info',
        expect.objectContaining({queryHash: EXPENSIVE}),
        [
          expect.stringMatching(
            /^resetting pipeline: Advancement exceeded timeout processing current change at 1 of 2 changes after 60 ms/,
          ),
          expect.objectContaining({reason: 'slow-change'}),
        ],
      ],
    ]);

    // One poke, which is not cancelled, takes the client to the new version.
    expect(pokeEnds(poke)).toEqual([
      expect.not.objectContaining({cancel: true}),
    ]);
    expect(summarize(applyPokes(state, poke))).toEqual(FINAL_STATE);
    // `EXPENSIVE` emitted the edit of issue 3 before it was dropped. The
    // rebuild re-declared its rows: issue 3 was unreferenced (a del) and then
    // re-added (a put), in order.
    expect(
      pokeParts(poke)
        .flatMap(part => part.rowsPatch ?? [])
        .map(patch =>
          patch.op === 'put'
            ? `put ${patch.tableName}/${String(patch.value.id)}`
            : patch.op === 'del'
              ? `del ${patch.tableName}/${String(patch.id.id)}`
              : patch.op,
        ),
    ).toEqual([
      'put issues/3',
      'put comments/3',
      'del issues/3',
      'put users/101',
      'put issues/3',
    ]);

    await vi.waitFor(async () =>
      expect(await cvrRows(initial)).toEqual(FINAL_CVR_ROWS),
    );
    // The row set of `EXPENSIVE` did not change.
    expect(await rowSetSignature(initial, EXPENSIVE)).toBe(signatureBefore);

    expect(partialResetAdds(partialResets)).toEqual([
      [1, {reason: 'slow-change'}],
    ]);
    // Value histograms record without attributes; latency histograms (which
    // may share the no-op instance) record with them.
    expect(partialResetSizes.mock.calls.filter(c => c.length === 1)).toEqual([
      [1],
    ]);

    // The lifecycle of the rebuilt pipeline.
    const lifecycle = infoLogs(logSink)
      .map(([, context]) => context)
      .filter(
        context =>
          context?.queryHash === EXPENSIVE &&
          (context.stopReason === 'advancement-reset' ||
            context.hydrationReason === 'advancement-reset'),
      )
      .map(context => ({
        zeroEvent: context?.zeroEvent,
        stopReason: context?.stopReason,
        hydrationReason: context?.hydrationReason,
      }));
    expect(lifecycle).toEqual([
      {
        zeroEvent: 'query-pipeline-stop',
        stopReason: 'advancement-reset',
        hydrationReason: 'query-set-sync',
      },
      {
        zeroEvent: 'query-pipeline-hydrate-start',
        stopReason: undefined,
        hydrationReason: 'advancement-reset',
      },
      {
        zeroEvent: 'query-pipeline-hydrate-finish',
        stopReason: undefined,
        hydrationReason: 'advancement-reset',
      },
    ]);
  } finally {
    await teardown(testDBs, initial);
  }
});

test<PgTest>('a client behind the CVR is caught up after a partial reset', async ({
  testDBs,
}) => {
  const clock = fakeClock();
  const initial = await setup(
    testDBs,
    'vs_partial_reset_catchup',
    permissionsAll,
    {partialPipelineReset: true},
  );
  try {
    const {client, state, cookie} = await hydrate(initial);
    const behind = structuredClone(state);
    advance(initial, clock);
    applyPokes(state, await nextPoke(client));
    expect(clock.rebuilt).toEqual([EXPENSIVE]);
    expect(summarize(state)).toEqual(FINAL_STATE);
    await vi.waitFor(async () =>
      expect(await cvrRows(initial)).toEqual(FINAL_CVR_ROWS),
    );

    // The client reconnects from the version before the advancement, and is
    // caught up from the CVR.
    const reconnected = initial.connect(
      {...SYNC_CONTEXT, wsID: 'ws2', baseCookie: cookie ?? null},
      [],
    );
    const catchup = await collectUntil(reconnected, appliedPoke);
    expect(summarize(applyPokes(behind, catchup))).toEqual(FINAL_STATE);
  } finally {
    await teardown(testDBs, initial);
  }
});

test<PgTest>('a rebuild that exceeds the hydration timeout is aborted in place', async ({
  testDBs,
}) => {
  const TIMEOUT_MS = 450;
  const clock = fakeClock({...DEFAULT_COSTS, rebuild: {issues: 200}});
  const initial = await setup(
    testDBs,
    'vs_partial_reset_rebuild_timeout',
    permissionsAll,
    {partialPipelineReset: true, queryHydrationTimeoutMs: TIMEOUT_MS},
  );
  // The rebuild yields after every row, which is where the timeout is
  // checked.
  const elapsedLap = TimeSliceTimer.prototype.elapsedLap;
  vi.spyOn(TimeSliceTimer.prototype, 'elapsedLap').mockImplementation(
    function (this: TimeSliceTimer) {
      return clock.phase === 'rebuild'
        ? YIELD_THRESHOLD_MS + 1
        : elapsedLap.call(this);
    },
  );
  try {
    const {client, state} = await hydrate(initial);
    const versionBefore = await cvrVersion(initial);

    advance(initial, clock);
    const messages = await collectUntil(
      client,
      ms => appliedPoke(ms) && transformErrors(ms).length > 0,
    );

    expect(clock.rebuilt).toEqual([EXPENSIVE]);
    expect(pokeEnds(messages)).toEqual([
      expect.not.objectContaining({cancel: true}),
    ]);
    // The query is removed and errored in the poke that advances the other
    // query.
    expect(gotQueriesPatch(messages)).toEqual([{op: 'del', hash: EXPENSIVE}]);
    expect(transformErrors(messages)).toEqual([
      {
        error: 'app',
        id: EXPENSIVE,
        name: 'legacy',
        message: expect.stringContaining(`${TIMEOUT_MS}ms`),
        details: {kind: 'HydrationTimeout', timeoutMs: TIMEOUT_MS},
      },
    ]);
    // The rows that only `EXPENSIVE` held are gone.
    expect(summarize(applyPokes(state, messages))).toEqual(CHEAP_ONLY_STATE);

    await vi.waitFor(async () =>
      expect(await cvrVersion(initial)).not.toBe(versionBefore),
    );
    const rows = await cvrRows(initial);
    expect(
      Object.entries(rows).filter(
        ([, refCounts]) => refCounts !== null && EXPENSIVE in refCounts,
      ),
    ).toEqual([]);
    expect(
      Object.fromEntries(
        Object.entries(rows).filter(([, refCounts]) => refCounts !== null),
      ),
    ).toEqual({
      'comments/1': {[CHEAP]: 1},
      'comments/2': {[CHEAP]: 1},
      'comments/3': {[CHEAP]: 1},
    });
  } finally {
    await teardown(testDBs, initial);
  }
});

test<PgTest>('a dropped query whose hash differs from the CVR resets the group', async ({
  testDBs,
}) => {
  const clock = fakeClock();
  const initial = await setup(
    testDBs,
    'vs_partial_reset_hash_mismatch',
    permissionsAll,
    {partialPipelineReset: true},
  );
  const droppedQueries = PipelineDriver.prototype.droppedQueries;
  vi.spyOn(PipelineDriver.prototype, 'droppedQueries').mockImplementation(
    function (this: PipelineDriver) {
      return new Map(
        Array.from(droppedQueries.call(this), ([id, q]) => [
          id,
          {...q, transformationHash: 'not-the-cvr-hash'},
        ]),
      );
    },
  );
  const discardPending = vi.spyOn(CVRStore.prototype, 'discardPending');
  try {
    const {client, state} = await hydrate(initial);
    discardPending.mockClear();

    advance(initial, clock);
    const messages = await collectUntil(client, appliedPoke);

    // The advancement's poke is cancelled, its writes discarded, and the
    // group is reset and hydrated again instead of rebuilding the query.
    expect(pokeEnds(messages)).toEqual([
      expect.objectContaining({cancel: true}),
      expect.not.objectContaining({cancel: true}),
    ]);
    expect(clock.rebuilt).toEqual([]);
    expect(discardPending).toHaveBeenCalled();
    expect(summarize(applyPokes(state, messages))).toEqual(FINAL_STATE);
    await vi.waitFor(async () =>
      expect(await cvrRows(initial)).toEqual(FINAL_CVR_ROWS),
    );
  } finally {
    await teardown(testDBs, initial);
  }
});

test<PgTest>('without partial resets, the query is not dropped', async ({
  testDBs,
}) => {
  const clock = fakeClock();
  const initial = await setup(
    testDBs,
    'vs_partial_reset_disabled',
    permissionsAll,
  );
  try {
    const {client, state} = await hydrate(initial);
    advance(initial, clock);
    const poke = await nextPoke(client);

    // The group's budget covers the advancement, so nothing is reset.
    expect(clock.rebuilt).toEqual([]);
    expect(pokeEnds(poke)).toEqual([
      expect.not.objectContaining({cancel: true}),
    ]);
    expect(summarize(applyPokes(state, poke))).toEqual(FINAL_STATE);
    await vi.waitFor(async () =>
      expect(await cvrRows(initial)).toEqual(FINAL_CVR_ROWS),
    );
  } finally {
    await teardown(testDBs, initial);
  }
});

const clientFallback: ConnectionValidation = {kind: 'client-fallback'};

function transformAttempt(
  result: HashedTransformResponse['result'],
): HashedTransformResponse {
  return Array.isArray(result)
    ? {kind: 'success', result, cached: false, validation: clientFallback}
    : {kind: 'failed', result};
}

test<PgTest>('escalating to a group reset reuses the dropped query without transforming it', async ({
  testDBs,
}) => {
  // `EXPENSIVE` takes 400 ms to hydrate and `CHEAP` 200 ms, so dropping
  // `EXPENSIVE` would rebuild most of the group. It is dropped when it goes
  // over half of its budget, before the group goes over half of its own.
  const clock = fakeClock({
    hydrate: {issues: 80, comments: 100},
    advance: {issues: 120},
    rebuild: {},
  });
  const initial = await setup(
    testDBs,
    'vs_partial_reset_escalation',
    permissionsAll,
    {partialPipelineReset: true, queryFetchMode: 'empty-validation'},
  );
  const transform = vi
    .spyOn(must(initial.customQueryTransformer), 'transform')
    .mockResolvedValue(
      transformAttempt([
        {
          id: EXPENSIVE,
          transformedAst: ISSUES_QUERY_WITH_OWNER,
          transformationHash: 'expensive-hash',
        },
        {
          id: CHEAP,
          transformedAst: COMMENTS_QUERY,
          transformationHash: 'cheap-hash',
        },
      ]),
    );
  const discardPending = vi.spyOn(CVRStore.prototype, 'discardPending');
  const resets = vi.spyOn(
    getOrCreateCounter('sync', 'pipeline-resets', ''),
    'add',
  );
  const partialResets = vi.spyOn(
    getOrCreateCounter('sync', 'pipeline-partial-resets', ''),
    'add',
  );
  const droppedQueries = vi.spyOn(PipelineDriver.prototype, 'droppedQueries');
  try {
    const {client, state} = await hydrate(initial, [
      {op: 'put', hash: EXPENSIVE, name: 'expensive', args: []},
      {op: 'put', hash: CHEAP, name: 'cheap', args: []},
    ]);
    expect(transform).toHaveBeenCalledTimes(1);
    discardPending.mockClear();

    advance(initial, clock);
    const messages = await collectUntil(client, appliedPoke);

    // The advancement's poke was cancelled before any of its patches were
    // sent, so the client sees only the poke of the reset's hydration.
    expect(pokeEnds(messages)).toEqual([
      expect.not.objectContaining({cancel: true}),
    ]);
    expect(discardPending).toHaveBeenCalled();
    expect(
      resets.mock.calls.filter(
        ([, attrs]) =>
          (attrs as {reason?: string} | undefined)?.reason ===
          'advancement-timeout',
      ),
    ).toEqual([[1, {reason: 'advancement-timeout'}]]);
    expect(partialResetAdds(partialResets)).toEqual([]);
    // The dropped query was reported for reuse, and not rebuilt in place.
    expect(
      droppedQueries.mock.results.some(
        r => r.type === 'return' && r.value.has(EXPENSIVE),
      ),
    ).toBe(true);
    expect(clock.rebuilt).toEqual([]);
    // Neither query was transformed again.
    expect(transform).toHaveBeenCalledTimes(1);
    expect(summarize(applyPokes(state, messages))).toEqual(FINAL_STATE);
  } finally {
    await teardown(testDBs, initial);
  }
});

function must<T>(value: T | undefined): T {
  expect(value).toBeDefined();
  return value as T;
}
