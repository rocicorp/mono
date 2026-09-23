import {LogContext} from '@rocicorp/logger';
import {afterAll, afterEach, expect, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {Queue} from '../../../../shared/src/queue.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import type {Downstream} from '../../../../zero-protocol/src/down.ts';
import type {PokePartBody} from '../../../../zero-protocol/src/poke.ts';
import {PROTOCOL_VERSION} from '../../../../zero-protocol/src/protocol-version.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Stream} from '../../../../zql/src/ivm/stream.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {type PgTest, test} from '../../test/db.ts';
import {versionToLexi} from '../../types/lexi-version.ts';
import {ReplicationMessages} from '../replicator/test-utils.ts';
import {CVRQueryDrivenUpdater} from './cvr.ts';
import {PipelineDriver, type Timer} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';
import {
  ALL_ISSUES_QUERY,
  COMMENTS_QUERY,
  defaultClientSchema,
  ISSUES_QUERY_WITH_OWNER,
  messages,
  permissionsAll,
  serviceID,
  setup,
  SHARD,
  USERS_QUERY,
  YIELD_THRESHOLD_MS,
} from './view-syncer-test-util.ts';
import {type SyncContext, TimeSliceTimer} from './view-syncer.ts';

/**
 * Randomized test for partial pipeline resets
 * (designs/003_per_pipeline_reset.md).
 *
 * Random transactions are applied while fetching rows from each table costs
 * a random amount of (simulated) processing time, so that pipelines are
 * dropped and rebuilt at random points of advancements, and client groups
 * are sometimes reset as a whole. After each advancement, the state the
 * client applied and the refCounts in the CVR must equal a full hydration of
 * every query at the new version.
 */

// FUZZ_SEEDS=<n> runs seeds 1..n, and FUZZ_ITERATIONS sets the number of
// transactions per seed, for longer runs.
const SEEDS = Array.from(
  {length: Number(process.env.FUZZ_SEEDS ?? 3)},
  (_, i) => i + 1,
);
const ITERATIONS = Number(process.env.FUZZ_ITERATIONS ?? 40);

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

const QUERIES: Record<string, AST> = {
  allIssues: ALL_ISSUES_QUERY,
  issuesWithOwner: ISSUES_QUERY_WITH_OWNER,
  comments: COMMENTS_QUERY,
  users: USERS_QUERY,
  usersWithIssues: {
    table: 'users',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['owner']},
        subquery: {table: 'issues', alias: 'issues', orderBy: [['id', 'asc']]},
      },
    ],
  },
  issuesWithComments: {
    table: 'issues',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  },
  firstIssuesByTitle: {
    table: 'issues',
    orderBy: [
      ['title', 'asc'],
      ['id', 'asc'],
    ],
    limit: 2,
  },
  // Every transaction renames label '1', so that every advancement pokes.
  labels: {table: 'labels', orderBy: [['id', 'asc']]},
  commentedIssues: {
    table: 'issues',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
    },
  },
};

/**
 * The rows of an EXISTS subquery are limited (by a Cap) to the first few
 * that the pipeline saw, which depends on its history. So these references
 * are not compared with a fresh hydration.
 */
function isCapped(queryID: string, table: string) {
  return queryID === 'commentedIssues' && table === 'comments';
}

const TABLES = ['issues', 'users', 'comments'] as const;
const labelMessages = new ReplicationMessages({labels: 'id'});
const PRIMARY_KEYS: Record<string, string[]> = {
  issues: ['id'],
  users: ['id'],
  comments: ['id'],
  issueLabels: ['issueID', 'labelID'],
  labels: ['id'],
};

afterEach(() => {
  vi.restoreAllMocks();
});

const totals = {drops: 0, groupResets: 0};
afterAll(() => {
  // The seeds must exercise both partial and whole-group resets.
  expect(totals.drops).toBeGreaterThan(0);
  expect(totals.groupResets).toBeGreaterThan(0);
});

/** mulberry32 */
function random(seed: number): () => number {
  let a = seed;
  return () => {
    a |= 0;
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

function rowKeyString(table: string, rowKey: Record<string, unknown>) {
  return `${table}:${JSON.stringify(
    Object.entries(rowKey).sort(([a], [b]) => a.localeCompare(b)),
  )}`;
}

function rowKeyOf(table: string, row: Record<string, unknown>) {
  return Object.fromEntries(PRIMARY_KEYS[table].map(col => [col, row[col]]));
}

for (const seed of SEEDS) {
  test<PgTest>(`random partial resets (seed ${seed})`, async ({testDBs}) => {
    const rand = random(seed);
    const pick = <T>(values: readonly T[]): T =>
      values[Math.floor(rand() * values.length)];

    // Simulated processing time.
    let now = 0;
    const rowCosts = new Map<string, number>();
    let consumerCost = 0;
    let yieldChance = 0;
    const starts = new WeakMap<TimeSliceTimer, number>();
    const start = TimeSliceTimer.prototype.startWithoutYielding;
    vi.spyOn(
      TimeSliceTimer.prototype,
      'startWithoutYielding',
    ).mockImplementation(function (this: TimeSliceTimer) {
      starts.set(this, now);
      return start.call(this);
    });
    vi.spyOn(TimeSliceTimer.prototype, 'totalElapsed').mockImplementation(
      function (this: TimeSliceTimer) {
        return now - (starts.get(this) ?? now);
      },
    );
    vi.spyOn(TimeSliceTimer.prototype, 'elapsedLap').mockImplementation(() =>
      rand() < yieldChance ? YIELD_THRESHOLD_MS + 1 : 0,
    );
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
    function* charged(
      table: string,
      nodes: Stream<Node | 'yield'>,
    ): Stream<Node | 'yield'> {
      for (const node of nodes) {
        if (node !== 'yield') {
          now += rowCosts.get(table) ?? 0;
        }
        yield node;
      }
    }
    // The consumer's time is part of the group's budget (hydrationTimeMs),
    // but not of the pipelines' (their IVM hydration time).
    const received = CVRQueryDrivenUpdater.prototype.received;
    vi.spyOn(CVRQueryDrivenUpdater.prototype, 'received').mockImplementation(
      function (this: CVRQueryDrivenUpdater, ...args) {
        now += consumerCost;
        return received.apply(this, args);
      },
    );
    let driver: PipelineDriver | undefined;
    const init = PipelineDriver.prototype.init;
    vi.spyOn(PipelineDriver.prototype, 'init').mockImplementation(function (
      this: PipelineDriver,
      ...args
    ) {
      driver ??= this;
      return init.apply(this, args);
    });

    function randomizeCosts() {
      for (const table of TABLES) {
        rowCosts.set(table, pick([0, 0, 1, 5, 20, 40, 80, 500]));
      }
      consumerCost = pick([0, 10, 200]);
      yieldChance = pick([0, 0, 0.2]);
    }

    const logSink = new TestLogSink();
    const lc = new LogContext('info', {}, logSink);
    const harness = await setup(
      testDBs,
      `vs_partial_reset_fuzz_${seed}`,
      permissionsAll,
      {partialPipelineReset: process.env.PARTIAL_OFF !== '1', lc},
    );
    const {vs, viewSyncerDone, replicator, stateChanges, cvrDB, replicaDbFile} =
      harness;

    try {
      // The replica's rows, as a model for generating valid writes.
      const model = {
        issues: new Map<string, {id: string; title: string; owner: string}>(
          [
            ['1', 'parent issue foo', '100'],
            ['2', 'parent issue bar', '101'],
            ['3', 'foo', '102'],
            ['4', 'bar', '101'],
            ['5', 'not matched', '101'],
          ].map(([id, title, owner]) => [id, {id, title, owner}]),
        ),
        users: new Map<string, {id: string; name: string}>(
          [
            ['100', 'Alice'],
            ['101', 'Bob'],
            ['102', 'Candice'],
          ].map(([id, name]) => [id, {id, name}]),
        ),
        comments: new Map<string, {id: string; issueID: string; text: string}>(
          [
            ['1', '1', 'comment 1'],
            ['2', '1', 'bar'],
          ].map(([id, issueID, text]) => [id, {id, issueID, text}]),
        ),
      };
      const issueIDs = ['1', '2', '3', '4', '5', '6', '7', '8'];
      const userIDs = ['100', '101', '102', '103', '104'];
      const commentIDs = ['1', '2', '3', '4', '5', '6', '7', '8'];
      const words = ['a', 'b', 'c', 'd', 'e'];

      function randomWrite() {
        const table = pick(TABLES);
        switch (table) {
          case 'issues': {
            const id = pick(issueIDs);
            if (model.issues.has(id) && rand() < 0.3) {
              model.issues.delete(id);
              return messages.delete('issues', {id});
            }
            const row = {id, title: pick(words), owner: pick(userIDs)};
            const exists = model.issues.has(id);
            model.issues.set(id, row);
            return exists
              ? messages.update('issues', row)
              : messages.insert('issues', row);
          }
          case 'users': {
            const id = pick(userIDs);
            if (model.users.has(id) && rand() < 0.3) {
              model.users.delete(id);
              return messages.delete('users', {id});
            }
            const row = {id, name: pick(words)};
            const exists = model.users.has(id);
            model.users.set(id, row);
            return exists
              ? messages.update('users', row)
              : messages.insert('users', row);
          }
          case 'comments': {
            const id = pick(commentIDs);
            if (model.comments.has(id) && rand() < 0.3) {
              model.comments.delete(id);
              return messages.delete('comments', {id});
            }
            const row = {id, issueID: pick(issueIDs), text: pick(words)};
            const exists = model.comments.has(id);
            model.comments.set(id, row);
            return exists
              ? messages.update('comments', row)
              : messages.insert('comments', row);
          }
        }
      }

      // The client's rows, from the pokes it applied.
      const clientRows = new Map<string, Record<string, unknown>>();
      let pokes = 0;

      /** Applies pokes until the client is at `version` or later. */
      async function catchUp(client: Queue<Downstream>, version: string) {
        let parts: PokePartBody[] = [];
        for (;;) {
          const msg = await client.dequeue();
          switch (msg[0]) {
            case 'pokeStart':
              parts = [];
              break;
            case 'pokePart':
              parts.push(msg[1]);
              break;
            case 'pokeEnd': {
              if (msg[1].cancel) {
                break;
              }
              pokes++;
              for (const part of parts) {
                for (const patch of part.rowsPatch ?? []) {
                  if (patch.op === 'clear') {
                    throw new Error('unexpected clear');
                  }
                  if (!(patch.tableName in PRIMARY_KEYS)) {
                    continue; // internal tables
                  }
                  if (patch.op === 'put') {
                    const key = rowKeyString(
                      patch.tableName,
                      rowKeyOf(patch.tableName, patch.value),
                    );
                    clientRows.set(key, patch.value);
                  } else if (patch.op === 'del') {
                    clientRows.delete(rowKeyString(patch.tableName, patch.id));
                  } else {
                    throw new Error(
                      `unexpected patch ${JSON.stringify(patch)}`,
                    );
                  }
                }
              }
              if (process.env.FUZZ_DEBUG) {
                // oxlint-disable-next-line no-console
                console.log('pokeEnd', msg[1].cookie, 'waiting for', version);
              }
              if (msg[1].cookie.split(':')[0] >= version) {
                return;
              }
              break;
            }
          }
        }
      }

      /**
       * The rows and refCounts that a full hydration of every query at the
       * replica's head produces.
       */
      function hydrateAtHead() {
        const storage = new Database(lc, ':memory:');
        storage.prepare(CREATE_STORAGE_TABLE).run();
        const shadow = new PipelineDriver(
          lc.withContext('component', 'shadow'),
          testLogConfig,
          new Snapshotter(lc, replicaDbFile.path, SHARD),
          SHARD,
          new DatabaseStorage(storage).createClientGroupStorage('shadow'),
          'shadow',
          new InspectorDelegate(undefined),
          () => YIELD_THRESHOLD_MS,
        );
        shadow.init(defaultClientSchema);
        const timer: Timer = {totalElapsed: () => 0, elapsedLap: () => 0};
        const rows = new Map<string, Record<string, unknown>>();
        const refCounts = new Map<string, Record<string, number>>();
        try {
          for (const [queryID, query] of must(driver).queries()) {
            if (!(queryID in QUERIES)) {
              continue; // internal queries
            }
            for (const change of shadow.addQuery(
              query.transformationHash,
              queryID,
              query.originalAst ?? query.transformedAst,
              timer,
            )) {
              if (change === 'yield') {
                continue;
              }
              const key = rowKeyString(change.table, change.rowKey);
              const {_0_version: _, ...contents} = change.row;
              rows.set(key, contents);
              if (isCapped(queryID, change.table)) {
                continue;
              }
              const counts = refCounts.get(key) ?? {};
              counts[queryID] = (counts[queryID] ?? 0) + 1;
              refCounts.set(key, counts);
            }
          }
        } finally {
          shadow.destroy();
        }
        return {rows, refCounts};
      }

      async function cvrRefCounts() {
        const rows = await cvrDB<
          {
            table: string;
            rowKey: Record<string, unknown>;
            refCounts: Record<string, number>;
          }[]
        >`SELECT "table", "rowKey", "refCounts"
            FROM "this_app_2/cvr".rows
           WHERE "clientGroupID" = ${serviceID}
             AND "refCounts" IS NOT NULL`;
        const refCounts = new Map<string, Record<string, number>>();
        for (const {table, rowKey, refCounts: counts} of rows) {
          // A query that added and removed a row that was not in the CVR,
          // in one batch, is recorded with a count of 0.
          const app = Object.fromEntries(
            Object.entries(counts).filter(
              ([id, count]) =>
                id in QUERIES && count !== 0 && !isCapped(id, table),
            ),
          );
          if (Object.keys(app).length > 0) {
            refCounts.set(rowKeyString(table, rowKey), app);
          }
        }
        return refCounts;
      }

      async function expectConsistent(description: string) {
        const expected = hydrateAtHead();
        expect(
          Object.fromEntries(clientRows),
          `client rows ${description}`,
        ).toEqual(Object.fromEntries(expected.rows));
        expect(
          Object.fromEntries(await cvrRefCounts()),
          `CVR refCounts ${description}`,
        ).toEqual(Object.fromEntries(expected.refCounts));
      }

      randomizeCosts();
      const client = harness.connect(
        SYNC_CONTEXT,
        Object.entries(QUERIES).map(([hash, ast]) => ({op: 'put', hash, ast})),
      );
      await catchUp(client, '00'); // the desired queries
      stateChanges.push({state: 'version-ready'});
      await catchUp(client, '01');
      await expectConsistent('after hydration');

      for (let i = 0; i < ITERATIONS; i++) {
        const version = versionToLexi(i + 2);
        randomizeCosts();
        const writes = Array.from({length: 1 + Math.floor(rand() * 4)}, () =>
          randomWrite(),
        );
        if (process.env.FUZZ_DEBUG) {
          // oxlint-disable-next-line no-console
          console.log('iteration', i, version, JSON.stringify(writes));
        }
        replicator.processTransaction(
          version,
          ...writes,
          labelMessages.update('labels', {id: '1', name: `bug ${version}`}),
        );
        stateChanges.push({state: 'version-ready'});
        await catchUp(client, version);
        await expectConsistent(`at ${version} (iteration ${i})`);
      }

      const logged = (prefix: string) =>
        logSink.messages.filter(
          ([level, , args]) =>
            level === 'info' &&
            typeof args[0] === 'string' &&
            args[0].startsWith(prefix),
        ).length;
      const drops = logged('resetting pipeline: ');
      const groupResets = logged('resetting pipelines: ');
      const escalations = logged('resetting pipelines: Dropped pipelines');
      if (process.env.FUZZ_DEBUG) {
        // oxlint-disable-next-line no-console
        console.log({seed, drops, groupResets, escalations, pokes});
      }
      totals.drops += drops;
      totals.groupResets += groupResets;
      expect(pokes).toBeGreaterThan(ITERATIONS);
    } finally {
      harness.clearMocks();
      await vs.stop();
      await viewSyncerDone;
      await testDBs.drop(cvrDB, harness.upstreamDb);
      replicaDbFile.delete();
    }
  });
}

function must<T>(value: T | undefined): T {
  if (value === undefined) {
    throw new Error('missing value');
  }
  return value;
}
