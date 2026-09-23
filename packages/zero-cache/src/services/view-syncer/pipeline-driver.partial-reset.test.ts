import {LogContext} from '@rocicorp/logger';
import {afterEach, beforeEach, describe, expect, test, vi} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {TestLogSink} from '../../../../shared/src/logging-test-utils.ts';
import type {AST, Condition} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import type {Node} from '../../../../zql/src/ivm/data.ts';
import type {Stream} from '../../../../zql/src/ivm/stream.ts';
import {Take} from '../../../../zql/src/ivm/take.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import type {Database as DB} from '../../../../zqlite/src/db.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {TableSource} from '../../../../zqlite/src/table-source.ts';
import type {ZeroConfig} from '../../config/zero-config.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {
  DropPipelineSignal,
  PipelineDriver,
  type RowChange,
  type Timer,
} from './pipeline-driver.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';
import {ResetPipelinesSignal, Snapshotter} from './snapshotter.ts';

/**
 * Tests for partial pipeline resets (designs/003_per_pipeline_reset.md): a
 * pipeline that goes over its own advancement budget is dropped, and the
 * other pipelines finish advancing.
 *
 * Time is simulated. The advancement timer reads `now`, which advances only
 * when a pipeline fetches rows from a table that a test makes expensive (see
 * `rowCost` and `fetchCost`). So the time of a fetch is charged to whichever
 * pipeline the driver is working on at that moment.
 */
describe('view-syncer/pipeline-driver/partial-reset', () => {
  let dbFile: DbFile;
  let db: DB;
  let lc: LogContext;
  let logSink: TestLogSink;
  let pipelines: PipelineDriver;
  let replicator: FakeReplicator;

  /** Simulated time, in ms. */
  let now: number;
  /** The time since the last yield, which decides whether to yield. */
  let lap: number;
  /** The time charged for each row fetched from a table. */
  let rowCost: (table: string) => number;
  /** The time charged when a fetch from a table starts. */
  let fetchCost: (table: string) => number;
  /** Called for each row fetched. */
  let onRow: (table: string) => void;
  /** Rows fetched, by table. */
  let fetched: Map<string, number>;

  const advanceTimer: Timer = {
    totalElapsed: () => now,
    elapsedLap: () => lap,
  };

  function newDriver(
    clientGroupID: string,
    partialPipelineReset = true,
    rowCache?: SnapshotRowCache,
  ) {
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    return new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, {appID: 'zeroz'}, undefined, rowCache),
      {appID: 'zeroz', shardNum: 1},
      new DatabaseStorage(storage).createClientGroupStorage(clientGroupID),
      clientGroupID,
      new InspectorDelegate(undefined),
      () => 200 /** yield threshold */,
      undefined,
      {partialPipelineReset} as ZeroConfig,
    );
  }

  beforeEach(() => {
    logSink = new TestLogSink();
    lc = new LogContext('info', undefined, logSink);
    dbFile = new DbFile('pipelines_partial_reset_test');
    dbFile.connect(lc).pragma('journal_mode = wal2');

    now = 0;
    lap = 0;
    rowCost = () => 0;
    fetchCost = () => 0;
    onRow = () => {};
    fetched = new Map();

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

    pipelines = newDriver('partial-reset-client-group');

    db = dbFile.connect(lc);
    initReplicationState(db, ['zero_data'], '123');
    db.exec(/*sql*/ `
      CREATE TABLE user (
        id TEXT PRIMARY KEY,
        name TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE issue (
        id TEXT PRIMARY KEY,
        creatorID TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE comment (
        id TEXT PRIMARY KEY,
        issueID TEXT,
        _0_version TEXT NOT NULL
      );
      CREATE TABLE uniques (
        id "TEXT|NOT_NULL",
        name "TEXT|NOT_NULL",
        _0_version TEXT NOT NULL
      );
      CREATE UNIQUE INDEX uniques_id ON uniques (id);
      CREATE UNIQUE INDEX uniques_name ON uniques (name);

      INSERT INTO uniques (id, name, _0_version) VALUES ('foo', 'bar', '123');
      INSERT INTO uniques (id, name, _0_version) VALUES ('boo', 'dar', '123');

      INSERT INTO user (id, name, _0_version) VALUES ('u1', 'fuzzy', '123');
      INSERT INTO user (id, name, _0_version) VALUES ('u9', 'nine', '123');

      -- 1000 issues created by u1.
      INSERT INTO issue (id, creatorID, _0_version)
        WITH RECURSIVE cnt(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 1000
        )
        SELECT 'i' || n, 'u1', '123' FROM cnt;

      -- 20 issues created by u2, who does not exist yet. The first 10 have
      -- 5 comments each.
      INSERT INTO issue (id, creatorID, _0_version)
        WITH RECURSIVE cnt(n) AS (
          SELECT 1 UNION ALL SELECT n + 1 FROM cnt WHERE n < 20
        )
        SELECT 'j' || printf('%02d', n), 'u2', '123' FROM cnt;
      INSERT INTO comment (id, issueID, _0_version)
        WITH RECURSIVE
          i(n) AS (SELECT 1 UNION ALL SELECT n + 1 FROM i WHERE n < 10),
          c(m) AS (SELECT 1 UNION ALL SELECT m + 1 FROM c WHERE m < 5)
        SELECT 'c' || printf('%02d', n) || '-' || m,
               'j' || printf('%02d', n), '123'
        FROM i, c;
    `);
    replicator = fakeReplicator(lc, db);
  });

  afterEach(() => {
    vi.restoreAllMocks();
    dbFile.delete();
  });

  function* charged(
    table: string,
    nodes: Stream<Node | 'yield'>,
  ): Stream<Node | 'yield'> {
    now += fetchCost(table);
    for (const node of nodes) {
      if (node !== 'yield') {
        now += rowCost(table);
        fetched.set(table, (fetched.get(table) ?? 0) + 1);
        onRow(table);
      }
      yield node;
    }
  }

  const user = table('user')
    .columns({id: string(), name: string()})
    .primaryKey('id');
  const issue = table('issue')
    .columns({id: string(), creatorID: string()})
    .primaryKey('id');
  const comment = table('comment')
    .columns({id: string(), issueID: string()})
    .primaryKey('id');
  const uniques = table('uniques')
    .columns({id: string(), name: string()})
    .primaryKey('id');
  const clientSchema = createSchema({
    tables: [user, issue, comment, uniques],
  });

  const messages = new ReplicationMessages({
    user: 'id',
    issue: 'id',
    comment: 'id',
    uniques: 'id',
  });

  const USERS: AST = {table: 'user', orderBy: [['id', 'asc']]};
  const ISSUES: AST = {table: 'issue', orderBy: [['id', 'asc']]};

  /** A change to a user pushes a child change for each of their issues. */
  const ISSUES_WITH_CREATOR: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator', orderBy: [['id', 'asc']]},
      },
    ],
  };

  /** A new user's issues are fetched when the user's row is streamed. */
  const USERS_WITH_ISSUES: AST = {
    table: 'user',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['creatorID']},
        subquery: {table: 'issue', alias: 'issues', orderBy: [['id', 'asc']]},
      },
    ],
  };

  /**
   * A new user adds each of their issues, fetching them one by one, and the
   * issues' comments are fetched when each issue's row is streamed.
   */
  const ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      related: {
        system: 'client',
        correlation: {parentField: ['creatorID'], childField: ['id']},
        subquery: {table: 'user', alias: 'creator', orderBy: [['id', 'asc']]},
      },
    },
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'comments',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  };

  /**
   * Like ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS, and a new user's issue is
   * only added once an EXISTS fetches its first comments (through a Cap).
   */
  const ISSUES_WITH_EXISTING_CREATOR_AND_EXISTING_COMMENTS: AST = {
    ...ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS,
    where: {
      type: 'and',
      conditions: [
        ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS.where as Condition,
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            system: 'client',
            correlation: {parentField: ['id'], childField: ['issueID']},
            subquery: {
              table: 'comment',
              alias: 'someComment',
              orderBy: [['id', 'asc']],
            },
          },
        },
      ],
    },
  };

  /** Reads `issue` through two connections. */
  const ISSUES_WITH_SIBLINGS: AST = {
    table: 'issue',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['creatorID'], childField: ['creatorID']},
        subquery: {table: 'issue', alias: 'siblings', orderBy: [['id', 'asc']]},
      },
    ],
  };

  /** Reads only `comment`, through two connections. */
  const COMMENTS_WITH_SIBLINGS: AST = {
    table: 'comment',
    orderBy: [['id', 'asc']],
    related: [
      {
        system: 'client',
        correlation: {parentField: ['issueID'], childField: ['issueID']},
        subquery: {
          table: 'comment',
          alias: 'siblings',
          orderBy: [['id', 'asc']],
        },
      },
    ],
  };

  /**
   * Resolves to `user WHERE id = <the creator of issue i1>`, with a
   * companion pipeline that watches issue i1.
   */
  const CREATOR_OF_I1_WITH_ISSUES: AST = {
    table: 'user',
    orderBy: [['id', 'asc']],
    where: {
      type: 'correlatedSubquery',
      op: 'EXISTS',
      scalar: true,
      related: {
        correlation: {parentField: ['id'], childField: ['creatorID']},
        subquery: {
          table: 'issue',
          orderBy: [['id', 'asc']],
          where: {
            type: 'simple',
            op: '=',
            left: {type: 'column', name: 'id'},
            right: {type: 'literal', value: 'i1'},
          },
        },
      },
    },
    related: USERS_WITH_ISSUES.related,
  };

  /**
   * Adds a query, with a hydration that takes `ivmMs` of the driver's time,
   * and `totalMs` in all (i.e. including the time the consumer of the
   * hydration takes). The advancement budget of the pipeline is `ivmMs`, and
   * the client group's budget is the sum of the `totalMs`.
   */
  function addQuery(
    queryID: string,
    ast: AST,
    ivmMs: number,
    totalMs = ivmMs,
    driver = pipelines,
  ): RowChange[] {
    let calls = 0;
    const timer: Timer = {
      totalElapsed: () => {
        calls++;
        return calls === 1 ? 0 : calls === 2 ? ivmMs : totalMs;
      },
      elapsedLap: () => 0,
    };
    const rows: RowChange[] = [];
    for (const change of driver.addQuery(
      `${queryID}-hash`,
      queryID,
      ast,
      timer,
    )) {
      if (change !== 'yield') {
        rows.push(change);
      }
    }
    return rows;
  }

  function advance(
    driver = pipelines,
    onChange: (change: RowChange | 'yield') => void = () => {},
  ): (RowChange | 'yield')[] {
    // Each advancement has its own timer.
    now = 0;
    fetched = new Map();
    const changes: (RowChange | 'yield')[] = [];
    for (const change of driver.advance(advanceTimer).changes) {
      changes.push(change);
      onChange(change);
    }
    return changes;
  }

  function rows(changes: (RowChange | 'yield')[], queryID?: string) {
    return changes
      .filter(c => c !== 'yield')
      .filter(c => queryID === undefined || c.queryID === queryID)
      .map(c => `${c.queryID}:${c.type}:${c.table}:${String(c.rowKey.id)}`);
  }

  function dropLogs() {
    return logSink.messages
      .filter(
        ([level, , args]) =>
          level === 'info' &&
          typeof args[0] === 'string' &&
          args[0].startsWith('resetting pipeline:'),
      )
      .map(([, context, args]) => ({context, args}));
  }

  test('drops the pipeline that goes over its budget and advances the others', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    addQuery('cheap', USERS, 1000);
    const advanceSpy = vi.spyOn(Snapshotter.prototype, 'advance');

    // The update of u1 fetches each of u1's 1000 issues in `expensive`.
    rowCost = table => (table === 'issue' ? 1 : 0);
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    const changes = advance();

    // The partial output of `expensive` is discarded.
    expect(rows(changes)).toEqual([`cheap:${ChangeType.EDIT}:user:u1`]);
    // `expensive` stopped at its budget: its first 51 rows took 51 ms.
    expect(fetched.get('issue')).toBe(51);

    expect(pipelines.droppedQueries()).toEqual(
      new Map([
        [
          'expensive',
          {
            transformationHash: 'expensive-hash',
            transformedAst: expect.objectContaining({table: 'issue'}),
            originalAst: ISSUES_WITH_CREATOR,
            reason: 'slow-change',
            hydrationTimeMs: 10,
          },
        ],
      ]),
    );
    expect(dropLogs()).toEqual([
      {
        context: expect.objectContaining({
          queryHash: 'expensive',
          transformationHash: 'expensive-hash',
        }),
        args: [
          'resetting pipeline: Advancement exceeded timeout processing ' +
            'current change at 0 of 1 changes after 51 ms (51 ms total). ' +
            'Advancement time limited based on pipeline hydration time of ' +
            '10 ms.',
          {
            reason: 'slow-change',
            advancementTimeMs: 51,
            hydrationTimeMs: 10,
            pos: 0,
            numChanges: 1,
          },
        ],
      },
    ]);

    // The dropped pipeline was destroyed, and `issue`, which only it read,
    // is no longer observed.
    expect([...pipelines.queries().keys()]).toEqual(['cheap']);
    const observed = advanceSpy.mock.calls.at(-1)?.[2];
    expect(observed?.has('issue')).toBe(false);
    expect(observed?.has('user')).toBe(true);
    expect(pipelines.rowSetSignature('expensive')).toBeUndefined();

    // The dropped query can be added again, at the new head.
    const rebuilt = addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    expect(new Set(rebuilt.map(c => c.row))).toContainEqual({
      id: 'u1',
      name: 'wuzzy',
      _0_version: '134',
    });
    expect(pipelines.droppedQueries().size).toBe(1);

    // The next advancement clears the dropped set, and both advance.
    rowCost = () => 0;
    replicator.processTransaction(
      '135',
      messages.update('user', {id: 'u1', name: 'fuzzy'}),
    );
    const next = advance();
    expect(pipelines.droppedQueries().size).toBe(0);
    expect(rows(next, 'cheap')).toEqual([`cheap:${ChangeType.EDIT}:user:u1`]);
    expect(rows(next, 'expensive')).toHaveLength(1000);
  });

  test('without partial resets, the whole group is reset', () => {
    pipelines = newDriver('no-partial-reset', false);
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    addQuery('cheap', USERS, 1000);

    rowCost = table => (table === 'issue' ? 1 : 0);
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    let thrown: unknown;
    try {
      advance();
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ResetPipelinesSignal);
    expect(thrown).not.toBeInstanceOf(DropPipelineSignal);
    expect(String(thrown)).toMatch(/total hydration time of 1010 ms/);
    expect(pipelines.droppedQueries().size).toBe(0);
  });

  test('output from changes before the drop is kept', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    addQuery('cheap', USERS, 1000);

    rowCost = table => (table === 'issue' ? 1 : 0);
    replicator.processTransaction(
      '134',
      messages.insert('issue', {id: 'i1001', creatorID: 'u1'}),
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    const changes = advance();

    expect(rows(changes)).toEqual([
      `expensive:${ChangeType.ADD}:issue:i1001`,
      `expensive:${ChangeType.ADD}:user:u1`,
      `cheap:${ChangeType.EDIT}:user:u1`,
    ]);
    expect(pipelines.droppedQueries().get('expensive')?.reason).toBe(
      'slow-change',
    );
  });

  test('yields continue while a dropped pipeline awaits destruction', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    addQuery('also-expensive', ISSUES_WITH_CREATOR, 100_000);

    rowCost = table => (table === 'issue' ? 1 : 0);
    lap = 1000; // Every check yields.
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    let droppedAt: number | undefined;
    let consumed = 0;
    const changes = advance(pipelines, () => {
      consumed++;
      if (droppedAt === undefined && dropLogs().length > 0) {
        droppedAt = consumed;
      }
    });

    expect(pipelines.droppedQueries().has('expensive')).toBe(true);
    expect(droppedAt).toBeDefined();
    const afterDrop = changes.slice(droppedAt);
    expect(afterDrop.filter(c => c === 'yield').length).toBeGreaterThan(900);
    expect(rows(afterDrop, 'expensive')).toEqual([]);
    expect(rows(changes, 'also-expensive')).toHaveLength(1000);
  });

  test('a drop raised while streaming lazily fetched relationships', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', USERS_WITH_ISSUES, 10);
    addQuery('cheap', USERS, 1000);

    // Adding u2 streams u2's row, and then fetches u2's 20 issues.
    rowCost = table => (table === 'issue' ? 5 : 0);
    replicator.processTransaction(
      '134',
      messages.insert('user', {id: 'u2', name: 'two'}),
    );
    const changes = advance();

    // The rows streamed before the drop (u2 and 11 issues) are discarded with
    // the rest of the change's output: the output of a push is yielded after
    // the push.
    expect(rows(changes)).toEqual([`cheap:${ChangeType.ADD}:user:u2`]);
    expect(fetched.get('issue')).toBe(11);
    expect(pipelines.droppedQueries().get('expensive')?.reason).toBe(
      'slow-change',
    );
    expect([...pipelines.queries().keys()]).toEqual(['cheap']);
  });

  test('a pipeline dropped after a yield in its push keeps the output yielded before it', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS, 10);
    addQuery('cheap', USERS, 1000);
    addQuery('creators', ISSUES_WITH_CREATOR, 100_000);

    // Adding u2 adds u2's issues to `expensive`, fetching them one by one.
    // Each issue is streamed with its comments as it is added: the comments
    // are expensive. The push yields when it fetches the 2nd issue, which
    // yields the output of the 1st.
    rowCost = table => (table === 'comment' ? 10 : 0);
    let issuesFetched = 0;
    onRow = table => {
      if (table === 'issue' && ++issuesFetched === 2) {
        lap = 1000;
      }
    };
    replicator.processTransaction(
      '134',
      messages.insert('user', {id: 'u2', name: 'two'}),
      // Its creator is read from the snapshot, which must have u2.
      messages.insert('issue', {id: 'j21', creatorID: 'u2'}),
    );
    const changes = advance(pipelines, change => {
      if (change === 'yield') {
        lap = 0;
      }
    });

    expect(pipelines.droppedQueries().get('expensive')?.reason).toBe(
      'slow-change',
    );
    // The output yielded at the yield is kept. The 6th comment (c02-1) takes
    // `expensive` over the 50 ms minimum limit, and the output streamed after
    // the yield (j02, u2 and c02-1) is discarded.
    expect(rows(changes, 'expensive')).toEqual([
      `expensive:${ChangeType.ADD}:issue:j01`,
      `expensive:${ChangeType.ADD}:user:u2`,
      ...Array.from(
        {length: 5},
        (_, i) => `expensive:${ChangeType.ADD}:comment:c01-${i + 1}`,
      ),
    ]);
    expect(changes.indexOf('yield')).toBeLessThan(
      changes.findIndex(c => c !== 'yield' && c.queryID === 'expensive'),
    );
    expect(fetched.get('comment')).toBe(6);
    // The other pipelines received the change, after the dropped one.
    expect(rows(changes, 'cheap')).toEqual([`cheap:${ChangeType.ADD}:user:u2`]);
    // The change was written to the snapshot: the creator of j21 is found.
    expect(rows(changes, 'creators')).toContain(
      `creators:${ChangeType.ADD}:issue:j21`,
    );
    expect(
      changes.filter(
        c =>
          c !== 'yield' &&
          c.queryID === 'creators' &&
          c.table === 'user' &&
          c.rowKey.id === 'u2',
      ),
    ).toHaveLength(21);
  });

  test('a drop after its push yielded inside an EXISTS fetch', () => {
    pipelines.init(clientSchema);
    addQuery(
      'expensive',
      ISSUES_WITH_EXISTING_CREATOR_AND_EXISTING_COMMENTS,
      10,
    );
    addQuery('cheap', USERS, 1000);

    // Adding u2 adds j01 once its EXISTS fetched j01's comments (Cap's
    // initial fetch). Streaming j01 in the push fetches them again, and the
    // push yields after the first one. The 6th comment then takes `expensive`
    // over the 50 ms minimum limit, and the drop is raised by the fetch of
    // the next issue.
    rowCost = table => (table === 'comment' ? 10 : 0);
    onRow = table => {
      if (table === 'comment' && fetched.get('comment') === 4) {
        lap = 1000;
      }
    };
    replicator.processTransaction(
      '134',
      messages.insert('user', {id: 'u2', name: 'two'}),
    );
    const changes = advance(pipelines, change => {
      if (change === 'yield') {
        lap = 0;
      }
    });

    expect(changes).toContain('yield');
    expect(pipelines.droppedQueries().get('expensive')?.reason).toBe(
      'slow-change',
    );
    expect(fetched.get('comment')).toBe(6);
    // The output yielded at the yield is kept.
    expect(rows(changes, 'expensive')).toEqual([
      `expensive:${ChangeType.ADD}:issue:j01`,
      `expensive:${ChangeType.ADD}:user:u2`,
      `expensive:${ChangeType.ADD}:comment:c01-1`,
    ]);
    expect(rows(changes, 'cheap')).toEqual([`cheap:${ChangeType.ADD}:user:u2`]);
  });

  test('a drop in a Take refill is raised and caught in reconcile', () => {
    pipelines.init(clientSchema);
    addQuery('limited', {...ISSUES, limit: 3}, 10);
    addQuery('cheap', ISSUES, 1000);

    // Only the refill in phase 2 (reconcile) is expensive.
    let inReconcile = false;
    let raisedInReconcile: unknown;
    const reconcile = Take.prototype.reconcile;
    vi.spyOn(Take.prototype, 'reconcile').mockImplementation(function* (
      this: Take,
      ...args
    ) {
      inReconcile = true;
      try {
        yield* reconcile.apply(this, args);
      } catch (e) {
        raisedInReconcile = e;
        throw e;
      } finally {
        inReconcile = false;
      }
    });
    fetchCost = table => (inReconcile && table === 'issue' ? 100 : 0);
    replicator.processTransaction(
      '134',
      messages.delete('issue', {id: 'i1'}),
      messages.insert('issue', {id: 'i1001', creatorID: 'u9'}),
    );
    const changes = advance();

    expect(raisedInReconcile).toBeInstanceOf(DropPipelineSignal);
    expect(pipelines.droppedQueries().get('limited')?.reason).toBe(
      'slow-change',
    );
    // The remove was pushed to `limited` before its refill failed. The other
    // query advanced through both changes.
    expect(rows(changes)).toEqual([
      `limited:${ChangeType.REMOVE}:issue:i1`,
      `cheap:${ChangeType.REMOVE}:issue:i1`,
      `cheap:${ChangeType.ADD}:issue:i1001`,
    ]);
  });

  test('a pipeline dropped in its push is not reconciled', () => {
    pipelines.init(clientSchema);
    addQuery('limited', {...ISSUES, limit: 3}, 10);
    addQuery('cheap', ISSUES, 1000);

    // The removal of a row in the window fetches its predecessor in the push.
    let pushing = true;
    const reconcile = Take.prototype.reconcile;
    const reconcileSpy = vi
      .spyOn(Take.prototype, 'reconcile')
      .mockImplementation(function (this: Take, ...args) {
        pushing = false;
        return reconcile.apply(this, args);
      });
    fetchCost = table => (pushing && table === 'issue' ? 100 : 0);
    // The window is [i1, i10, i100].
    replicator.processTransaction('134', messages.delete('issue', {id: 'i10'}));
    const changes = advance();

    expect(pipelines.droppedQueries().get('limited')?.reason).toBe(
      'slow-change',
    );
    expect(reconcileSpy).not.toHaveBeenCalled();
    expect(rows(changes)).toEqual([`cheap:${ChangeType.REMOVE}:issue:i10`]);
  });

  test('one drop covers every connection of a pipeline', () => {
    pipelines.init(clientSchema);
    addQuery('siblings', ISSUES_WITH_SIBLINGS, 10);
    addQuery('cheap', ISSUES, 1000);

    // A new issue of u1 is streamed with its 1000 siblings (the first
    // connection). The second connection would fetch each of its siblings to
    // add it to their `siblings`.
    rowCost = table => (table === 'issue' ? 1 : 0);
    replicator.processTransaction(
      '134',
      messages.insert('issue', {id: 'i1001', creatorID: 'u1'}),
    );
    const changes = advance();

    expect(pipelines.droppedQueries().get('siblings')?.reason).toBe(
      'slow-change',
    );
    expect(fetched.get('issue')).toBeLessThan(100);
    expect(rows(changes, 'cheap')).toEqual([
      `cheap:${ChangeType.ADD}:issue:i1001`,
    ]);
  });

  test('a split edit dropped between the remove and the add', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 10);
    addQuery('cheap', ISSUES, 1000);

    // Changing an issue's creator splits the edit into a remove and an add
    // (for every connection). Streaming the removed issue fetches its
    // creator, which drops `expensive` and discards its remove.
    fetchCost = table => (table === 'user' ? 100 : 0);
    const removed: number[] = [];
    let consumed = 0;
    const removeQuery = vi.spyOn(pipelines, 'removeQuery');
    removeQuery.mockImplementation(function (
      this: PipelineDriver,
      ...args: Parameters<PipelineDriver['removeQuery']>
    ) {
      removed.push(consumed);
      removeQuery.mock.calls.length; // (keeps the spy's bookkeeping)
      return PipelineDriver.prototype.removeQuery.apply(this, args);
    });
    replicator.processTransaction(
      '134',
      messages.update('issue', {id: 'i1', creatorID: 'u9'}),
    );
    const changes = advance(pipelines, () => consumed++);

    expect(pipelines.droppedQueries().get('expensive')?.reason).toBe(
      'slow-change',
    );
    expect(rows(changes)).toEqual([
      `cheap:${ChangeType.REMOVE}:issue:i1`,
      `cheap:${ChangeType.ADD}:issue:i1`,
    ]);
    // Destroyed after both halves of the edit.
    expect(removed).toEqual([changes.length]);
  });

  test('time is charged once for output streamed in a push that yields', () => {
    pipelines.init(clientSchema);
    // The advancement stays within half of the budget only if the time spent
    // streaming the push's output is charged once.
    addQuery('comments', ISSUES_WITH_EXISTING_CREATOR_AND_COMMENTS, 400, 1e9);

    rowCost = table => (table === 'comment' ? 3 : 0);
    let issuesFetched = 0;
    onRow = table => {
      if (table === 'issue' && ++issuesFetched === 10) {
        lap = 1000;
      }
    };
    replicator.processTransaction(
      '134',
      messages.insert('user', {id: 'u2', name: 'two'}),
    );
    const changes = advance(pipelines, change => {
      if (change === 'yield') {
        lap = 0;
      }
    });

    expect(changes).toContain('yield');
    // 50 comments at 3 ms each.
    expect(fetched.get('comment')).toBe(50);
    expect(pipelines.droppedQueries().size).toBe(0);
    expect(rows(changes, 'comments')).toHaveLength(20 + 20 + 50);
  });

  test("the consumer's time between pulls is not charged to pipelines", () => {
    pipelines.init(clientSchema);
    addQuery('issues', ISSUES, 100, 1e9);

    replicator.processTransaction(
      '134',
      ...Array.from({length: 10}, (_, i) =>
        messages.insert('issue', {id: `k${i}`, creatorID: 'u9'}),
      ),
    );
    const changes = advance(pipelines, () => {
      now += 1000;
    });

    expect(rows(changes)).toHaveLength(10);
    expect(pipelines.droppedQueries().size).toBe(0);
  });

  test('progress is measured by the changes to the tables a pipeline reads', () => {
    pipelines.init(clientSchema);
    addQuery('comments', COMMENTS_WITH_SIBLINGS, 300, 1e9);
    addQuery('issues', ISSUES, 1000, 1e9);

    // The changes to comments all come first, and take 10 ms each. By the
    // time `comments` has taken more than half of its budget (150 ms), it is
    // more than half through its own 20 changes, but not through all 120.
    rowCost = table => (table === 'comment' ? 10 : 0);
    replicator.processTransaction(
      '134',
      ...Array.from({length: 20}, (_, i) =>
        messages.insert('comment', {id: `d${i}`, issueID: `x${i}`}),
      ),
    );
    replicator.processTransaction(
      '135',
      ...Array.from({length: 100}, (_, i) =>
        messages.insert('issue', {id: `k${i}`, creatorID: 'u9'}),
      ),
    );
    const changes = advance();

    expect(pipelines.droppedQueries().size).toBe(0);
    expect(fetched.get('comment')).toBe(20);
    expect(rows(changes, 'issues')).toHaveLength(100);
  });

  test('resets the whole group when the dropped pipelines are most of it', () => {
    pipelines.init(clientSchema);
    addQuery('expensive', ISSUES_WITH_CREATOR, 100, 1000);
    addQuery('cheap', USERS, 50, 1000);

    rowCost = table => (table === 'issue' ? 1 : 0);
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    let thrown: unknown;
    try {
      advance();
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ResetPipelinesSignal);
    expect(thrown).not.toBeInstanceOf(DropPipelineSignal);
    expect(String(thrown)).toMatch(
      /^ResetPipelinesSignal: Dropped pipelines account for 100 ms of the total hydration time of 150 ms\. Advancement exceeded timeout at 0 of 1 changes after 51 ms\. Advancement time limited based on pipeline hydration time of 100 ms\.$/,
    );
    // The dropped pipeline is reported, so that the reset can reuse it.
    expect([...pipelines.droppedQueries().keys()]).toEqual(['expensive']);
  });

  test('the group budget still bounds pipelines that are each within theirs', () => {
    pipelines.init(clientSchema);
    for (let i = 0; i < 5; i++) {
      addQuery(`q${i}`, ISSUES_WITH_CREATOR, 5);
    }

    // Each pipeline takes 30 ms, within the 50 ms minimum limit that each
    // pipeline has, but the group's budget is 25 ms.
    rowCost = table => (table === 'issue' ? 0.03 : 0);
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    let thrown: unknown;
    try {
      advance();
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ResetPipelinesSignal);
    expect(thrown).not.toBeInstanceOf(DropPipelineSignal);
    expect(String(thrown)).toMatch(/total hydration time of 25 ms\.$/);
    expect(pipelines.droppedQueries().size).toBe(0);
  });

  test('a drop signal that escapes resets the group and is not logged as a failure', () => {
    pipelines.init(clientSchema);
    addQuery('issues', ISSUES_WITH_CREATOR, 1000);

    onRow = () => {
      throw new DropPipelineSignal('some-other-query', 'timeout', 'escaped');
    };
    replicator.processTransaction(
      '134',
      messages.update('user', {id: 'u1', name: 'wuzzy'}),
    );
    let thrown: unknown;
    try {
      advance();
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toBeInstanceOf(ResetPipelinesSignal);
    expect(logSink.messages.filter(([level]) => level === 'error')).toEqual([]);
  });

  describe('scalar subqueries', () => {
    test('a dropped query is rebuilt with companions at the new head', () => {
      pipelines.init(clientSchema);
      addQuery('creator', CREATOR_OF_I1_WITH_ISSUES, 10);
      addQuery('cheap', ISSUES, 1000);

      // A new issue of u1 is added to u1's issues in `creator`, which first
      // fetches u1.
      fetchCost = table => (table === 'user' ? 100 : 0);
      replicator.processTransaction(
        '134',
        messages.insert('issue', {id: 'i1001', creatorID: 'u1'}),
        // Changes the scalar value of `creator` after it is dropped. Its
        // companion is gone, so this does not reset the group.
        messages.update('issue', {id: 'i1', creatorID: 'u9'}),
      );
      const changes = advance();
      expect(pipelines.droppedQueries().get('creator')?.reason).toBe(
        'slow-change',
      );
      expect(rows(changes, 'cheap')).toEqual([
        `cheap:${ChangeType.ADD}:issue:i1001`,
        `cheap:${ChangeType.EDIT}:issue:i1`,
      ]);

      // The rebuild resolves the scalar value at the new head.
      fetchCost = () => 0;
      const dropped = must(pipelines.droppedQueries().get('creator'));
      const rebuilt = addQuery('creator', dropped.originalAst, 10);
      expect(rebuilt.filter(c => c.table === 'user')).toEqual([
        expect.objectContaining({rowKey: {id: 'u9'}}),
      ]);

      // Its new companion watches the scalar value.
      replicator.processTransaction(
        '135',
        messages.update('issue', {id: 'i1', creatorID: 'u1'}),
      );
      expect(() => advance()).toThrowError(/Scalar subquery value changed/);
    });
  });

  test('a drop on a table with unique keys, with a row cache shared with a group that does not drop', () => {
    const rowCache = new SnapshotRowCache(100);
    const a = newDriver('a', true, rowCache);
    const b = newDriver('b', true, rowCache);
    a.init(clientSchema);
    b.init(clientSchema);
    const UNIQUES_WITH_NAMESAKES: AST = {
      table: 'uniques',
      orderBy: [['id', 'asc']],
      related: [
        {
          system: 'client',
          correlation: {parentField: ['name'], childField: ['name']},
          subquery: {
            table: 'uniques',
            alias: 'namesakes',
            orderBy: [['id', 'asc']],
          },
        },
      ],
    };
    addQuery('namesakes', UNIQUES_WITH_NAMESAKES, 10, 10, a);
    addQuery('cheap', USERS, 1000, 1000, a);
    addQuery(
      'uniques',
      {table: 'uniques', orderBy: [['id', 'asc']]},
      10,
      10,
      b,
    );

    // The rename of foo is split, and streaming the removed foo fetches its
    // namesakes in `a`, which drops `namesakes` (and discards the remove).
    // `a` then no longer observes `uniques`, and skips the insert of baz,
    // which takes foo's old name.
    fetchCost = table => (table === 'uniques' ? 100 : 0);
    replicator.processTransaction(
      '134',
      messages.update('uniques', {id: 'foo', name: 'wuzzy'}),
      messages.insert('uniques', {id: 'baz', name: 'bar'}),
    );

    expect(rows(advance(a))).toEqual([]);
    expect(a.droppedQueries().has('namesakes')).toBe(true);
    expect(rows(advance(b))).toEqual([
      `uniques:${ChangeType.EDIT}:uniques:foo`,
      `uniques:${ChangeType.ADD}:uniques:baz`,
    ]);
  });
});

function must<T>(value: T | undefined): T {
  expect(value).toBeDefined();
  return value as T;
}
