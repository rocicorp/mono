// Benchmarks the per-client-group duplication of IVM advancement: N
// PipelineDrivers (one per client group today) each advancing IDENTICAL
// queries over one replica. Every driver independently scans the changelog and
// pushes the same changes through its own copy of the same pipelines, so the
// cost of serving one logical write scales with the number of client groups
// rather than the number of unique queries. This quantifies the headroom for
// shared pipeline advancement (lever A1 in
// apps/zero-throughput/reports/2026-07-rm-vs-fanout/design-10x.md): a shared
// driver would do the N=1 work once, plus a cheap per-client-group fold.
//
//   pnpm --filter zero-cache exec vitest run --config vitest.config.bench.ts \
//     pipeline-driver-advance
//
// Env overrides: PIPELINE_ADVANCE_REPS (default 5),
// PIPELINE_ADVANCE_WRITES (logical writes per rep, default 32).

import {LogContext} from '@rocicorp/logger';
import {afterEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {upstreamSchema, type ShardID} from '../../types/shards.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.ts';
import {PipelineDriver, type Timer} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';

const REPS = integerFromEnv('PIPELINE_ADVANCE_REPS', 5);
const WRITES_PER_REP = integerFromEnv('PIPELINE_ADVANCE_WRITES', 32);
const DRIVER_COUNTS = [1, 10, 50];
const QUERIES_PER_DRIVER = 3;

const ISSUE_COUNT = 64;
const COMMENTS_PER_ISSUE = 4;

const TEST_TIMEOUT_MS = 600_000;

// Keeps the driver's advancement-budget heuristics out of the measurement
// (matches pipeline-driver.test.ts).
const NO_TIME_ADVANCEMENT_TIMER: Timer = {
  elapsedLap: () => 0,
  totalElapsed: () => 0,
};

const shardID: ShardID = {appID: 'bench_app', shardNum: 0};

const issues = table('issues')
  .columns({
    id: string(),
    closed: boolean(),
    title: string(),
  })
  .primaryKey('id');
const comments = table('comments')
  .columns({
    id: string(),
    issueID: string(),
    upvotes: number(),
  })
  .primaryKey('id');

const clientSchema = createSchema({
  tables: [issues, comments],
  relationships: [],
});

const ISSUES_AND_COMMENTS: AST = {
  table: 'issues',
  orderBy: [['id', 'desc']],
  related: [
    {
      system: 'client',
      correlation: {
        parentField: ['id'],
        childField: ['issueID'],
      },
      subquery: {
        table: 'comments',
        alias: 'comments',
        orderBy: [['id', 'desc']],
      },
    },
  ],
};

const OPEN_ISSUES: AST = {
  table: 'issues',
  orderBy: [['id', 'asc']],
  where: {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'closed'},
    right: {type: 'literal', value: false},
  },
};

const TOP_COMMENTS: AST = {
  table: 'comments',
  orderBy: [
    ['upvotes', 'desc'],
    ['id', 'asc'],
  ],
  limit: 100,
};

const QUERIES: readonly AST[] = [
  ISSUES_AND_COMMENTS,
  OPEN_ISSUES,
  TOP_COMMENTS,
];

const messages = new ReplicationMessages({issues: 'id', comments: 'id'});

function createReplica(lc: LogContext): {dbFile: DbFile; db: Database} {
  const dbFile = new DbFile('pipeline_advance_bench');
  dbFile.connect(lc).pragma('journal_mode = wal2');

  const db = dbFile.connect(lc);
  initReplicationState(db, ['zero_data'], '01');
  const mutationsTableName = `${upstreamSchema(shardID)}.mutations`;
  db.exec(/*sql*/ `
    CREATE TABLE "${mutationsTableName}" (
      "clientGroupID"  TEXT,
      "clientID"       TEXT,
      "mutationID"     INTEGER,
      "result"         TEXT,
      _0_version       TEXT NOT NULL,
      PRIMARY KEY ("clientGroupID", "clientID", "mutationID")
    );
    CREATE TABLE issues (
      id TEXT PRIMARY KEY,
      closed BOOL,
      title TEXT,
      _0_version TEXT NOT NULL
    );
    CREATE TABLE comments (
      id TEXT PRIMARY KEY,
      issueID TEXT,
      upvotes INTEGER,
      _0_version TEXT NOT NULL
    );
    `);

  const insertIssue = db.prepare(
    `INSERT INTO issues (id, closed, title, _0_version) VALUES (?, 0, ?, '01')`,
  );
  const insertComment = db.prepare(
    `INSERT INTO comments (id, issueID, upvotes, _0_version) VALUES (?, ?, ?, '01')`,
  );
  for (let i = 0; i < ISSUE_COUNT; i++) {
    const issueID = `issue-${String(i).padStart(4, '0')}`;
    insertIssue.run(issueID, `Issue ${i}`);
    for (let c = 0; c < COMMENTS_PER_ISSUE; c++) {
      insertComment.run(`${issueID}-c${c}`, issueID, c);
    }
  }
  return {dbFile, db};
}

function createDriver(
  lc: LogContext,
  dbFile: DbFile,
  clientGroup: number,
): PipelineDriver {
  const storage = new Database(lc, ':memory:');
  storage.prepare(CREATE_STORAGE_TABLE).run();
  const driver = new PipelineDriver(
    lc,
    testLogConfig,
    new Snapshotter(lc, dbFile.path, {appID: shardID.appID}),
    shardID,
    new DatabaseStorage(storage).createClientGroupStorage(`cg-${clientGroup}`),
    'pipeline-driver-advance.bench.ts',
    new InspectorDelegate(undefined),
    () => 200,
  );
  driver.init(clientSchema);
  for (let q = 0; q < QUERIES_PER_DRIVER; q++) {
    // Consuming the hydration stream is what materializes the pipeline. The
    // same query set is added to every driver, modelling client groups that
    // run identical (identically-transformed) queries.
    consume(
      driver.addQuery(
        `hash-${q}`,
        `query-${q}`,
        QUERIES[q % QUERIES.length],
        NO_TIME_ADVANCEMENT_TIMER,
      ),
    );
  }
  return driver;
}

/** Drains a change stream, returning the number of non-'yield' entries. */
function consume(changes: Iterable<unknown>): number {
  let n = 0;
  for (const change of changes) {
    if (change !== 'yield') {
      n++;
    }
  }
  return n;
}

describe('view-syncer/pipeline-driver advance duplication', () => {
  const recorder = createManualBenchmarkRecorder();
  const cleanup: (() => void)[] = [];

  afterEach(() => {
    for (const fn of cleanup.splice(0)) {
      fn();
    }
  });

  test.each(DRIVER_COUNTS)(
    'advance %i drivers with identical queries',
    driverCount => {
      const lc = new LogContext('error');
      const {dbFile, db} = createReplica(lc);
      cleanup.push(() => dbFile.delete());

      const replicator = fakeReplicator(lc, db);
      const drivers = Array.from({length: driverCount}, (_, i) =>
        createDriver(lc, dbFile, i),
      );
      cleanup.push(() => drivers.forEach(d => d.destroy()));

      let nextVersion = 100;
      let nextCommentID = ISSUE_COUNT * COMMENTS_PER_ISSUE;

      const writeBatch = () => {
        for (let w = 0; w < WRITES_PER_REP; w++) {
          const issueID = `issue-${String(w % ISSUE_COUNT).padStart(4, '0')}`;
          // One logical write = update an issue + insert a comment, the
          // update+append shape of the throughput hot model.
          replicator.processTransaction(
            String(nextVersion++),
            messages.update('issues', {
              id: issueID,
              closed: false,
              title: `Issue ${w} v${nextVersion}`,
            }),
            messages.insert('comments', {
              id: `comment-${nextCommentID++}`,
              issueID,
              upvotes: nextVersion,
            }),
          );
        }
      };

      const advanceAll = () => {
        let rowChanges = 0;
        for (const driver of drivers) {
          // Fully consuming the stream is what commits the advance.
          rowChanges += consume(
            driver.advance(NO_TIME_ADVANCEMENT_TIMER).changes,
          );
        }
        return rowChanges;
      };

      // Warmup.
      writeBatch();
      expect(advanceAll()).toBeGreaterThan(0);

      const samples: number[] = [];
      for (let rep = 0; rep < REPS; rep++) {
        writeBatch();
        const start = performance.now();
        advanceAll();
        samples.push(performance.now() - start);
      }

      recorder.recordThroughput(
        `advance drivers=${driverCount} queries=${QUERIES_PER_DRIVER} writes=${WRITES_PER_REP} logical writes`,
        samples,
        WRITES_PER_REP,
      );
    },
    TEST_TIMEOUT_MS,
  );
});

function integerFromEnv(name: string, defaultValue: number): number {
  const raw = process.env[name];
  if (raw === undefined || raw === '') {
    return defaultValue;
  }
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
  return value;
}
