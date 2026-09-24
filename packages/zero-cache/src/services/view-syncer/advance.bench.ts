import {describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createManualBenchmarkRecorder} from '../../../../shared/src/bench.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST} from '../../../../zero-protocol/src/ast.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {listTables} from '../../db/lite-tables.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import type {ShardID} from '../../types/shards.ts';
import {populateFromExistingTables} from '../replicator/schema/column-metadata.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {fakeReplicator, ReplicationMessages} from '../replicator/test-utils.ts';
import {BYTES_PER_ROW, DeferredWritesBudget} from './deferred-writes-budget.ts';
import {PipelineDriver, type Timer} from './pipeline-driver.ts';
import {SnapshotRowCache} from './snapshot-row-cache.ts';
import {Snapshotter} from './snapshotter.ts';

// Run: pnpm --filter zero-cache run bench advance
//
// The cost of advancing the pipelines of the client groups on one sync worker
// through a transaction, with IVM derivation written through to (and rolled
// back out of) each group's replica snapshot, as on main, and with it held in
// memory (`deferIvmWrites`).
//
// Each client group has a PipelineDriver of its own over a shared replica, row
// cache and (when deferring) budget, as in a sync worker. Each hydrates one
// query: the top issues of its owner, with their latest comments. Each
// transaction moves issues to the top of their owners' windows and adds
// comments to random issues, so advancing pushes through Take and the
// comments join, which fetch from the sources mid-push. The timer never
// elapses, so no advancement is reset, and each sample is the thread CPU time
// to advance every group through one transaction (CPU rather than wall time,
// so that the rest of the machine's load does not show up in it). The
// transactions are the same in both modes.

const GROUPS = 100;
const ISSUES_PER_GROUP = 100;
const COMMENTS_PER_ISSUE = 5;
const WARMUP_TRANSACTIONS = 10;
const TRANSACTIONS = 60;

const shardID: ShardID = {appID: 'bench', shardNum: 0};
const lc = createSilentLogContext();
const NEVER_ELAPSES: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};

function cpuMs() {
  const {user, system} = process.threadCpuUsage();
  return (user + system) / 1000;
}

const clientSchema = createSchema({
  tables: [
    table('issues')
      .columns({id: string(), owner: string(), modified: number()})
      .primaryKey('id'),
    table('comments')
      .columns({
        id: string(),
        issueID: string(),
        body: string(),
        modified: number(),
      })
      .primaryKey('id'),
  ],
});

function ownerQuery(owner: string): AST {
  return {
    table: 'issues',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'owner'},
      op: '=',
      right: {type: 'literal', value: owner},
    },
    orderBy: [
      ['modified', 'desc'],
      ['id', 'desc'],
    ],
    limit: 20,
    related: [
      {
        system: 'client',
        correlation: {parentField: ['id'], childField: ['issueID']},
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: [
            ['modified', 'desc'],
            ['id', 'desc'],
          ],
          limit: 3,
        },
      },
    ],
  };
}

const ISSUES = GROUPS * ISSUES_PER_GROUP;
const issueID = (i: number) => `i${i}`;
const ownerOf = (i: number) => `u${i % GROUPS}`;

function createReplica(dbFile: DbFile) {
  const db = dbFile.connect(lc);
  db.pragma('journal_mode = wal2');
  initReplicationState(db, ['zero_data'], '01');
  db.exec(/*sql*/ `
    CREATE TABLE issues (
      id TEXT PRIMARY KEY,
      owner TEXT,
      modified INTEGER,
      _0_version TEXT NOT NULL
    );
    CREATE INDEX issues_owner ON issues (owner, modified);
    CREATE TABLE comments (
      id TEXT PRIMARY KEY,
      issueID TEXT,
      body TEXT,
      modified INTEGER,
      _0_version TEXT NOT NULL
    );
    CREATE INDEX comments_issue ON comments (issueID, modified);

    WITH RECURSIVE n(i) AS (
      SELECT 0 UNION ALL SELECT i + 1 FROM n WHERE i < ${ISSUES - 1}
    )
    INSERT INTO issues
      SELECT 'i' || i, 'u' || (i % ${GROUPS}), i, '01' FROM n;

    WITH RECURSIVE n(i) AS (
      SELECT 0 UNION ALL
      SELECT i + 1 FROM n WHERE i < ${ISSUES * COMMENTS_PER_ISSUE - 1}
    )
    INSERT INTO comments
      SELECT 'c' || i, 'i' || (i % ${ISSUES}),
        'a comment of a typical length, give or take', i, '01' FROM n;
  `);
  populateFromExistingTables(db, listTables(db, false));
  return db;
}

// mulberry32, so both modes see the same transactions.
function rng(seed: number) {
  return () => {
    seed = (seed + 0x6d2b79f5) | 0;
    let t = seed;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

type Mode = 'write-through' | 'deferred';

function run(mode: Mode, changesPerTransaction: number) {
  const dbFile = new DbFile('advance_bench');
  try {
    const replica = fakeReplicator(lc, createReplica(dbFile));
    const messages = new ReplicationMessages({issues: 'id', comments: 'id'});
    const rowCache = new SnapshotRowCache();
    const maxBytes = 1024 * 1024 * 1024;
    const budget =
      mode === 'deferred'
        ? new DeferredWritesBudget(maxBytes / BYTES_PER_ROW, maxBytes)
        : undefined;
    const storage = new Database(lc, ':memory:');
    storage.prepare(CREATE_STORAGE_TABLE).run();
    const operatorStorage = new DatabaseStorage(storage);

    const drivers = Array.from({length: GROUPS}, (_, g) => {
      const driver = new PipelineDriver(
        lc,
        testLogConfig,
        new Snapshotter(
          lc,
          dbFile.path,
          {appID: shardID.appID},
          undefined,
          rowCache,
        ),
        shardID,
        operatorStorage.createClientGroupStorage(`cg${g}`),
        `cg${g}`,
        new InspectorDelegate(undefined),
        () => Number.MAX_SAFE_INTEGER,
        undefined,
        undefined,
        budget,
      );
      driver.init(clientSchema);
      for (const _ of driver.addQuery(
        'hash',
        'query',
        ownerQuery(`u${g}`),
        NEVER_ELAPSES,
      )) {
        // hydrate
      }
      return driver;
    });

    const random = rng(1);
    let stamp = ISSUES * COMMENTS_PER_ISSUE;
    let nextComment = ISSUES * COMMENTS_PER_ISSUE;
    let version = 2;
    const commit = () => {
      const msgs = [];
      for (let c = 0; c < changesPerTransaction; c += 2) {
        const i = Math.floor(random() * ISSUES);
        msgs.push(
          messages.update('issues', {
            id: issueID(i),
            owner: ownerOf(i),
            modified: stamp++,
          }),
          messages.insert('comments', {
            id: `c${nextComment++}`,
            issueID: issueID(Math.floor(random() * ISSUES)),
            body: 'a comment of a typical length, give or take',
            modified: stamp++,
          }),
        );
      }
      replica.processTransaction(
        (version++).toString(36).padStart(2, '0'),
        ...msgs,
      );
    };
    const advanceAll = () => {
      let rows = 0;
      for (const driver of drivers) {
        for (const change of driver.advance(NEVER_ELAPSES).changes) {
          if (change !== 'yield') {
            rows++;
          }
        }
      }
      return rows;
    };

    for (let t = 0; t < WARMUP_TRANSACTIONS; t++) {
      commit();
      advanceAll();
    }
    const samples: number[] = [];
    let rows = 0;
    for (let t = 0; t < TRANSACTIONS; t++) {
      commit();
      const start = cpuMs();
      rows += advanceAll();
      samples.push(cpuMs() - start);
    }
    for (const driver of drivers) {
      driver.destroy();
    }
    if (budget) {
      // Every advancement was held in memory, and released.
      expect([budget.reservedRows, budget.heldBytes]).toEqual([0, 0]);
    }
    return {samples, rows};
  } finally {
    dbFile.delete();
  }
}

describe(`advancing ${GROUPS} client groups through a transaction`, () => {
  const recorder = createManualBenchmarkRecorder();
  for (const changes of [10, 100]) {
    test(`${changes} changes per transaction`, {timeout: 300_000}, () => {
      const rows: number[] = [];
      for (const mode of ['write-through', 'deferred'] as const) {
        const result = run(mode, changes);
        recorder.recordLatency(
          `${mode}: ${changes} changes × ${GROUPS} groups`,
          result.samples,
        );
        rows.push(result.rows);
      }
      // Both modes produce the same row changes.
      expect(rows[1]).toBe(rows[0]);
    });
  }
});
