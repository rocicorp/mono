import {afterEach, beforeEach, describe, expect, test} from 'vitest';
import {testLogConfig} from '../../../../otel/src/test-log-config.ts';
import {createSilentLogContext} from '../../../../shared/src/logging-test-utils.ts';
import type {AST, CompoundKey} from '../../../../zero-protocol/src/ast.ts';
import type {ClientSchema} from '../../../../zero-protocol/src/client-schema.ts';
import {ChangeType} from '../../../../zql/src/ivm/change-type.ts';
import {
  CREATE_STORAGE_TABLE,
  DatabaseStorage,
} from '../../../../zqlite/src/database-storage.ts';
import {Database} from '../../../../zqlite/src/db.ts';
import {InspectorDelegate} from '../../server/inspector-delegate.ts';
import {DbFile} from '../../test/lite.ts';
import {initReplicationState} from '../replicator/schema/replication-state.ts';
import {
  fakeReplicator,
  ReplicationMessages,
  type FakeReplicator,
} from '../replicator/test-utils.ts';
import {PipelineDriver, type RowChange, type Timer} from './pipeline-driver.ts';
import {Snapshotter} from './snapshotter.ts';

/**
 * A pipeline's rows must not depend on its history, because a client group may be
 * served by a pipeline with any history. This drives random writes through a
 * long-lived pipeline and checks it against a fresh hydration at every version.
 *
 * The exception is the child rows of an EXISTS. They only prove that a parent
 * qualifies, so Cap keeps a few of them and which ones depends on the order they
 * arrived in. Those are checked for validity instead of compared.
 */

const SHARD = {appID: 'this_app', shardNum: 2};
const timer: Timer = {elapsedLap: () => 0, totalElapsed: () => 0};
const messages = new ReplicationMessages({issues: 'id', comments: 'id'});

const SEEDS = Number(process.env['ZERO_TEST_HISTORY_SEEDS'] ?? 8);
const STEPS = Number(process.env['ZERO_TEST_HISTORY_STEPS'] ?? 25);

const CLIENT_SCHEMA: ClientSchema = {
  tables: {
    issues: {
      columns: {id: {type: 'string'}, rank: {type: 'number'}},
      primaryKey: ['id'],
    },
    comments: {
      columns: {
        id: {type: 'string'},
        issueID: {type: 'string'},
        rank: {type: 'number'},
      },
      primaryKey: ['id'],
    },
  },
};

const BY_RANK: AST['orderBy'] = [
  ['rank', 'asc'],
  ['id', 'asc'],
];
const commentsOfIssue = {
  correlation: {
    parentField: ['id'] as CompoundKey,
    childField: ['issueID'] as CompoundKey,
  },
};
const exists = (op: 'EXISTS' | 'NOT EXISTS'): AST['where'] => ({
  type: 'correlatedSubquery',
  op,
  related: {
    ...commentsOfIssue,
    subquery: {
      table: 'comments',
      alias: 'zsubq_comments',
      orderBy: [['id', 'asc']],
    },
  },
});

const WITH_PROOF_ROWS = new Set(['exists', 'existsLimit']);

const QUERIES: Record<string, AST> = {
  all: {table: 'issues', orderBy: BY_RANK},
  limit: {table: 'issues', orderBy: BY_RANK, limit: 3},
  related: {
    table: 'issues',
    orderBy: BY_RANK,
    related: [
      {
        ...commentsOfIssue,
        subquery: {table: 'comments', alias: 'comments', orderBy: BY_RANK},
      },
    ],
  },
  relatedLimit: {
    table: 'issues',
    orderBy: BY_RANK,
    limit: 3,
    related: [
      {
        ...commentsOfIssue,
        subquery: {
          table: 'comments',
          alias: 'comments',
          orderBy: BY_RANK,
          limit: 2,
        },
      },
    ],
  },
  exists: {table: 'issues', orderBy: BY_RANK, where: exists('EXISTS')},
  existsLimit: {
    table: 'issues',
    orderBy: BY_RANK,
    limit: 2,
    where: exists('EXISTS'),
  },
  notExists: {table: 'issues', orderBy: BY_RANK, where: exists('NOT EXISTS')},
  count: {table: 'comments', orderBy: [['id', 'asc']], limit: 4},
};

function mulberry32(seed: number) {
  let a = seed;
  return () => {
    a = (a + 0x6d2b79f5) | 0;
    let t = Math.imul(a ^ (a >>> 15), 1 | a);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

/** The rows a pipeline has delivered: what a client's view would hold. */
class Delivered {
  readonly #counts = new Map<string, number>();
  readonly #rows = new Map<string, unknown>();

  apply(changes: Iterable<RowChange | 'yield'>) {
    for (const change of changes) {
      if (change === 'yield') {
        continue;
      }
      const key = `${change.table}/${JSON.stringify(change.rowKey)}`;
      if (change.type === ChangeType.REMOVE) {
        const count = (this.#counts.get(key) ?? 0) - 1;
        if (count <= 0) {
          this.#counts.delete(key);
          this.#rows.delete(key);
        } else {
          this.#counts.set(key, count);
        }
      } else {
        if (change.type === ChangeType.ADD) {
          this.#counts.set(key, (this.#counts.get(key) ?? 0) + 1);
        }
        const {_0_version: _, ...row} = change.row as Record<string, unknown>;
        this.#rows.set(key, row);
      }
    }
  }

  snapshot(table?: string) {
    return Object.fromEntries(
      [...this.#rows.entries()]
        .filter(([key]) => table === undefined || key.startsWith(`${table}/`))
        .sort(([a], [b]) => (a < b ? -1 : 1)),
    );
  }
}

describe('pipeline rows do not depend on history', () => {
  const lc = createSilentLogContext();
  let dbFile: DbFile;
  let replica: Database;
  let replicator: FakeReplicator;
  let storage: DatabaseStorage;
  let nextGroup: number;

  beforeEach(() => {
    dbFile = new DbFile('pipeline_history');
    replica = dbFile.connect(lc);
    initReplicationState(replica, ['zero_data'], '01');
    replica.pragma('journal_mode = WAL2');
    replica.pragma('busy_timeout = 1');
    replica.exec(`
      CREATE TABLE "this_app.permissions" (lock INT PRIMARY KEY, permissions JSON, hash TEXT, _0_version TEXT NOT NULL);
      INSERT INTO "this_app.permissions" (lock, permissions, hash, _0_version) VALUES (1, NULL, NULL, '01');
      CREATE TABLE issues (id TEXT PRIMARY KEY, rank INTEGER, _0_version TEXT NOT NULL);
      CREATE TABLE comments (id TEXT PRIMARY KEY, issueID TEXT, rank INTEGER, _0_version TEXT NOT NULL);
    `);
    replicator = fakeReplicator(lc, replica);
    const storageDB = new Database(lc, ':memory:');
    storageDB.prepare(CREATE_STORAGE_TABLE).run();
    storage = new DatabaseStorage(storageDB);
    nextGroup = 0;
  });

  afterEach(() => {
    replica.close();
    dbFile.delete();
  });

  function newDriver() {
    const id = `group${nextGroup++}`;
    const driver = new PipelineDriver(
      lc,
      testLogConfig,
      new Snapshotter(lc, dbFile.path, SHARD),
      SHARD,
      storage.createClientGroupStorage(id),
      id,
      new InspectorDelegate(undefined),
      () => 200,
    );
    driver.init(CLIENT_SCHEMA);
    return driver;
  }

  describe.each(Object.keys(QUERIES))('%s', name => {
    test.each(Array.from({length: SEEDS}, (_, i) => i + 1))('seed %i', seed => {
      const ast = QUERIES[name];
      const random = mulberry32(seed);
      const pick = (n: number) => Math.floor(random() * n);
      const issues = new Map<string, number>();
      const comments = new Map<string, {issueID: string; rank: number}>();
      let version = 1;
      let nextID = 0;

      const change = () => {
        const op = pick(7);
        const issueIDs = [...issues.keys()];
        const commentIDs = [...comments.keys()];
        const anyIssue = issueIDs[pick(issueIDs.length)];
        const anyComment = commentIDs[pick(commentIDs.length)];
        if (op === 0 || issueIDs.length === 0) {
          const id = `i${nextID++}`;
          const rank = pick(20);
          issues.set(id, rank);
          return messages.insert('issues', {id, rank});
        }
        if (op === 1 || (op === 2 && issueIDs.length > 0)) {
          const rank = pick(20);
          issues.set(anyIssue, rank);
          return messages.update('issues', {id: anyIssue, rank});
        }
        if (op === 3) {
          issues.delete(anyIssue);
          return messages.delete('issues', {id: anyIssue});
        }
        if (op === 4 || commentIDs.length === 0) {
          const id = `c${nextID++}`;
          const row = {issueID: anyIssue, rank: pick(20)};
          comments.set(id, row);
          return messages.insert('comments', {id, ...row});
        }
        if (op === 5) {
          const row = {issueID: anyIssue, rank: pick(20)};
          comments.set(anyComment, row);
          return messages.update('comments', {id: anyComment, ...row});
        }
        comments.delete(anyComment);
        return messages.delete('comments', {id: anyComment});
      };

      const commit = (count: number) => {
        version++;
        replicator.processTransaction(
          String(version).padStart(4, '0'),
          ...(Array.from({length: count}, change) as never[]),
        );
      };

      commit(12);
      const longLived = newDriver();
      const delivered = new Delivered();
      delivered.apply(longLived.addQuery('h', 'q', ast, timer));
      try {
        for (let step = 0; step < STEPS; step++) {
          commit(1 + pick(3));
          delivered.apply(longLived.advance(timer).changes);

          const fresh = newDriver();
          const expected = new Delivered();
          expected.apply(fresh.addQuery('h', 'q', ast, timer));
          const signature = fresh.rowSetSignature('q');
          fresh.destroy();

          if (WITH_PROOF_ROWS.has(name)) {
            expect(
              delivered.snapshot('issues'),
              `parents at step ${step}`,
            ).toEqual(expected.snapshot('issues'));
            const parents = new Set(
              Object.values(delivered.snapshot('issues')).map(
                row => (row as {id: string}).id,
              ),
            );
            const proven = new Set<string>();
            for (const row of Object.values(delivered.snapshot('comments'))) {
              const {id, issueID, rank} = row as {
                id: string;
                issueID: string;
                rank: number;
              };
              expect(comments.get(id), `proof ${id} at ${step}`).toEqual({
                issueID,
                rank,
              });
              expect(parents.has(issueID), `parent of ${id}`).toBe(true);
              proven.add(issueID);
            }
            expect(proven, `proof for each parent at ${step}`).toEqual(parents);
          } else {
            expect(delivered.snapshot(), `rows at step ${step}`).toEqual(
              expected.snapshot(),
            );
            expect(
              longLived.rowSetSignature('q'),
              `signature at step ${step}`,
            ).toBe(signature);
          }
        }
      } finally {
        longLived.destroy();
      }
    });
  });
});
