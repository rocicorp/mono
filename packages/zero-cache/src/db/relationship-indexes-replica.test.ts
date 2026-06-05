import {expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import type {IndexRequirement} from '../../../zero-protocol/src/inspect-up.ts';
import {Database} from '../../../zqlite/src/db.ts';
import {findMissingIndexesInReplica} from './relationship-indexes-replica.ts';

function req(
  overrides: Partial<IndexRequirement> & {
    clientTable: string;
    serverTable: string;
    serverColumns: string[];
  },
): IndexRequirement {
  return {
    ownerTable: 'issue',
    relationship: 'comments',
    hop: 1,
    hopCount: 1,
    side: 'dest',
    cardinality: 'many',
    clientColumns: overrides.serverColumns,
    ...overrides,
  };
}

function db(setup: string): Database {
  const d = new Database(createSilentLogContext(), ':memory:');
  d.exec(setup);
  return d;
}

test('detects a missing index on a foreign key in the replica', () => {
  const requirements: IndexRequirement[] = [
    // issue.id is the primary key — covered (via listTables, even though a
    // non-INTEGER PK is an implicit auto-index not listed by listIndexes).
    req({
      side: 'source',
      clientTable: 'issue',
      serverTable: 'issue',
      serverColumns: ['id'],
    }),
    // comment.issueID has no index — missing.
    req({
      clientTable: 'comment',
      serverTable: 'comment',
      serverColumns: ['issueID'],
    }),
  ];

  const {missing, unsyncedTables} = findMissingIndexesInReplica(
    db(`
      CREATE TABLE issue (id TEXT PRIMARY KEY);
      CREATE TABLE comment (id TEXT PRIMARY KEY, "issueID" TEXT);
    `),
    requirements,
  );

  expect(unsyncedTables).toEqual([]);
  expect(missing).toMatchObject([
    {
      serverTable: 'comment',
      serverColumns: ['issueID'],
      createIndexSQL: 'CREATE INDEX ON comment ("issueID");',
    },
  ]);
  expect(missing).toHaveLength(1);
});

test('a secondary index in the replica covers the requirement', () => {
  const {missing} = findMissingIndexesInReplica(
    db(`
      CREATE TABLE comment (id TEXT PRIMARY KEY, "issueID" TEXT);
      CREATE INDEX comment_issue ON comment ("issueID");
    `),
    [
      req({
        clientTable: 'comment',
        serverTable: 'comment',
        serverColumns: ['issueID'],
      }),
    ],
  );

  expect(missing).toEqual([]);
});

test('reports requirements for tables absent from the replica as unsynced', () => {
  const {missing, unsyncedTables} = findMissingIndexesInReplica(
    db(`CREATE TABLE issue (id TEXT PRIMARY KEY);`),
    [
      req({
        clientTable: 'comment',
        serverTable: 'comment',
        serverColumns: ['issueID'],
      }),
    ],
  );

  expect(missing).toEqual([]);
  expect(unsyncedTables).toEqual(['comment']);
});
