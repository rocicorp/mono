import {expect, test} from 'vitest';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  type CheckIndexSpec,
  type CheckTableSpec,
  findMissingRelationshipIndexes,
} from './relationship-indexes.ts';

const tbl = (name: string, primaryKey: string[]): CheckTableSpec => ({
  table: name,
  primaryKey,
});
const idx = (table: string, columns: string[]): CheckIndexSpec => ({
  table,
  columns,
});

test('one-hop many: warns when the foreign-key (dest) side is unindexed', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const comment = table('comment')
    .columns({id: string(), issueID: string()})
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, ({many}) => ({
        comments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });

  const {missing, unsyncedTables} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id']), tbl('comment', ['id'])],
    [],
  );

  // The source side (issue.id) is covered by the primary key; only the
  // dest side (comment.issueID) is missing an index.
  expect(unsyncedTables).toEqual([]);
  expect(missing).toEqual([
    {
      ownerTable: 'issue',
      relationship: 'comments',
      hop: 1,
      hopCount: 1,
      side: 'dest',
      cardinality: 'many',
      clientTable: 'comment',
      clientColumns: ['issueID'],
      serverTable: 'comment',
      serverColumns: ['issueID'],
      createIndexSQL: 'CREATE INDEX ON comment ("issueID");',
    },
  ]);
});

test('one-hop many: no warning once the dest field is indexed', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const comment = table('comment')
    .columns({id: string(), issueID: string()})
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, ({many}) => ({
        comments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });

  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id']), tbl('comment', ['id'])],
    [idx('comment', ['issueID'])],
  );

  expect(missing).toEqual([]);
});

test('one-hop one: warns when the source foreign-key side is unindexed', () => {
  const issue = table('issue')
    .columns({id: string(), projectID: string()})
    .primaryKey('id');
  const project = table('project').columns({id: string()}).primaryKey('id');
  const schema = createSchema({
    tables: [issue, project],
    relationships: [
      relationships(issue, ({one}) => ({
        project: one({
          sourceField: ['projectID'],
          destField: ['id'],
          destSchema: project,
        }),
      })),
    ],
  });

  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id']), tbl('project', ['id'])],
    [],
  );

  // The dest side (project.id) is the primary key; the source foreign key
  // (issue.projectID) is the one that needs an index.
  expect(missing).toMatchObject([
    {
      relationship: 'project',
      side: 'source',
      cardinality: 'one',
      clientTable: 'issue',
      serverTable: 'issue',
      serverColumns: ['projectID'],
      createIndexSQL: 'CREATE INDEX ON issue ("projectID");',
    },
  ]);
});

test('junction relationship: checks join fields across both hops', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const label = table('label').columns({id: string()}).primaryKey('id');
  const issueLabel = table('issueLabel')
    .columns({issueID: string(), labelID: string()})
    .primaryKey('issueID', 'labelID');
  const schema = createSchema({
    tables: [issue, label, issueLabel],
    relationships: [
      relationships(issue, ({many}) => ({
        labels: many(
          {sourceField: ['id'], destField: ['issueID'], destSchema: issueLabel},
          {sourceField: ['labelID'], destField: ['id'], destSchema: label},
        ),
      })),
    ],
  });

  // issueLabel's primary key is (issueID, labelID). Its leading column is
  // issueID, so issueLabel.issueID (hop 1 dest) is covered, but
  // issueLabel.labelID (hop 2 source) is not. issue.id and label.id are
  // both primary keys.
  const {missing} = findMissingRelationshipIndexes(
    schema,
    [
      tbl('issue', ['id']),
      tbl('label', ['id']),
      tbl('issueLabel', ['issueID', 'labelID']),
    ],
    [],
  );

  expect(missing).toEqual([
    {
      ownerTable: 'issue',
      relationship: 'labels',
      hop: 2,
      hopCount: 2,
      side: 'source',
      cardinality: 'many',
      clientTable: 'issueLabel',
      clientColumns: ['labelID'],
      serverTable: 'issueLabel',
      serverColumns: ['labelID'],
      createIndexSQL: 'CREATE INDEX ON "issueLabel" ("labelID");',
    },
  ]);
});

test('uses server names for the table and columns to index', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const comment = table('comment')
    .from('comments')
    .columns({id: string(), issueID: string().from('issue_id')})
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, ({many}) => ({
        comments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });

  // Published tables/indexes are keyed by server names.
  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id']), tbl('comments', ['id'])],
    [],
  );

  expect(missing).toMatchObject([
    {
      ownerTable: 'issue',
      side: 'dest',
      clientTable: 'comment',
      clientColumns: ['issueID'],
      serverTable: 'comments',
      serverColumns: ['issue_id'],
      createIndexSQL: 'CREATE INDEX ON comments (issue_id);',
    },
  ]);
});

test('schema-qualifies the suggested index for non-public tables', () => {
  const event = table('event')
    .from('analytics.events')
    .columns({id: string(), userID: string()})
    .primaryKey('id');
  const user = table('user').columns({id: string()}).primaryKey('id');
  const schema = createSchema({
    tables: [event, user],
    relationships: [
      relationships(event, ({one}) => ({
        user: one({
          sourceField: ['userID'],
          destField: ['id'],
          destSchema: user,
        }),
      })),
    ],
  });

  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('analytics.events', ['id']), tbl('user', ['id'])],
    [],
  );

  expect(missing).toMatchObject([
    {
      side: 'source',
      clientTable: 'event',
      serverTable: 'analytics.events',
      serverColumns: ['userID'],
      createIndexSQL: 'CREATE INDEX ON analytics.events ("userID");',
    },
  ]);
});

test('composite join keys are covered only when both are leading columns', () => {
  const a = table('a')
    .columns({id: string(), k1: string(), k2: string()})
    .primaryKey('id');
  const b = table('b')
    .columns({id: string(), c1: string(), c2: string()})
    .primaryKey('id');
  const schema = createSchema({
    tables: [a, b],
    relationships: [
      relationships(a, ({many}) => ({
        bs: many({
          sourceField: ['k1', 'k2'],
          destField: ['c1', 'c2'],
          destSchema: b,
        }),
      })),
    ],
  });

  // b(c2, c1) covers the dest lookup [c1, c2] (leading columns, order among
  // equality columns doesn't matter), but a(k1) does NOT cover [k1, k2].
  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('a', ['id']), tbl('b', ['id'])],
    [idx('a', ['k1']), idx('b', ['c2', 'c1'])],
  );

  expect(missing).toMatchObject([
    {
      side: 'source',
      serverTable: 'a',
      serverColumns: ['k1', 'k2'],
      createIndexSQL: 'CREATE INDEX ON a (k1, k2);',
    },
  ]);
});

test('deduplicates suggested CREATE INDEX statements', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const comment = table('comment')
    .columns({id: string(), issueID: string()})
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, ({many}) => ({
        comments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
        // A second relationship that joins on the same field.
        openComments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });

  const {missing} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id']), tbl('comment', ['id'])],
    [],
  );

  expect(missing).toHaveLength(2);
  expect(new Set(missing.map(m => m.createIndexSQL))).toEqual(
    new Set(['CREATE INDEX ON comment ("issueID");']),
  );
});

test('reports tables referenced by relationships that are not synced', () => {
  const issue = table('issue').columns({id: string()}).primaryKey('id');
  const comment = table('comment')
    .columns({id: string(), issueID: string()})
    .primaryKey('id');
  const schema = createSchema({
    tables: [issue, comment],
    relationships: [
      relationships(issue, ({many}) => ({
        comments: many({
          sourceField: ['id'],
          destField: ['issueID'],
          destSchema: comment,
        }),
      })),
    ],
  });

  // Only `issue` is published; `comment` is not synced, so its index can't
  // be checked.
  const {missing, unsyncedTables} = findMissingRelationshipIndexes(
    schema,
    [tbl('issue', ['id'])],
    [],
  );

  expect(unsyncedTables).toEqual(['comment']);
  expect(missing).toEqual([]);
});
