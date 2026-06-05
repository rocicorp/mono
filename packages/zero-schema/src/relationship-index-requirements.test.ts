import {expect, test} from 'vitest';
import {relationships} from './builder/relationship-builder.ts';
import {createSchema} from './builder/schema-builder.ts';
import {string, table} from './builder/table-builder.ts';
import {enumerateRelationshipIndexRequirements} from './relationship-index-requirements.ts';

test('one-hop relationship emits both source and dest requirements', () => {
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

  expect(enumerateRelationshipIndexRequirements(schema)).toEqual([
    {
      ownerTable: 'issue',
      relationship: 'comments',
      hop: 1,
      hopCount: 1,
      side: 'source',
      cardinality: 'many',
      clientTable: 'issue',
      clientColumns: ['id'],
      serverTable: 'issue',
      serverColumns: ['id'],
    },
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
    },
  ]);
});

test('junction relationship emits requirements for both hops', () => {
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

  const reqs = enumerateRelationshipIndexRequirements(schema);
  expect(reqs).toMatchObject([
    {hop: 1, side: 'source', clientTable: 'issue', serverColumns: ['id']},
    {
      hop: 1,
      side: 'dest',
      clientTable: 'issueLabel',
      serverColumns: ['issueID'],
    },
    {
      hop: 2,
      side: 'source',
      clientTable: 'issueLabel',
      serverColumns: ['labelID'],
    },
    {hop: 2, side: 'dest', clientTable: 'label', serverColumns: ['id']},
  ]);
  expect(
    reqs.every(
      r =>
        r.hopCount === 2 &&
        r.ownerTable === 'issue' &&
        r.relationship === 'labels',
    ),
  ).toBe(true);
});

test('maps client names to server names', () => {
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

  const dest = enumerateRelationshipIndexRequirements(schema).find(
    r => r.side === 'dest',
  );
  expect(dest).toMatchObject({
    clientTable: 'comment',
    clientColumns: ['issueID'],
    serverTable: 'comments',
    serverColumns: ['issue_id'],
  });
});
