/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-types */
import {expectTypeOf, test} from 'vitest';
import {relationships} from './builder/relationship-builder.ts';
import {createSchema} from './builder/schema-builder.ts';
import {number, string, table} from './builder/table-builder.ts';
import type {Relationship, TableSchema} from './table-schema.ts';

test('relationship schema types', () => {
  const issueLabel = table('issueLabel')
    .columns({
      id: number(),
      issueID: number(),
      labelID: number(),
    })
    .primaryKey('id');

  const comment = table('comment')
    .columns({
      id: number(),
      issueID: number(),
      body: string(),
    })
    .primaryKey('id');

  const label = table('label')
    .columns({
      id: number(),
      issueID: number(),
      name: string(),
    })
    .primaryKey('id');

  const issue = table('issue')
    .columns({
      id: number(),
      title: string(),
      body: string(),
    })
    .primaryKey('id');

  const issueRelationships = relationships(issue, connect => ({
    comments: connect.many({
      sourceField: ['id'],
      destField: ['issueID'],
      destSchema: comment,
    }),
    labels: connect.many(
      {
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: issueLabel,
      },
      {
        sourceField: ['labelID'],
        destField: ['id'],
        destSchema: label,
      },
    ),
  }));

  const schema = createSchema({
    tables: [issueLabel, comment, label, issue],
    relationships: [issueRelationships],
  });

  expectTypeOf(schema.tables.issueLabel).toExtend<TableSchema>();

  expectTypeOf(schema.tables.comment).toExtend<TableSchema>();

  expectTypeOf(schema.tables.label).toExtend<TableSchema>();

  expectTypeOf(schema.tables.issue).toExtend<TableSchema>();

  expectTypeOf(schema.relationships.issue.comments).toExtend<Relationship>();

  expectTypeOf(schema.relationships.issue.labels).toExtend<Relationship>();
});
