/* eslint-disable @typescript-eslint/no-explicit-any */
/* eslint-disable @typescript-eslint/ban-types */
import {expectTypeOf, test} from 'vitest';
import type {
  FieldRelationship,
  JunctionRelationship,
  Relationship,
  TableSchema,
} from './table-schema.js';

test('relationship schema types', () => {
  const issueLabelSchema = {
    tableName: 'issueLabel',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      issueID: {type: 'number'},
      labelID: {type: 'number'},
    },
    relationships: {},
  } as const;

  const commentSchema = {
    tableName: 'comment',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      issueID: {type: 'number'},
      body: {type: 'string'},
    },
    relationships: {},
  } as const;

  const labelSchema = {
    tableName: 'label',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      issueID: {type: 'number'},
      name: {type: 'string'},
    },
    relationships: {},
  } as const;

  const issueSchema = {
    tableName: 'issue',
    primaryKey: ['id'],
    columns: {
      id: {type: 'number'},
      title: {type: 'string'},
      body: {type: 'string'},
    },
    relationships: {
      comments: {
        correlation: [['id', 'issueID']],
        destSchema: commentSchema,
      },
      labels: {
        correlation: [['id', 'id']],
        destSchema: () => labelSchema,
        junction: {
          correlation: [['issueID', 'labelID']],
          destSchema: issueLabelSchema,
        },
      },
    },
  } as const;

  expectTypeOf(issueLabelSchema).toMatchTypeOf<TableSchema>();
  type IssueLabel = typeof issueLabelSchema;

  expectTypeOf(commentSchema).toMatchTypeOf<TableSchema>();
  type Comment = typeof commentSchema;

  expectTypeOf(labelSchema).toMatchTypeOf<TableSchema>();
  type Label = typeof labelSchema;

  expectTypeOf(issueSchema).toMatchTypeOf<TableSchema>();
  type Issue = typeof issueSchema;

  expectTypeOf(
    issueSchema.relationships.comments,
  ).toMatchTypeOf<Relationship>();
  expectTypeOf(issueSchema.relationships.comments).toMatchTypeOf<
    FieldRelationship<Issue, Comment>
  >();
  expectTypeOf(issueSchema.relationships.comments).not.toMatchTypeOf<
    JunctionRelationship<Issue, any, Comment>
  >();

  expectTypeOf(issueSchema.relationships.labels).toMatchTypeOf<Relationship>();
  expectTypeOf(issueSchema.relationships.labels).toMatchTypeOf<
    FieldRelationship<Issue, Label>
  >();
  expectTypeOf(issueSchema.relationships.labels.junction).toMatchTypeOf<
    FieldRelationship<IssueLabel, IssueLabel>
  >();

  expectTypeOf(issueSchema.relationships.labels).toMatchTypeOf<
    JunctionRelationship<Issue, IssueLabel, Label>
  >();
});
