import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  json,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.ts';
import type {Row} from '../query.ts';

const issue = table('issue')
  .from('issues')
  .columns({
    id: string(),
    title: string(),
    description: string(),
    closed: boolean(),
    ownerId: string().from('owner_id').optional(),
    createdAt: number(),
  })
  .primaryKey('id');

const user = table('user')
  .from('users')
  .columns({
    id: string(),
    name: string(),
    metadata: json<{
      registrar: 'github' | 'google';
      email: string;
      altContacts?: string[];
    }>().optional(),
  })
  .primaryKey('id');

const comment = table('comment')
  .from('comments')
  .columns({
    id: string(),
    authorId: string(),
    issueId: string().from('issue_id'),
    text: string(),
    createdAt: number(),
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .columns({
    issueId: string(),
    labelId: string(),
  })
  .primaryKey('issueId', 'labelId');

const label = table('label')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const revision = table('revision')
  .columns({
    id: string(),
    authorId: string(),
    commentId: string(),
    text: string(),
  })
  .primaryKey('id');

const issueRelationships = relationships(issue, ({one, many}) => ({
  owner: one({
    sourceField: ['ownerId'],
    destField: ['id'],
    destSchema: user,
  }),
  comments: many({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
  oneComment: one({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
  labels: many(
    {
      sourceField: ['id'],
      destField: ['issueId'],
      destSchema: issueLabel,
    },
    {
      sourceField: ['labelId'],
      destField: ['id'],
      destSchema: label,
    },
  ),
}));

const userRelationships = relationships(user, ({many}) => ({
  issues: many({
    sourceField: ['id'],
    destField: ['ownerId'],
    destSchema: issue,
  }),
}));

const commentRelationships = relationships(comment, ({one, many}) => ({
  issue: one({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
  revisions: many({
    sourceField: ['id'],
    destField: ['commentId'],
    destSchema: revision,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const revisionRelationships = relationships(revision, ({one}) => ({
  comment: one({
    sourceField: ['commentId'],
    destField: ['id'],
    destSchema: comment,
  }),
  author: one({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const labelRelationships = relationships(label, ({many}) => ({
  issues: many(
    {
      sourceField: ['id'],
      destField: ['labelId'],
      destSchema: issueLabel,
    },
    {
      sourceField: ['issueId'],
      destField: ['id'],
      destSchema: issue,
    },
  ),
}));

export const schemaOptions = {
  tables: [issue, user, comment, revision, label, issueLabel],
  relationships: [
    issueRelationships,
    userRelationships,
    commentRelationships,
    revisionRelationships,
    labelRelationships,
  ],
};

export const schema = createSchema(schemaOptions);

export const issueSchema = schema.tables.issue;
export const commentSchema = schema.tables.comment;
export const issueLabelSchema = schema.tables.issueLabel;
export const labelSchema = schema.tables.label;
export const revisionSchema = schema.tables.revision;
export const userSchema = schema.tables.user;

export type Issue = Row<typeof issueSchema>;
export type Comment = Row<typeof commentSchema>;
export type IssueLabel = Row<typeof issueLabelSchema>;
export type Label = Row<typeof labelSchema>;
export type Revision = Row<typeof revisionSchema>;
export type User = Row<typeof userSchema>;

export const createTableSQL = /*sql*/ `
CREATE TABLE IF NOT EXISTS "issues" (
  "id" TEXT PRIMARY KEY,
  "title" TEXT NOT NULL,
  "description" TEXT NOT NULL,
  "closed" BOOLEAN NOT NULL,
  "owner_id" TEXT,
  "createdAt" TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS "users" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL,
  "metadata" JSONB
);

CREATE TABLE IF NOT EXISTS "comments" (
  "id" TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  "issue_id" TEXT NOT NULL,
  "text" TEXT NOT NULL,
  -- not TIMESTAMPTZ so that we're checking both TIMESTAMP and TIMESTAMPTZ behaviour
  "createdAt" TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS "issueLabel" (
  "issueId" TEXT NOT NULL,
  "labelId" TEXT NOT NULL,
  PRIMARY KEY ("issueId", "labelId")
);

CREATE TABLE IF NOT EXISTS "label" (
  "id" TEXT PRIMARY KEY,
  "name" TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS "revision" (
  "id" TEXT PRIMARY KEY,
  "authorId" TEXT NOT NULL,
  "commentId" TEXT NOT NULL,
  "text" TEXT NOT NULL
);
`;
