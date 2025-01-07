import {
  boolean,
  json,
  number,
  string,
  table,
} from '../../../../zero-schema/src/builder/table-builder.js';
import {relationships} from '../../../../zero-schema/src/builder/relationship-builder.js';
import {createSchema} from '../../../../zero-schema/src/builder/schema-builder.js';

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    description: string(),
    closed: boolean(),
    ownerId: string().optional(),
  })
  .primaryKey('id');

const user = table('user')
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
  .columns({
    id: string(),
    authorId: string(),
    issueId: string(),
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

const issueRelationships = relationships(issue, connect => ({
  owner: connect({
    sourceField: ['ownerId'],
    destField: ['id'],
    destSchema: user,
  }),
  comments: connect({
    sourceField: ['id'],
    destField: ['issueId'],
    destSchema: comment,
  }),
  labels: connect(
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

const userRelationships = relationships(user, connect => ({
  issues: connect({
    sourceField: ['id'],
    destField: ['ownerId'],
    destSchema: issue,
  }),
}));

const commentRelationships = relationships(comment, connect => ({
  issue: connect({
    sourceField: ['issueId'],
    destField: ['id'],
    destSchema: issue,
  }),
  revisions: connect({
    sourceField: ['id'],
    destField: ['commentId'],
    destSchema: revision,
  }),
  author: connect({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const revisionRelationships = relationships(revision, connect => ({
  comment: connect({
    sourceField: ['commentId'],
    destField: ['id'],
    destSchema: comment,
  }),
  author: connect({
    sourceField: ['authorId'],
    destField: ['id'],
    destSchema: user,
  }),
}));

const labelRelationships = relationships(label, connect => ({
  issues: connect(
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

export const schema = createSchema(
  1,
  {
    issue,
    user,
    comment,
    revision,
    label,
    issueLabel,
  },
  {
    issueRelationships,
    userRelationships,
    commentRelationships,
    revisionRelationships,
    labelRelationships,
  },
);

export const issueSchema = schema.tables.issue;
export const commentSchema = schema.tables.comment;
export const issueLabelSchema = schema.tables.issueLabel;
export const labelSchema = schema.tables.label;
export const revisionSchema = schema.tables.revision;
export const userSchema = schema.tables.user;
