import {relationships} from '../../zero-schema/src/builder/relationship-builder.js';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.js';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.js';

const member = table('member')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    priority: number(),
    status: number(),
    modified: number(),
    created: number(),
    creatorID: string(),
    kanbanOrder: string(),
    description: string(),
  })
  .primaryKey('id');

const comment = table('comment')
  .columns({
    id: string(),
    issueID: string(),
    created: number(),
    body: string(),
    creatorID: string(),
  })
  .primaryKey('id');

const label = table('label')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .columns({
    id: string(),
    issueID: string(),
    labelID: string(),
  })
  .primaryKey('labelID', 'issueID');

// Relationships
const issueRelationships = relationships(issue, connect => ({
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
  comments: connect.many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: comment,
  }),
  creator: connect.many({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: member,
  }),
}));

const commentRelationships = relationships(comment, connect => ({
  creator: connect.many({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: member,
  }),
}));

export const schema = createSchema(
  1,
  {
    member,
    issue,
    comment,
    label,
    issueLabel,
  },
  {
    issueRelationships,
    commentRelationships,
  },
);

type AppSchema = typeof schema;
export type {AppSchema as Schema};
