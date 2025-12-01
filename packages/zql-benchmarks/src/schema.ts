import {relationships} from '../../zero-schema/src/builder/relationship-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import {
  boolean,
  enumeration,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {createBuilder} from '../../zql/src/query/create-builder.ts';
import type {Row} from '../../zql/src/query/query.ts';

// Table definitions
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string(),
    role: enumeration<'crew' | 'user'>(),
  })
  .primaryKey('id');

const project = table('project')
  .columns({
    id: string(),
    name: string(),
    lowerCaseName: string(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    shortID: number().optional(),
    title: string(),
    open: boolean(),
    modified: number(),
    created: number(),
    projectID: string(),
    creatorID: string(),
    assigneeID: string().optional(),
    description: string(),
    visibility: enumeration<'internal' | 'public'>(),
  })
  .primaryKey('id');

const viewState = table('viewState')
  .columns({
    issueID: string(),
    userID: string(),
    viewed: number(),
  })
  .primaryKey('userID', 'issueID');

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
    projectID: string(),
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .columns({
    issueID: string(),
    labelID: string(),
    projectID: string(),
  })
  .primaryKey('labelID', 'issueID');

const emoji = table('emoji')
  .columns({
    id: string(),
    value: string(),
    annotation: string(),
    subjectID: string(),
    creatorID: string(),
    created: number(),
  })
  .primaryKey('id');

const userPref = table('userPref')
  .columns({
    key: string(),
    userID: string(),
    value: string(),
  })
  .primaryKey('userID', 'key');

const issueNotifications = table('issueNotifications')
  .columns({
    userID: string(),
    issueID: string(),
    subscribed: boolean(),
    created: number(),
  })
  .primaryKey('userID', 'issueID');

// Relationships
const userRelationships = relationships(user, ({many}) => ({
  createdIssues: many({
    sourceField: ['id'],
    destField: ['creatorID'],
    destSchema: issue,
  }),
  assignedIssues: many({
    sourceField: ['id'],
    destField: ['assigneeID'],
    destSchema: issue,
  }),
}));

const projectRelationships = relationships(project, ({many}) => ({
  issues: many({
    sourceField: ['id'],
    destField: ['projectID'],
    destSchema: issue,
  }),
  labels: many({
    sourceField: ['id'],
    destField: ['projectID'],
    destSchema: label,
  }),
}));

const issueRelationships = relationships(issue, ({many, one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
  labels: many(
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
  comments: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: comment,
  }),
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  assignee: one({
    sourceField: ['assigneeID'],
    destField: ['id'],
    destSchema: user,
  }),
  viewState: many({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: viewState,
  }),
  emoji: many({
    sourceField: ['id'],
    destField: ['subjectID'],
    destSchema: emoji,
  }),
  notificationState: one({
    sourceField: ['id'],
    destField: ['issueID'],
    destSchema: issueNotifications,
  }),
}));

const commentRelationships = relationships(comment, ({one, many}) => ({
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  emoji: many({
    sourceField: ['id'],
    destField: ['subjectID'],
    destSchema: emoji,
  }),
  issue: one({
    sourceField: ['issueID'],
    destField: ['id'],
    destSchema: issue,
  }),
}));

const issueLabelRelationships = relationships(issueLabel, ({one}) => ({
  issue: one({
    sourceField: ['issueID'],
    destField: ['id'],
    destSchema: issue,
  }),
}));

const labelRelationships = relationships(label, ({one}) => ({
  project: one({
    sourceField: ['projectID'],
    destField: ['id'],
    destSchema: project,
  }),
}));

const emojiRelationships = relationships(emoji, ({one}) => ({
  creator: one({
    sourceField: ['creatorID'],
    destField: ['id'],
    destSchema: user,
  }),
  issue: one({
    sourceField: ['subjectID'],
    destField: ['id'],
    destSchema: issue,
  }),
  comment: one({
    sourceField: ['subjectID'],
    destField: ['id'],
    destSchema: comment,
  }),
}));

export const schema = createSchema({
  tables: [
    user,
    project,
    issue,
    comment,
    label,
    issueLabel,
    viewState,
    emoji,
    userPref,
    issueNotifications,
  ],
  relationships: [
    userRelationships,
    projectRelationships,
    issueRelationships,
    commentRelationships,
    issueLabelRelationships,
    labelRelationships,
    emojiRelationships,
  ],
  enableLegacyMutators: false,
  enableLegacyQueries: false,
});

export type Schema = typeof schema;

export type IssueRow = Row<typeof schema.tables.issue>;
export type CommentRow = Row<typeof schema.tables.comment>;
export type UserRow = Row<typeof schema.tables.user>;
export type ProjectRow = Row<typeof schema.tables.project>;

export const builder = createBuilder(schema);
