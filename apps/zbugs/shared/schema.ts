import {
  ANYONE_CAN,
  boolean,
  createSchema,
  definePermissions,
  enumeration,
  number,
  relationships,
  string,
  table,
  type ExpressionBuilder,
  type Row,
  query,
} from '@rocicorp/zero';
import type {AuthData, Role} from './auth.ts';

// Table definitions
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string(),
    role: enumeration<Role>(),
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
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .columns({
    issueID: string(),
    labelID: string(),
  })
  .primaryKey('issueID', 'labelID');

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

// Relationships
const userRelationships = relationships(user, ({many}) => ({
  createdIssues: many({
    sourceField: ['id'],
    destField: ['creatorID'],
    destSchema: issue,
  }),
}));

const issueRelationships = relationships(issue, ({many, one}) => ({
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
  tables: [user, issue, comment, label, issueLabel, viewState, emoji, userPref],
  relationships: [
    userRelationships,
    issueRelationships,
    commentRelationships,
    issueLabelRelationships,
    emojiRelationships,
  ],
});

export type Schema = typeof schema;
type TableName = keyof Schema['tables'];

export type IssueRow = Row<typeof schema.tables.issue>;
export type CommentRow = Row<typeof schema.tables.comment>;
export type UserRow = Row<typeof schema.tables.user>;

export const queries = query(schema);

export const permissions: ReturnType<typeof definePermissions> =
  definePermissions<AuthData, Schema>(schema, () => {
    const userIsLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<Schema, TableName>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const loggedInUserIsAdmin = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, TableName>,
    ) =>
      eb.and(
        userIsLoggedIn(authData, eb),
        eb.cmpLit(authData.role, '=', 'crew'),
      );

    const allowIfUserIDMatchesLoggedInUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<Schema, 'viewState' | 'userPref'>,
    ) => cmp('userID', '=', authData.sub);

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'issue'>,
    ) =>
      eb.or(loggedInUserIsAdmin(authData, eb), eb.cmp('visibility', 'public'));

    return {
      user: {
        row: {
          select: ANYONE_CAN,
        },
      },
      issue: {
        row: {
          select: [canSeeIssue],
        },
      },
      comment: {
        row: {
          select: ANYONE_CAN,
        },
      },
      label: {
        row: {
          select: ANYONE_CAN,
        },
      },
      viewState: {
        row: {
          select: ANYONE_CAN,
        },
      },
      issueLabel: {
        row: {
          select: ANYONE_CAN,
        },
      },
      emoji: {
        row: {
          select: ANYONE_CAN,
        },
      },
      userPref: {
        row: {
          select: [allowIfUserIDMatchesLoggedInUser],
        },
      },
    };
  });
