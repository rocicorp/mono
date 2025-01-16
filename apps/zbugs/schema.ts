import {
  boolean,
  createSchema,
  definePermissions,
  NOBODY_CAN,
  number,
  relationships,
  string,
  table,
  type ExpressionBuilder,
  type Row,
} from '@rocicorp/zero';
import type {Condition} from 'zero-protocol/src/ast.js';

// Table definitions
const user = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string().optional(),
    avatar: string(),
    role: string(),
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
    visibility: string(),
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

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
  role: 'crew' | 'user';
};

export const schema = createSchema(
  5,
  {
    user,
    issue,
    comment,
    label,
    issueLabel,
    viewState,
    emoji,
    userPref,
  },
  {
    userRelationships,
    issueRelationships,
    commentRelationships,
    issueLabelRelationships,
    emojiRelationships,
  },
);

export type Schema = typeof schema;
type TableName = keyof Schema['tables'];

export type IssueRow = Row<typeof schema.tables.issue>;
export type CommentRow = Row<typeof schema.tables.comment>;
export type UserRow = Row<typeof schema.tables.user>;

type PermissionRule<TTable extends TableName> = (
  authData: AuthData,
  eb: ExpressionBuilder<Schema, TTable>,
) => Condition;

function and<TTable extends TableName>(
  ...rules: PermissionRule<TTable>[]
): PermissionRule<TTable> {
  return (authData, eb) => eb.and(...rules.map(rule => rule(authData, eb)));
}

export const permissions: ReturnType<typeof definePermissions> =
  definePermissions<AuthData, Schema>(schema, () => {
    const userIsLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<Schema, TableName>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const loggedInUserIsCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'comment' | 'emoji' | 'issue'>,
    ) =>
      eb.and(
        userIsLoggedIn(authData, eb),
        eb.cmp('creatorID', '=', authData.sub),
      );

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

    const allowIfAdminOrIssueCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'issueLabel'>,
    ) =>
      eb.or(
        loggedInUserIsAdmin(authData, eb),
        eb.exists('issue', iq =>
          iq.where(eb => loggedInUserIsCreator(authData, eb)),
        ),
      );

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'issue'>,
    ) =>
      eb.or(loggedInUserIsAdmin(authData, eb), eb.cmp('visibility', 'public'));

    /**
     * Comments are only visible if the user can see the issue they're attached to.
     */
    const canSeeComment = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'comment'>,
    ) => eb.exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    /**
     * Issue labels are only visible if the user can see the issue they're attached to.
     */
    const canSeeIssueLabel = (
      authData: AuthData,
      eb: ExpressionBuilder<Schema, 'issueLabel'>,
    ) => eb.exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    /**
     * Emoji are only visible if the user can see the issue they're attached to.
     */
    const canSeeEmoji = (
      authData: AuthData,
      {exists, or}: ExpressionBuilder<Schema, 'emoji'>,
    ) =>
      or(
        exists('issue', q => {
          return q.where(eb => canSeeIssue(authData, eb));
        }),
        exists('comment', q => {
          return q.where(eb => canSeeComment(authData, eb));
        }),
      );

    return {
      user: {
        // Only the authentication system can write to the user table.
        row: {
          insert: NOBODY_CAN,
          update: {
            preMutation: NOBODY_CAN,
          },
          delete: NOBODY_CAN,
        },
      },
      issue: {
        row: {
          insert: [
            // prevents setting the creatorID of an issue to someone
            // other than the user doing the creating
            loggedInUserIsCreator,
          ],
          update: {
            preMutation: [loggedInUserIsCreator, loggedInUserIsAdmin],
            postMutation: [loggedInUserIsCreator, loggedInUserIsAdmin],
          },
          delete: [loggedInUserIsCreator, loggedInUserIsAdmin],
          select: [canSeeIssue],
        },
      },
      comment: {
        row: {
          insert: [
            loggedInUserIsAdmin,
            and(loggedInUserIsCreator, canSeeComment),
          ],
          update: {
            preMutation: [
              loggedInUserIsAdmin,
              and(loggedInUserIsCreator, canSeeComment),
            ],
          },
          delete: [
            loggedInUserIsAdmin,
            and(canSeeComment, loggedInUserIsCreator),
          ],
        },
      },
      label: {
        row: {
          insert: [loggedInUserIsAdmin],
          update: {
            preMutation: [loggedInUserIsAdmin],
          },
          delete: [loggedInUserIsAdmin],
        },
      },
      viewState: {
        row: {
          insert: [allowIfUserIDMatchesLoggedInUser],
          update: {
            preMutation: [allowIfUserIDMatchesLoggedInUser],
            postMutation: [allowIfUserIDMatchesLoggedInUser],
          },
          delete: NOBODY_CAN,
        },
      },
      issueLabel: {
        row: {
          insert: [and(canSeeIssueLabel, allowIfAdminOrIssueCreator)],
          update: {
            preMutation: NOBODY_CAN,
          },
          delete: [and(canSeeIssueLabel, allowIfAdminOrIssueCreator)],
        },
      },
      emoji: {
        row: {
          // Can only insert emoji if the can see the issue.
          insert: [and(canSeeEmoji, loggedInUserIsCreator)],

          // Can only update their own emoji.
          update: {
            preMutation: [and(canSeeEmoji, loggedInUserIsCreator)],
            postMutation: [and(canSeeEmoji, loggedInUserIsCreator)],
          },
          delete: [and(canSeeEmoji, loggedInUserIsCreator)],
        },
      },
      userPref: {
        row: {
          insert: [allowIfUserIDMatchesLoggedInUser],
          update: {
            preMutation: [allowIfUserIDMatchesLoggedInUser],
            postMutation: [allowIfUserIDMatchesLoggedInUser],
          },
          delete: [allowIfUserIDMatchesLoggedInUser],
        },
      },
    };
  });
