import {
  createSchema,
  createTableSchema,
  definePermissions,
  type ExpressionBuilder,
  type TableSchema,
  type Row,
} from '@rocicorp/zero';
import type {Condition} from 'zero-protocol/src/ast.js';

const userSchema = createTableSchema({
  tableName: 'user',
  columns: {
    id: 'string',
    login: 'string',
    name: 'string',
    avatar: 'string',
    role: 'string',
  },
  primaryKey: 'id',
});

const issueSchema = {
  tableName: 'issue',
  columns: {
    id: 'string',
    shortID: {type: 'number', optional: true},
    title: 'string',
    open: 'boolean',
    modified: 'number',
    created: 'number',
    creatorID: 'string',
    assigneeID: {type: 'string', optional: true},
    description: 'string',
    visibility: {type: 'string'},
  },
  primaryKey: 'id',
  relationships: {
    labels: [
      {
        sourceField: 'id',
        destField: 'issueID',
        destSchema: () => issueLabelSchema,
      },
      {
        sourceField: 'labelID',
        destField: 'id',
        destSchema: () => labelSchema,
      },
    ],
    comments: {
      sourceField: 'id',
      destField: 'issueID',
      destSchema: () => commentSchema,
    },
    creator: {
      sourceField: 'creatorID',
      destField: 'id',
      destSchema: () => userSchema,
    },
    assignee: {
      sourceField: 'assigneeID',
      destField: 'id',
      destSchema: () => userSchema,
    },
    viewState: {
      sourceField: 'id',
      destField: 'issueID',
      destSchema: () => viewStateSchema,
    },
    emoji: {
      sourceField: 'id',
      destField: 'subjectID',
      destSchema: () => emojiSchema,
    },
  },
} as const;

const viewStateSchema = createTableSchema({
  tableName: 'viewState',
  columns: {
    issueID: 'string',
    userID: 'string',
    viewed: 'number',
  },
  primaryKey: ['userID', 'issueID'],
});

const commentSchema = {
  tableName: 'comment',
  columns: {
    id: 'string',
    issueID: 'string',
    created: 'number',
    body: 'string',
    creatorID: 'string',
  },
  primaryKey: 'id',
  relationships: {
    creator: {
      sourceField: 'creatorID',
      destField: 'id',
      destSchema: () => userSchema,
    },
    emoji: {
      sourceField: 'id',
      destField: 'subjectID',
      destSchema: () => emojiSchema,
    },
    issue: {
      sourceField: 'issueID',
      destField: 'id',
      destSchema: () => issueSchema,
    },
  },
} as const;

const labelSchema = createTableSchema({
  tableName: 'label',
  columns: {
    id: 'string',
    name: 'string',
  },
  primaryKey: 'id',
});

const issueLabelSchema = {
  tableName: 'issueLabel',
  columns: {
    issueID: 'string',
    labelID: 'string',
  },
  primaryKey: ['issueID', 'labelID'],
  relationships: {
    issue: {
      sourceField: 'issueID',
      destField: 'id',
      destSchema: () => issueSchema,
    },
  },
} as const;

const emojiSchema = {
  tableName: 'emoji',
  columns: {
    id: 'string',
    value: 'string',
    annotation: 'string',
    subjectID: 'string',
    creatorID: 'string',
    created: 'number',
  },
  primaryKey: 'id',
  relationships: {
    creator: {
      sourceField: 'creatorID',
      destField: 'id',
      destSchema: userSchema,
    },
    issue: {
      sourceField: 'subjectID',
      destField: 'id',
      destSchema: issueSchema,
    },
    comment: {
      sourceField: 'subjectID',
      destField: 'id',
      destSchema: commentSchema,
    },
  },
} as const;

const userPrefSchema = createTableSchema({
  tableName: 'userPref',
  columns: {
    key: 'string',
    userID: 'string',
    value: 'string',
  },
  primaryKey: ['userID', 'key'],
});

export type IssueRow = Row<typeof issueSchema>;
export type CommentRow = Row<typeof commentSchema>;
export type Schema = typeof schema;

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
  role: 'crew' | 'user';
};

export const schema = createSchema({
  // If you change this make sure to change apps/zbugs/docker/init_upstream/init.sql
  // as well as updating the database on both prod and on sandbox.
  version: 5,

  tables: {
    user: userSchema,
    issue: issueSchema,
    comment: commentSchema,
    label: labelSchema,
    issueLabel: issueLabelSchema,
    viewState: viewStateSchema,
    emoji: emojiSchema,
    userPref: userPrefSchema,
  },
});

type PermissionRule<TSchema extends TableSchema> = (
  authData: AuthData,
  eb: ExpressionBuilder<TSchema>,
) => Condition;

function and<TSchema extends TableSchema>(
  ...rules: PermissionRule<TSchema>[]
): PermissionRule<TSchema> {
  return (authData, eb) => eb.and(...rules.map(rule => rule(authData, eb)));
}

export const permissions: ReturnType<typeof definePermissions> =
  definePermissions<AuthData, Schema>(schema, () => {
    const userIsLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const loggedInUserIsCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<
        typeof commentSchema | typeof emojiSchema | typeof issueSchema
      >,
    ) =>
      eb.and(
        userIsLoggedIn(authData, eb),
        eb.cmp('creatorID', '=', authData.sub),
      );

    const loggedInUserIsAdmin = (
      authData: AuthData,
      eb: ExpressionBuilder<TableSchema>,
    ) =>
      eb.and(
        userIsLoggedIn(authData, eb),
        eb.cmpLit(authData.role, '=', 'crew'),
      );

    const allowIfUserIDMatchesLoggedInUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof viewStateSchema | typeof userPrefSchema>,
    ) => cmp('userID', '=', authData.sub);

    const allowIfAdminOrIssueCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueLabelSchema>,
    ) =>
      eb.or(
        loggedInUserIsAdmin(authData, eb),
        eb.exists('issue', iq =>
          iq.where(eb => loggedInUserIsCreator(authData, eb)),
        ),
      );

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueSchema>,
    ) =>
      eb.or(loggedInUserIsAdmin(authData, eb), eb.cmp('visibility', 'public'));

    /**
     * Comments are only visible if the user can see the issue they're attached to.
     */
    const canSeeComment = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof commentSchema>,
    ) => eb.exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    /**
     * Issue labels are only visible if the user can see the issue they're attached to.
     */
    const canSeeIssueLabel = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueLabelSchema>,
    ) => eb.exists('issue', q => q.where(eb => canSeeIssue(authData, eb)));

    /**
     * Emoji are only visible if the user can see the issue they're attached to.
     */
    const canSeeEmoji = (
      authData: AuthData,
      {exists, or}: ExpressionBuilder<typeof emojiSchema>,
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
          insert: [],
          update: {
            preMutation: [],
          },
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
            postProposedMutation: [loggedInUserIsCreator, loggedInUserIsAdmin],
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
          select: [canSeeComment],
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
          // view state cannot be deleted
          delete: [],
        },
      },
      issueLabel: {
        row: {
          insert: [and(canSeeIssueLabel, allowIfAdminOrIssueCreator)],
          update: {
            preMutation: [],
          },
          delete: [and(canSeeIssueLabel, allowIfAdminOrIssueCreator)],
          select: [canSeeIssueLabel],
        },
      },
      emoji: {
        row: {
          // Can only insert emoji if the can see the issue.
          insert: [and(canSeeEmoji, loggedInUserIsCreator)],

          // Can only update their own emoji.
          update: {
            preMutation: [and(canSeeEmoji, loggedInUserIsCreator)],
            postProposedMutation: [and(canSeeEmoji, loggedInUserIsCreator)],
          },
          delete: [and(canSeeEmoji, loggedInUserIsCreator)],
          select: [canSeeEmoji],
        },
      },
      userPref: {
        row: {
          insert: [allowIfUserIDMatchesLoggedInUser],
          update: {
            preMutation: [allowIfUserIDMatchesLoggedInUser],
            postProposedMutation: [allowIfUserIDMatchesLoggedInUser],
          },
          delete: [allowIfUserIDMatchesLoggedInUser],
        },
      },
    };
  });
