import {
  createSchema,
  createTableSchema,
  definePermissions,
  type ExpressionBuilder,
  type TableSchema,
  type TableSchemaToRow,
} from '@rocicorp/zero';

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

const issueSchema = createTableSchema({
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
});

const viewStateSchema = createTableSchema({
  tableName: 'viewState',
  columns: {
    issueID: 'string',
    userID: 'string',
    viewed: 'number',
  },
  primaryKey: ['userID', 'issueID'],
});

const commentSchema = createTableSchema({
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
  },
});

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

const emojiSchema = createTableSchema({
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
  },
});

const userPrefSchema = createTableSchema({
  tableName: 'userPref',
  columns: {
    key: 'string',
    userID: 'string',
    value: 'string',
  },
  primaryKey: ['userID', 'key'],
});

export type IssueRow = TableSchemaToRow<typeof issueSchema>;
export type CommentRow = TableSchemaToRow<typeof commentSchema>;
export type Schema = typeof schema;

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
  role: 'crew' | 'user';
};

export const schema = createSchema({
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

export const permissions: ReturnType<typeof definePermissions> =
  definePermissions<AuthData, Schema>(schema, () => {
    const userIsLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const loggedInUserIsIssueCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof issueSchema>,
    ) => cmp('creatorID', '=', authData.sub);

    const loggedInUserIsCommentCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof commentSchema>,
    ) => cmp('creatorID', '=', authData.sub);

    const loggedInUserIsAdmin = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.role, '=', 'crew');

    const allowIfUserIDMatchesLoggedInUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof viewStateSchema>,
    ) => cmp('userID', '=', authData.sub);

    const allowIfAdminOrIssueCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueLabelSchema>,
    ) =>
      eb.or(
        loggedInUserIsAdmin(authData, eb),
        eb.exists('issue', iq =>
          iq.where(eb => loggedInUserIsIssueCreator(authData, eb)),
        ),
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
            (authData, eb) =>
              eb.and(
                userIsLoggedIn(authData, eb),
                // prevents setting the creatorID of an issue to someone
                // other than the user doing the creating
                loggedInUserIsIssueCreator(authData, eb),
              ),
          ],
          update: {
            // TODO: add a check to prevent changing the creatorID
            preMutation: [loggedInUserIsIssueCreator, loggedInUserIsAdmin],
          },
          delete: [loggedInUserIsIssueCreator, loggedInUserIsAdmin],
        },
      },
      comment: {
        row: {
          insert: [
            (authData, eb) =>
              eb.and(
                userIsLoggedIn(authData, eb),
                loggedInUserIsCommentCreator(authData, eb),
              ),
          ],
          update: {
            preMutation: [loggedInUserIsCommentCreator, loggedInUserIsAdmin],
          },
          delete: [loggedInUserIsCommentCreator, loggedInUserIsAdmin],
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
            postProposedMutation: [allowIfUserIDMatchesLoggedInUser],
          },
          // view state cannot be deleted
          delete: [],
        },
      },
      issueLabel: {
        row: {
          insert: [allowIfAdminOrIssueCreator],
          update: {
            preMutation: [],
          },
          delete: [allowIfAdminOrIssueCreator],
        },
      },
    };
  });
