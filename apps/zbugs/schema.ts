import {
  createSchema,
  createTableSchema,
  definePermissions,
  type ExpressionBuilder,
  type TableSchema,
  type Row,
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

    const loggedInUserIsViewStateUser = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof viewStateSchema>,
    ) => cmp('userID', '=', authData.sub);

    const loggedInUserIsAdminOrIssueCreator = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueLabelSchema>,
    ) =>
      eb.or(
        loggedInUserIsAdmin(authData, eb),
        eb.exists('issue', iq =>
          iq.where(eb => loggedInUserIsIssueCreator(authData, eb)),
        ),
      );

    const canSeeIssue = (
      authData: AuthData,
      eb: ExpressionBuilder<typeof issueSchema>,
    ) =>
      eb.or(loggedInUserIsAdmin(authData, eb), eb.cmp('visibility', 'public'));

    const keyDidNotChange = <S extends TableSchema>(
      key: keyof TableSchemaToRow<S>,
      {cmpLit}: ExpressionBuilder<S>,
      oldRow: TableSchemaToRow<S>,
      newRow: TableSchemaToRow<S>,
    ) => cmpLit(oldRow[key], '=', newRow[key]);

    return {
      user: {
        // Only the authentication system can write to the user table.
        row: {
          insert: [],
          update: [],
          delete: [],
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
          update: [
            (authData, eb, oldRow, newRow) =>
              eb.and(
                keyDidNotChange('creatorID', eb, oldRow, newRow),
                eb.or(
                  loggedInUserIsIssueCreator(authData, eb),
                  loggedInUserIsAdmin(authData, eb),
                ),
              ),
          ],
          delete: [loggedInUserIsIssueCreator, loggedInUserIsAdmin],
          select: [canSeeIssue],
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
          update: [
            (authData, eb, oldRow, newRow) =>
              eb.and(
                keyDidNotChange('creatorID', eb, oldRow, newRow),
                eb.or(
                  loggedInUserIsCommentCreator(authData, eb),
                  loggedInUserIsAdmin(authData, eb),
                ),
              ),
          ],
          delete: [loggedInUserIsCommentCreator, loggedInUserIsAdmin],
          // comments are only visible if the user can see the issue they're on
          select: [
            (authData, {exists}) =>
              exists('issue', q => q.where(eb => canSeeIssue(authData, eb))),
          ],
        },
      },
      label: {
        row: {
          insert: [loggedInUserIsAdmin],
          update: [loggedInUserIsAdmin],
          delete: [loggedInUserIsAdmin],
        },
      },
      viewState: {
        row: {
          insert: [loggedInUserIsViewStateUser],
          update: [
            (authData, eb, oldRow, newRow) =>
              eb.and(
                loggedInUserIsViewStateUser(authData, eb),
                keyDidNotChange('userID', eb, oldRow, newRow),
              ),
          ],
          delete: [],
        },
      },
      issueLabel: {
        row: {
          insert: [loggedInUserIsAdminOrIssueCreator],
          update: [],
          delete: [loggedInUserIsAdminOrIssueCreator],
          select: [
            (authData, {exists}) =>
              exists('issue', q => q.where(eb => canSeeIssue(authData, eb))),
          ],
        },
      },
    };
  });
