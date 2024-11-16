import {
  createSchema,
  createTableSchema,
  defineAuthorization,
  type ExpressionBuilder,
  type TableSchemaToRow,
  type TableSchema,
} from '@rocicorp/zero';

const userSchema = createTableSchema({
  tableName: 'user',
  columns: {
    id: {type: 'string'},
    login: {type: 'string'},
    name: {type: 'string'},
    avatar: {type: 'string'},
    role: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
});

const issueSchema = createTableSchema({
  tableName: 'issue',
  columns: {
    id: {type: 'string'},
    shortID: {type: 'number', optional: true},
    title: {type: 'string'},
    open: {type: 'boolean'},
    modified: {type: 'number'},
    created: {type: 'number'},
    creatorID: {type: 'string'},
    assigneeID: {type: 'string', optional: true},
    description: {type: 'string'},
    labelIDs: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    labels: {
      source: 'id',
      junction: {
        schema: () => issueLabelSchema,
        sourceField: 'issueID',
        destField: 'labelID',
      },
      dest: {
        field: 'id',
        schema: () => labelSchema,
      },
    },
    comments: {
      source: 'id',
      dest: {
        field: 'issueID',
        schema: () => commentSchema,
      },
    },
    creator: {
      source: 'creatorID',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    assignee: {
      source: 'assigneeID',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    viewState: {
      source: 'id',
      dest: {
        field: 'issueID',
        schema: () => viewStateSchema,
      },
    },
    emoji: {
      source: 'id',
      dest: {
        field: 'subjectID',
        schema: () => emojiSchema,
      },
    },
  },
});

const viewStateSchema = createTableSchema({
  tableName: 'viewState',
  columns: {
    issueID: {type: 'string'},
    userID: {type: 'string'},
    viewed: {type: 'number'},
  },
  primaryKey: ['userID', 'issueID'],
  relationships: {},
});

const commentSchema = createTableSchema({
  tableName: 'comment',
  columns: {
    id: {type: 'string'},
    issueID: {type: 'string'},
    created: {type: 'number'},
    body: {type: 'string'},
    creatorID: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    creator: {
      source: 'creatorID',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    emoji: {
      source: 'id',
      dest: {
        field: 'subjectID',
        schema: () => emojiSchema,
      },
    },
  },
});

const labelSchema = createTableSchema({
  tableName: 'label',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
});

const issueLabelSchema = createTableSchema({
  tableName: 'issueLabel',
  columns: {
    issueID: {type: 'string'},
    labelID: {type: 'string'},
  },
  primaryKey: ['issueID', 'labelID'],
  relationships: {},
});

const emojiSchema = createTableSchema({
  tableName: 'emoji',
  columns: {
    id: {type: 'string'},
    value: {type: 'string'},
    annotation: {type: 'string'},
    subjectID: {type: 'string'},
    creatorID: {type: 'string'},
    created: {type: 'number'},
  },
  primaryKey: ['id'],
  relationships: {
    creator: {
      source: 'creatorID',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
  },
});

const userPrefSchema = createTableSchema({
  tableName: 'userPref',
  columns: {
    key: {type: 'string'},
    userID: {type: 'string'},
    value: {type: 'string'},
  },
  primaryKey: ['key', 'userID'],
  relationships: {},
});

export const schema = createSchema({
  version: 4,
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

export type IssueRow = TableSchemaToRow<typeof issueSchema>;
export type CommentRow = TableSchemaToRow<typeof commentSchema>;
export type Schema = typeof schema;

/** The contents of the zbugs JWT */
type AuthData = {
  // The logged in userID.
  sub: string;
  role: 'crew' | 'user';
};

export const authorization = defineAuthorization<AuthData, Schema>(
  schema,
  () => {
    const allowIfLoggedIn = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.sub, 'IS NOT', null);

    const allowIfIssueCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof issueSchema>,
    ) => cmp('creatorID', '=', authData.sub);

    const allowIfCommentCreator = (
      authData: AuthData,
      {cmp}: ExpressionBuilder<typeof commentSchema>,
    ) => cmp('creatorID', '=', authData.sub);

    const allowIfAdmin = (
      authData: AuthData,
      {cmpLit}: ExpressionBuilder<TableSchema>,
    ) => cmpLit(authData.role, '=', 'crew');

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
          insert: [allowIfLoggedIn],
          update: [allowIfIssueCreator, allowIfAdmin],
          delete: [allowIfIssueCreator, allowIfAdmin],
        },
      },
      comment: {
        row: {
          insert: [allowIfLoggedIn],
          update: [allowIfCommentCreator, allowIfAdmin],
          delete: [allowIfCommentCreator, allowIfAdmin],
        },
      },
      label: {
        row: {
          insert: [allowIfAdmin],
          update: [allowIfAdmin],
          delete: [allowIfAdmin],
        },
      },
      // TODO: issueLabel permissions (only issue creator can set)
      // TODO: viewState permissions (invariant on userID set to logged in user)
      // ^-- requires access to _current_ value of the row.
      // TODO: type errors should be raised if there is a schema mismatch between
      // 1. the rule and 2. the table it is applied to
    };
  },
) as ReturnType<typeof defineAuthorization>;
