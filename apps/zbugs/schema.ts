import {
  createSchema,
  createTableSchema,
  defineAuthorization,
  type ExpressionBuilder,
  type TableSchema,
  type TableSchemaToRow,
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
    labels: [
      {
        sourceField: ['id'],
        destField: ['issueID'],
        destSchema: () => issueLabelSchema,
      },
      {
        sourceField: ['labelID'],
        destField: ['id'],
        destSchema: () => labelSchema,
      },
    ],
    comments: {
      sourceField: ['id'],
      destField: ['issueID'],
      destSchema: () => commentSchema,
    },
    creator: {
      sourceField: ['creatorID'],
      destField: ['id'],
      destSchema: () => userSchema,
    },
    assignee: {
      sourceField: ['assigneeID'],
      destField: ['id'],
      destSchema: () => userSchema,
    },
    viewState: {
      sourceField: ['id'],
      destField: ['issueID'],
      destSchema: () => viewStateSchema,
    },
    emoji: {
      sourceField: ['id'],
      destField: ['subjectID'],
      destSchema: () => emojiSchema,
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
      sourceField: ['creatorID'],
      destField: ['id'],
      destSchema: () => userSchema,
    },
    emoji: {
      sourceField: ['id'],
      destField: ['subjectID'],
      destSchema: () => emojiSchema,
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
      sourceField: ['creatorID'],
      destField: ['id'],
      destSchema: () => userSchema,
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
  primaryKey: ['userID', 'key'],
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

const authorization = defineAuthorization<AuthData, Schema>(schema, () => {
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

  const allowIfUserIDMatchesLoggedInUser = (
    authData: AuthData,
    {cmp}: ExpressionBuilder<typeof viewStateSchema>,
  ) => cmp('userID', '=', authData.sub);

  return {
    user: {
      // Only the authentication system can write to the user table.
      row: {
        insert: [],
        update: {
          preMutation: [],
        },
        delete: [],
      },
    },
    issue: {
      row: {
        insert: [allowIfLoggedIn],
        update: {
          preMutation: [allowIfIssueCreator, allowIfAdmin],
        },
        delete: [allowIfIssueCreator, allowIfAdmin],
      },
    },
    comment: {
      row: {
        insert: [allowIfLoggedIn],
        update: {
          preMutation: [allowIfCommentCreator, allowIfAdmin],
        },
        delete: [allowIfCommentCreator, allowIfAdmin],
      },
    },
    label: {
      row: {
        insert: [allowIfAdmin],
        update: {
          preMutation: [allowIfAdmin],
        },
        delete: [allowIfAdmin],
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
    // TODO (mlaw): issueLabel permissions (only issue creator can set)
  };
});

// TODO (mlaw): once we move auth to be defined on the table, there will be a single default export which is
// the schema. Working towards this next.
const exported: {
  schema: typeof schema;
  authorization: ReturnType<typeof defineAuthorization>;
} = {
  schema,
  authorization,
};
export default exported;
