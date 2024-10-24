const userSchema = {
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
} as const;

const issueSchema = {
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
  },
} as const;

const viewStateSchema = {
  tableName: 'viewState',
  columns: {
    issueID: {type: 'string'},
    userID: {type: 'string'},
    viewed: {type: 'number'},
  },
  primaryKey: ['userID', 'issueID'],
  relationships: {},
} as const;

const commentSchema = {
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
  },
} as const;

const labelSchema = {
  tableName: 'label',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
} as const;

const issueLabelSchema = {
  tableName: 'issueLabel',
  columns: {
    issueID: {type: 'string'},
    labelID: {type: 'string'},
  },
  primaryKey: ['issueID', 'labelID'],
  relationships: {},
} as const;

const emojiSchema = {
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
} as const;

const userPrefSchema = {
  tableName: 'userPref',
  columns: {
    key: {type: 'string'},
    userID: {type: 'string'},
    value: {type: 'string'},
  },
  primaryKey: ['key', 'userID'],
  relationships: {},
} as const;

export const schema = {
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
} as const;

export type Schema = typeof schema;
