export const issueSchema = {
  tableName: 'issue',
  columns: {
    id: {type: 'string'},
    title: {type: 'string'},
    description: {type: 'string'},
    closed: {type: 'boolean'},
    ownerId: {type: 'string', optional: true},
  },
  primaryKey: ['id'],
  relationships: {
    owner: {
      correlation: [['ownerId', 'id']],
      schema: () => userSchema,
    },
    comments: {
      correlation: [['id', 'issueId']],
      schema: () => commentSchema,
    },
    labels: {
      correlation: [['id', 'id']],
      schema: () => labelSchema,
      junction: {
        correlation: [['issueId', 'labelId']],
        schema: () => issueLabelSchema,
      },
    },
  },
} as const;

export const issueLabelSchema = {
  tableName: 'issueLabel',
  columns: {
    issueId: {type: 'string'},
    labelId: {type: 'string'},
  },
  primaryKey: ['issueId', 'labelId'],
  relationships: {},
} as const;

export const labelSchema = {
  tableName: 'label',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      correlation: [['id', 'id']],
      schema: issueSchema,
      junction: {
        correlation: [['labelId', 'issueId']],
        schema: issueLabelSchema,
      },
    },
  },
} as const;

export const commentSchema = {
  tableName: 'comment',
  columns: {
    id: {type: 'string'},
    authorId: {type: 'string'},
    issueId: {type: 'string'},
    text: {type: 'string'},
    createdAt: {type: 'number'},
  },
  primaryKey: ['id'],
  relationships: {
    issue: {
      correlation: [['issueId', 'id']],
      schema: issueSchema,
    },
    revisions: {
      correlation: [['id', 'commentId']],
      schema: () => revisionSchema,
    },
    author: {
      correlation: [['authorId', 'id']],
      schema: () => userSchema,
    },
  },
} as const;

export const revisionSchema = {
  tableName: 'revision',
  columns: {
    id: {type: 'string'},
    authorId: {type: 'string'},
    commentId: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    comment: {
      correlation: [['commentId', 'id']],
      schema: commentSchema,
    },
    author: {
      correlation: [['authorId', 'id']],
      schema: () => userSchema,
    },
  },
} as const;

export const userSchema = {
  tableName: 'user',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
    metadata: {type: 'json', optional: true},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      correlation: [['id', 'ownerId']],
      schema: issueSchema,
    },
  },
} as const;

export const schemas = {
  issue: issueSchema,
  issueLabel: issueLabelSchema,
  label: labelSchema,
  comment: commentSchema,
  revision: revisionSchema,
  user: userSchema,
} as const;
