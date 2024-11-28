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
      sourceField: ['ownerId'],
      destField: ['id'],
      destSchema: () => userSchema,
    },
    comments: {
      sourceField: ['id'],
      destField: ['issueId'],
      destSchema: () => commentSchema,
    },
    labels: [
      {
        sourceField: ['id'],
        destField: ['issueId'],
        destSchema: () => issueLabelSchema,
      },
      {
        sourceField: ['labelId'],
        destField: ['id'],
        destSchema: () => labelSchema,
      },
    ],
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
    issues: [
      {
        sourceField: ['id'],
        destField: ['labelId'],
        destSchema: issueLabelSchema,
      },
      {
        sourceField: ['issueId'],
        destField: ['id'],
        destSchema: issueSchema,
      },
    ],
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
      sourceField: ['issueId'],
      destField: ['id'],
      destSchema: issueSchema,
    },
    revisions: {
      sourceField: ['id'],
      destField: ['commentId'],
      destSchema: () => revisionSchema,
    },
    author: {
      sourceField: ['authorId'],
      destField: ['id'],
      destSchema: () => userSchema,
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
      sourceField: ['commentId'],
      destField: ['id'],
      destSchema: commentSchema,
    },
    author: {
      sourceField: ['authorId'],
      destField: ['id'],
      destSchema: () => userSchema,
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
      sourceField: ['id'],
      destField: ['ownerId'],
      destSchema: issueSchema,
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
