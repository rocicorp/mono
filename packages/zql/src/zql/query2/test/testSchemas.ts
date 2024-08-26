export const issueSchema = {
  table: 'issue',
  fields: {
    id: {type: 'string'},
    title: {type: 'string'},
    description: {type: 'string'},
    closed: {type: 'boolean'},
    ownerId: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    owner: {
      source: 'ownerId',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
    comments: {
      source: 'id',
      dest: {
        field: 'issueId',
        schema: () => commentSchema,
      },
    },
    labels: {
      source: 'id',
      junction: {
        sourceField: 'issueId',
        destField: 'labelId',
        schema: () => issueLabelSchema,
      },
      dest: {
        field: 'id',
        schema: () => labelSchema,
      },
    },
  },
} as const;

export const issueLabelSchema = {
  table: 'issueLabel',
  fields: {
    issueId: {type: 'string'},
    labelId: {type: 'string'},
  },
  primaryKey: ['issueId', 'labelId'],
} as const;

export const labelSchema = {
  table: 'label',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      source: 'id',
      junction: {
        sourceField: 'labelId',
        destField: 'issueId',
      },
      dest: {
        field: 'id',
        schema: issueSchema,
      },
    },
  },
} as const;

export const commentSchema = {
  table: 'comment',
  fields: {
    id: {type: 'string'},
    authorId: {type: 'string'},
    issueId: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issue: {
      source: 'issueId',
      dest: {
        field: 'id',
        schema: issueSchema,
      },
    },
    revisions: {
      source: 'id',
      dest: {
        field: 'commentId',
        schema: () => revisionSchema,
      },
    },
    author: {
      source: 'authorId',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
  },
} as const;

export const revisionSchema = {
  table: 'revision',
  fields: {
    id: {type: 'string'},
    authorId: {type: 'string'},
    commentId: {type: 'string'},
    text: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    comment: {
      source: 'commentId',
      dest: {
        field: 'id',
        schema: commentSchema,
      },
    },
    author: {
      source: 'authorId',
      dest: {
        field: 'id',
        schema: () => userSchema,
      },
    },
  },
} as const;

export const userSchema = {
  table: 'user',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    issues: {
      source: 'id',
      dest: {
        field: 'ownerId',
        schema: issueSchema,
      },
    },
  },
} as const;
