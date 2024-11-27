import type {TableSchema} from '../../zero-schema/src/table-schema.js';

const memberSchema: TableSchema = {
  tableName: 'member',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
};

const issueSchema: TableSchema = {
  tableName: 'issue',
  columns: {
    id: {type: 'string'},
    title: {type: 'string'},
    // TODO: support enum types?
    // Should we swap the fields def to Valita?
    priority: {type: 'number'},
    status: {type: 'number'},
    modified: {type: 'number'},
    created: {type: 'number'},
    creatorID: {type: 'string'},
    kanbanOrder: {type: 'string'},
    description: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    labels: {
      correlation: [['id', 'id']],
      schema: () => labelSchema,
      junction: {
        correlation: [['issueID', 'labelID']],
        schema: () => issueLabelSchema,
      },
    },
    comments: {
      correlation: [['id', 'issueID']],
      schema: () => commentSchema,
    },
    creator: {
      correlation: [['creatorID', 'id']],
      schema: () => memberSchema,
    },
  },
};

const commentSchema: TableSchema = {
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
      correlation: [['creatorID', 'id']],
      schema: () => memberSchema,
    },
  },
};

const labelSchema: TableSchema = {
  tableName: 'label',
  columns: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
};

const issueLabelSchema: TableSchema = {
  tableName: 'issueLabel',
  columns: {
    id: {type: 'string'},
    issueID: {type: 'string'},
    labelID: {type: 'string'},
  },
  // mutators require an ID field still.
  primaryKey: ['labelID', 'issueID'],
  relationships: {},
};

export const schema = {
  member: memberSchema,
  issue: issueSchema,
  comment: commentSchema,
  label: labelSchema,
  issueLabel: issueLabelSchema,
} as const;

type AppSchema = typeof schema;
export type {AppSchema as Schema};
