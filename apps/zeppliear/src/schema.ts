const memberSchema = {
  table: 'member',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
} as const;

const issueSchema = {
  table: 'issue',
  fields: {
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
  },
} as const;

const commentSchema = {
  table: 'comment',
  fields: {
    id: {type: 'string'},
    issueID: {type: 'string'},
    created: {type: 'number'},
    body: {type: 'string'},
    creatorID: {type: 'string'},
  },
  primaryKey: ['id'],
} as const;

const labelSchema = {
  table: 'label',
  fields: {
    id: {type: 'string'},
    name: {type: 'string'},
  },
  primaryKey: ['id'],
} as const;

const issueLabelSchema = {
  table: 'issueLabel',
  fields: {
    id: {type: 'string'},
    issueID: {type: 'string'},
    labelID: {type: 'string'},
  },
  // mutators require an ID field still.
  primaryKey: ['id'],
} as const;

export const schema = {
  member: memberSchema,
  issue: issueSchema,
  comment: commentSchema,
  label: labelSchema,
  issueLabel: issueLabelSchema,
} as const;

export type Schema = typeof schema;
