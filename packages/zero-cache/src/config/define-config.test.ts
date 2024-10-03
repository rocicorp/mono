import fs from 'fs';
import {afterEach} from 'node:test';
import {beforeEach, expect, type MockInstance, test, vi} from 'vitest';
import {defineConfig, type Queries} from './define-config.js';

type AuthData = {sub: string};

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
    title: {type: 'string'},
    open: {type: 'boolean'},
    modified: {type: 'number'},
    created: {type: 'number'},
    creatorID: {type: 'string'},
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
  },
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
    id: {type: 'string'},
    issueID: {type: 'string'},
    labelID: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
} as const;

export const schema = {
  version: 1,
  tables: {
    user: userSchema,
    issue: issueSchema,
    comment: commentSchema,
    label: labelSchema,
    issueLabel: issueLabelSchema,
  },
} as const;

const baseConfig = {
  upstreamUri: '',
  cvrDbUri: '',
  changeDbUri: '',
  replicaDbFile: '',
  replicaId: '',
  log: {
    level: 'info',
  },
} as const;

let writeFileSyncMock: MockInstance;
beforeEach(() => {
  writeFileSyncMock = vi
    .spyOn(fs, 'writeFileSync')
    .mockImplementation(() => {});
});

afterEach(() => {
  writeFileSyncMock.mockRestore();
});

test('static authorization rules', () => {
  const config = {
    ...baseConfig,
    authorization: {
      user: {
        table: {
          select: [],
          insert: [],
          update: [],
          delete: [],
        },
        column: {
          id: {
            select: [],
            insert: [],
            update: [],
            delete: [],
          },
        },
      },
    },
  };
  defineConfig(schema, () => config);

  expect(writeFileSyncMock.mock.calls[0][1]).toEqual(
    JSON.stringify(config, null, 2),
  );

  defineConfig(schema, queries => ({
    ...baseConfig,
    authorization: {
      user: {
        table: {
          select: [() => queries.user],
          insert: [() => queries.user],
          update: [() => queries.user],
          delete: [() => queries.user],
        },
        column: {
          id: {
            select: [() => queries.user],
            insert: [() => queries.user],
            update: [() => queries.user],
            delete: [() => queries.user],
          },
        },
      },
    },
  }));

  expect(writeFileSyncMock.mock.calls[1][1]).toMatchSnapshot();

  const policy = (queries: Queries<typeof schema>) => [
    (authData: AuthData) =>
      queries.user.where('id', '=', authData.sub).where('role', '=', 'crew'),
  ];
  defineConfig<AuthData, typeof schema>(schema, queries => ({
    ...baseConfig,
    authorization: {
      user: {
        table: {
          select: policy(queries),
          insert: policy(queries),
          update: policy(queries),
          delete: policy(queries),
        },
        column: {
          login: {
            select: policy(queries),
            insert: policy(queries),
            update: policy(queries),
            delete: policy(queries),
          },
        },
      },
    },
  }));

  expect(writeFileSyncMock.mock.calls[2][1]).toMatchSnapshot();
});

test('instance authorization rules', () => {
  defineConfig<AuthData, typeof schema>(schema, queries => ({
    ...baseConfig,
    authorization: {
      issue: {
        row: {
          update: [
            (authData, row) =>
              queries.issue
                .where('id', '=', row.id)
                .where('creatorID', '=', authData.sub),
            (authData, _row) =>
              queries.user
                .where('id', '=', authData.sub)
                .where('role', '=', 'crew'),
          ],
        },
      },
      comment: {
        row: {
          update: [
            (authData, row) =>
              queries.comment
                .where('id', '=', row.id)
                .where('creatorID', '=', authData.sub),
          ],
        },
        cell: {
          creatorID: {
            update: [
              (authData, _row) =>
                queries.user
                  .where('id', '=', authData.sub)
                  .where('role', '=', 'crew'),
            ],
          },
        },
      },
    },
  }));

  expect(writeFileSyncMock.mock.calls[0][1]).toMatchSnapshot();
});
