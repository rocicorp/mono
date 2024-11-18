import {expect, test} from 'vitest';
import {createSchema} from './schema.js';
import {createTableSchema, type TableSchema} from './table-schema.js';
import {defineAuthorization} from './authorization.js';
import type {ExpressionBuilder} from '../../zql/src/query/expression.js';

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
const schema = createSchema({
  version: 1,
  tables: {
    user: userSchema,
  },
});

type AuthData = {
  sub: string;
  role: 'admin' | 'user';
};

test('authorization rules create query ASTs', async () => {
  const config = await defineAuthorization<AuthData, typeof schema>(
    schema,
    () => {
      const allowIfAdmin = (
        authData: AuthData,
        {cmpLit}: ExpressionBuilder<TableSchema>,
      ) => cmpLit(authData.role, '=', 'admin');

      return {
        user: {
          row: {
            insert: [allowIfAdmin],
            update: [allowIfAdmin],
            delete: [allowIfAdmin],
          },
        },
      };
    },
  );

  expect(config).toMatchInlineSnapshot(`
    {
      "authorization": {
        "user": {
          "cell": undefined,
          "column": undefined,
          "row": undefined,
          "table": {
            "delete": [
              [
                "allow",
                {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "user",
                  "where": {
                    "conditions": [
                      {
                        "field": "id",
                        "op": "=",
                        "type": "simple",
                        "value": {
                          "anchor": "authData",
                          "field": "sub",
                          "type": "static",
                        },
                      },
                      {
                        "field": "role",
                        "op": "=",
                        "type": "simple",
                        "value": "admin",
                      },
                    ],
                    "type": "and",
                  },
                },
              ],
            ],
            "insert": [
              [
                "allow",
                {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "user",
                  "where": {
                    "conditions": [
                      {
                        "field": "id",
                        "op": "=",
                        "type": "simple",
                        "value": {
                          "anchor": "authData",
                          "field": "sub",
                          "type": "static",
                        },
                      },
                      {
                        "field": "role",
                        "op": "=",
                        "type": "simple",
                        "value": "admin",
                      },
                    ],
                    "type": "and",
                  },
                },
              ],
            ],
            "select": undefined,
            "update": [
              [
                "allow",
                {
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "table": "user",
                  "where": {
                    "conditions": [
                      {
                        "field": "id",
                        "op": "=",
                        "type": "simple",
                        "value": {
                          "anchor": "authData",
                          "field": "sub",
                          "type": "static",
                        },
                      },
                      {
                        "field": "role",
                        "op": "=",
                        "type": "simple",
                        "value": "admin",
                      },
                    ],
                    "type": "and",
                  },
                },
              ],
            ],
          },
        },
      },
    }
  `);
});
