import {expect, test} from 'vitest';
import type {Schema as ZeroSchema} from '../../zero-types/src/schema.ts';
import type {ExpressionBuilder} from '../../zql/src/query/expression.ts';
import {createSchema} from './builder/schema-builder.ts';
import {column, table} from './builder/table-builder.ts';
import {definePermissions} from './permissions.ts';

const {string} = column;

const userSchema = table('user')
  .from('users')
  .columns({
    id: string().from('user_id'),
    login: string(),
    name: string(),
    avatar: string(),
    role: string(),
  })
  .primaryKey('id');

const schema = createSchema({tables: [userSchema]});

type AuthData = {
  sub: string;
  role: 'admin' | 'user';
};

test('permission rules create query ASTs', async () => {
  const config = await definePermissions<AuthData, typeof schema>(
    schema,
    () => {
      const allowIfAdmin = (
        authData: AuthData,
        {cmpLit}: ExpressionBuilder<string, ZeroSchema>,
      ) => cmpLit(authData.role, '=', 'admin');

      return {
        user: {
          row: {
            insert: [allowIfAdmin],
            update: {
              preMutation: [allowIfAdmin],
            },
            delete: [allowIfAdmin],
          },
        },
      };
    },
  );

  expect(config).toMatchInlineSnapshot(`
    {
      "tables": {
        "users": {
          "cell": undefined,
          "row": {
            "delete": [
              [
                "allow",
                {
                  "left": {
                    "anchor": "authData",
                    "field": "role",
                    "type": "static",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": "admin",
                  },
                  "type": "simple",
                },
              ],
            ],
            "insert": [
              [
                "allow",
                {
                  "left": {
                    "anchor": "authData",
                    "field": "role",
                    "type": "static",
                  },
                  "op": "=",
                  "right": {
                    "type": "literal",
                    "value": "admin",
                  },
                  "type": "simple",
                },
              ],
            ],
            "select": undefined,
            "update": {
              "postMutation": undefined,
              "preMutation": [
                [
                  "allow",
                  {
                    "left": {
                      "anchor": "authData",
                      "field": "role",
                      "type": "static",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "admin",
                    },
                    "type": "simple",
                  },
                ],
              ],
            },
          },
        },
      },
    }
  `);
});

test('nested parameters', async () => {
  type AuthData = {
    sub: string;
    role: 'admin' | 'user';
    attributes: {role: 'admin' | 'user'; id: string};
  };
  const config = await definePermissions<AuthData, typeof schema>(
    schema,
    () => {
      const allowIfAdmin = (
        authData: AuthData,
        {or, cmpLit}: ExpressionBuilder<string, ZeroSchema>,
      ) =>
        or(
          cmpLit(authData.role, '=', 'admin'),
          cmpLit(authData.attributes.role, '=', 'admin'),
        );

      const allowIfSelf = (
        authData: AuthData,
        {cmp}: ExpressionBuilder<'user', typeof schema>,
      ) => cmp('id', authData.attributes.id);

      return {
        user: {
          row: {
            insert: [allowIfAdmin],
            update: {
              preMutation: [allowIfSelf],
            },
            delete: [allowIfAdmin],
            select: [allowIfAdmin],
          },
        },
      };
    },
  );

  expect(config).toMatchInlineSnapshot(`
    {
      "tables": {
        "users": {
          "cell": undefined,
          "row": {
            "delete": [
              [
                "allow",
                {
                  "conditions": [
                    {
                      "left": {
                        "anchor": "authData",
                        "field": "role",
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "anchor": "authData",
                        "field": [
                          "attributes",
                          "role",
                        ],
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                  ],
                  "type": "or",
                },
              ],
            ],
            "insert": [
              [
                "allow",
                {
                  "conditions": [
                    {
                      "left": {
                        "anchor": "authData",
                        "field": "role",
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "anchor": "authData",
                        "field": [
                          "attributes",
                          "role",
                        ],
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                  ],
                  "type": "or",
                },
              ],
            ],
            "select": [
              [
                "allow",
                {
                  "conditions": [
                    {
                      "left": {
                        "anchor": "authData",
                        "field": "role",
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                    {
                      "left": {
                        "anchor": "authData",
                        "field": [
                          "attributes",
                          "role",
                        ],
                        "type": "static",
                      },
                      "op": "=",
                      "right": {
                        "type": "literal",
                        "value": "admin",
                      },
                      "type": "simple",
                    },
                  ],
                  "type": "or",
                },
              ],
            ],
            "update": {
              "postMutation": undefined,
              "preMutation": [
                [
                  "allow",
                  {
                    "left": {
                      "name": "user_id",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "anchor": "authData",
                      "field": [
                        "attributes",
                        "id",
                      ],
                      "type": "static",
                    },
                    "type": "simple",
                  },
                ],
              ],
            },
          },
        },
      },
    }
  `);
});
