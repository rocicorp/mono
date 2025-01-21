import {expect, test} from 'vitest';
import {createSchema} from './builder/schema-builder.js';
import {definePermissions} from './permissions.js';
import type {ExpressionBuilder} from '../../zql/src/query/expression.js';
import {table, column} from './builder/table-builder.js';
import type {Schema as ZeroSchema} from './builder/schema-builder.js';

const {string} = column;

const userSchema = table('user')
  .columns({
    id: string(),
    login: string(),
    name: string(),
    avatar: string(),
    role: string(),
  })
  .primaryKey('id');

const schema = createSchema(1, {tables: [userSchema]});

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
        {cmpLit}: ExpressionBuilder<ZeroSchema, string>,
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
      "user": {
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
    }
  `);
});

test('nested parameters', async () => {
  type AuthData = {
    sub: string;
    attributes: {role: 'admin' | 'user'};
  };
  const config = await definePermissions<AuthData, typeof schema>(
    schema,
    () => {
      const allowIfAdmin = (
        authData: AuthData,
        {cmpLit}: ExpressionBuilder<ZeroSchema, string>,
      ) => cmpLit(authData.attributes.role, '=', 'admin');

      return {
        user: {
          row: {
            insert: [allowIfAdmin],
            update: {
              preMutation: [allowIfAdmin],
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
      "user": {
        "cell": undefined,
        "row": {
          "delete": [
            [
              "allow",
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
          ],
          "insert": [
            [
              "allow",
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
          ],
          "select": [
            [
              "allow",
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
          ],
          "update": {
            "postMutation": undefined,
            "preMutation": [
              [
                "allow",
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
            ],
          },
        },
      },
    }
  `);
});
