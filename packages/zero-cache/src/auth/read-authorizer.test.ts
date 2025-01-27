import {describe, expect, test} from 'vitest';
import {must} from '../../../shared/src/must.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {definePermissions} from '../../../zero-schema/src/permissions.ts';
import type {ExpressionBuilder} from '../../../zql/src/query/expression.ts';
import {
  astForTestingSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import type {Query} from '../../../zql/src/query/query.ts';
import {transformQuery} from './read-authorizer.ts';

const mockDelegate = {} as QueryDelegate;

function ast(q: Query<ZeroSchema, string>) {
  return (q as QueryImpl<ZeroSchema, string>)[astForTestingSymbol];
}

const unreadable = table('unreadable')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const readableThruUnreadable = table('readableThruUnreadable')
  .columns({
    id: string(),
    unreadableId: string(),
  })
  .primaryKey('id');

const readable = table('readable')
  .columns({
    id: string(),
    unreadableId: string(),
    readableId: string(),
  })
  .primaryKey('id');

const adminReadable = table('adminReadable')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const readableThruUnreadableRelationships = relationships(
  readableThruUnreadable,
  connect => ({
    unreadable: connect.many({
      sourceField: ['unreadableId'],
      destField: ['id'],
      destSchema: unreadable,
    }),
  }),
);

const readableRelationships = relationships(readable, connect => ({
  readable: connect.many({
    sourceField: ['readableId'],
    destField: ['id'],
    destSchema: readable,
  }),
  unreadable: connect.many({
    sourceField: ['unreadableId'],
    destField: ['id'],
    destSchema: unreadable,
  }),
  readableThruUnreadable: connect.many({
    sourceField: ['id'],
    destField: ['id'],
    destSchema: readableThruUnreadable,
  }),
}));

const adminReadableRelationships = relationships(adminReadable, connect => ({
  self1: connect.many({
    sourceField: ['id'],
    destField: ['id'],
    destSchema: adminReadable,
  }),
  self2: connect.many({
    sourceField: ['id'],
    destField: ['id'],
    destSchema: adminReadable,
  }),
}));

const schema = createSchema(1, {
  tables: [unreadable, readable, adminReadable, readableThruUnreadable],
  relationships: [
    readableThruUnreadableRelationships,
    readableRelationships,
    adminReadableRelationships,
  ],
});

type Schema = typeof schema;

type AuthData = {
  sub: string;
  role: string;
};

const authData: AuthData = {
  sub: '001',
  role: 'user',
};
const permissionRules = must(
  await definePermissions<AuthData, Schema>(schema, () => ({
    unreadable: {
      row: {
        select: [],
      },
    },
    adminReadable: {
      row: {
        select: [
          (
            authData: {role: string},
            eb: ExpressionBuilder<Schema, 'adminReadable'>,
          ) => eb.cmpLit(authData.role, '=', 'admin'),
        ],
      },
    },
    readableThruUnreadable: {
      row: {
        select: [
          (
            _authData: {role: string},
            eb: ExpressionBuilder<Schema, 'readableThruUnreadable'>,
          ) => eb.exists('unreadable'),
        ],
      },
    },
  })),
);

describe('unreadable tables', () => {
  test('nuke top level queries', () => {
    const query = newQuery(mockDelegate, schema, 'unreadable');
    // If a top-level query tries to query a table that cannot be read,
    // that query is set to `undefined`.
    expect(transformQuery(ast(query), permissionRules, authData)).toBe(
      undefined,
    );
    expect(transformQuery(ast(query), permissionRules, undefined)).toBe(
      undefined,
    );
  });

  test('nuke `related` queries', () => {
    const query = newQuery(mockDelegate, schema, 'readable')
      .related('unreadable')
      .related('readable');

    // any related calls to unreadable tables are removed.
    expect(transformQuery(ast(query), permissionRules, authData))
      .toMatchInlineSnapshot(`
        {
          "related": [
            {
              "correlation": {
                "childField": [
                  "id",
                ],
                "parentField": [
                  "readableId",
                ],
              },
              "subquery": {
                "alias": "readable",
                "orderBy": [
                  [
                    "id",
                    "asc",
                  ],
                ],
                "related": undefined,
                "table": "readable",
                "where": undefined,
              },
              "system": "client",
            },
          ],
          "table": "readable",
          "where": undefined,
        }
      `);
    expect(transformQuery(ast(query), permissionRules, undefined))
      .toMatchInlineSnapshot(`
        {
          "related": [
            {
              "correlation": {
                "childField": [
                  "id",
                ],
                "parentField": [
                  "readableId",
                ],
              },
              "subquery": {
                "alias": "readable",
                "orderBy": [
                  [
                    "id",
                    "asc",
                  ],
                ],
                "related": undefined,
                "table": "readable",
                "where": undefined,
              },
              "system": "client",
            },
          ],
          "table": "readable",
          "where": undefined,
        }
      `);

    // no matter how nested
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').related('readable', q =>
            q.related('readable', q => q.related('unreadable')),
          ),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "readableId",
              ],
            },
            "subquery": {
              "alias": "readable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": [
                {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "readableId",
                    ],
                  },
                  "subquery": {
                    "alias": "readable",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "related": [],
                    "table": "readable",
                    "where": undefined,
                  },
                  "system": "client",
                },
              ],
              "table": "readable",
              "where": undefined,
            },
            "system": "client",
          },
        ],
        "table": "readable",
        "where": undefined,
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').related('readable', q =>
            q.related('readable', q => q.related('unreadable')),
          ),
        ),
        permissionRules,
        undefined,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "readableId",
              ],
            },
            "subquery": {
              "alias": "readable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": [
                {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "readableId",
                    ],
                  },
                  "subquery": {
                    "alias": "readable",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "related": [],
                    "table": "readable",
                    "where": undefined,
                  },
                  "system": "client",
                },
              ],
              "table": "readable",
              "where": undefined,
            },
            "system": "client",
          },
        ],
        "table": "readable",
        "where": undefined,
      }
    `);

    // also nukes those tables with empty row policies
    expect(
      transformQuery(
        ast(newQuery(mockDelegate, schema, 'readable').related('unreadable')),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": [],
        "table": "readable",
        "where": undefined,
      }
    `);
  });

  test('subqueries in conditions are replaced by `const true` or `const false` expressions', () => {
    const query = newQuery(mockDelegate, schema, 'readable').whereExists(
      'unreadable',
    );

    // `unreadable` should be replaced by `false` condition.
    expect(transformQuery(ast(query), permissionRules, undefined))
      .toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "left": {
            "type": "literal",
            "value": true,
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": false,
          },
          "type": "simple",
        },
      }
    `);
    expect(transformQuery(ast(query), permissionRules, authData))
      .toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "left": {
            "type": "literal",
            "value": true,
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": false,
          },
          "type": "simple",
        },
      }
    `);

    // unreadable whereNotExists should be replaced by a `true` condition
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').where(({not, exists}) =>
            not(exists('unreadable')),
          ),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "left": {
            "type": "literal",
            "value": true,
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      }
    `);
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').where(({not, exists}) =>
            not(exists('unreadable')),
          ),
        ),
        permissionRules,
        undefined,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "left": {
            "type": "literal",
            "value": true,
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": true,
          },
          "type": "simple",
        },
      }
    `);

    // works no matter how nested
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').whereExists(
            'readable',
            q => q.whereExists('unreadable', q => q.where('id', '1')),
          ),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "op": "EXISTS",
          "related": {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "readableId",
              ],
            },
            "subquery": {
              "alias": "zsubq_readable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "readable",
              "where": {
                "left": {
                  "type": "literal",
                  "value": true,
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": false,
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          "type": "correlatedSubquery",
        },
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable').whereExists(
            'readable',
            q => q.whereExists('unreadable', q => q.where('id', '1')),
          ),
        ),
        permissionRules,
        undefined,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "op": "EXISTS",
          "related": {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "readableId",
              ],
            },
            "subquery": {
              "alias": "zsubq_readable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "readable",
              "where": {
                "left": {
                  "type": "literal",
                  "value": true,
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": false,
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          "type": "correlatedSubquery",
        },
      }
    `);

    // having siblings doesn't break it
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable')
            .where(({not, exists}) => not(exists('unreadable')))
            .whereExists('readable'),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "conditions": [
            {
              "left": {
                "type": "literal",
                "value": true,
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "readableId",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_readable",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "readable",
                  "where": undefined,
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
          ],
          "type": "and",
        },
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'readable')
            .where(({not, exists}) => not(exists('unreadable')))
            .whereExists('readable'),
        ),
        permissionRules,
        undefined,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "conditions": [
            {
              "left": {
                "type": "literal",
                "value": true,
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": true,
              },
              "type": "simple",
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "readableId",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_readable",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "readable",
                  "where": undefined,
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
          ],
          "type": "and",
        },
      }
    `);
  });
});

test('exists rules in permissions are tagged as the permissions system', () => {
  expect(
    transformQuery(
      ast(newQuery(mockDelegate, schema, 'readableThruUnreadable')),
      permissionRules,
      undefined,
    ),
  ).toMatchInlineSnapshot(`
    {
      "related": undefined,
      "table": "readableThruUnreadable",
      "where": {
        "op": "EXISTS",
        "related": {
          "correlation": {
            "childField": [
              "id",
            ],
            "parentField": [
              "unreadableId",
            ],
          },
          "subquery": {
            "alias": "zsubq_unreadable",
            "orderBy": [
              [
                "id",
                "asc",
              ],
            ],
            "related": undefined,
            "table": "unreadable",
            "where": undefined,
          },
          "system": "permissions",
        },
        "type": "correlatedSubquery",
      },
    }
  `);

  expect(
    transformQuery(
      ast(
        newQuery(mockDelegate, schema, 'readable').related(
          'readableThruUnreadable',
        ),
      ),
      permissionRules,
      undefined,
    ),
  ).toMatchInlineSnapshot(`
    {
      "related": [
        {
          "correlation": {
            "childField": [
              "id",
            ],
            "parentField": [
              "id",
            ],
          },
          "subquery": {
            "alias": "readableThruUnreadable",
            "orderBy": [
              [
                "id",
                "asc",
              ],
            ],
            "related": undefined,
            "table": "readableThruUnreadable",
            "where": {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "unreadableId",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_unreadable",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "unreadable",
                  "where": undefined,
                },
                "system": "permissions",
              },
              "type": "correlatedSubquery",
            },
          },
          "system": "client",
        },
      ],
      "table": "readable",
      "where": undefined,
    }
  `);
});

describe('tables with no read policies', () => {
  function checkWithAndWithoutAuthData(
    cb: (authData: AuthData | undefined) => void,
  ) {
    cb(authData);
    cb(undefined);
  }
  test('top level query is unmodified', () => {
    checkWithAndWithoutAuthData(authData => {
      const query = newQuery(mockDelegate, schema, 'readable');
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );
    });
  });
  test('related queries are unmodified', () => {
    checkWithAndWithoutAuthData(authData => {
      let query = newQuery(mockDelegate, schema, 'readable').related(
        'readable',
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );

      query = newQuery(mockDelegate, schema, 'readable').related(
        'readable',
        q => q.related('readable'),
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );
    });
  });
  test('subqueries in conditions are unmodified', () => {
    checkWithAndWithoutAuthData(authData => {
      let query = newQuery(mockDelegate, schema, 'readable').whereExists(
        'readable',
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );

      query = newQuery(mockDelegate, schema, 'readable').whereExists(
        'readable',
        q => q.whereExists('readable'),
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );
    });
  });
});

describe('admin readable', () => {
  test('relationships have the rules applied', () => {
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'adminReadable')
            .related('self1')
            .related('self2'),
        ),
        permissionRules,
        authData,
      ),
      // all levels of the query (root, self1, self2) should have the admin policy applied.
    ).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "id",
              ],
            },
            "subquery": {
              "alias": "self1",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "adminReadable",
              "where": {
                "left": {
                  "type": "literal",
                  "value": "user",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "admin",
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "id",
              ],
            },
            "subquery": {
              "alias": "self2",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "adminReadable",
              "where": {
                "left": {
                  "type": "literal",
                  "value": "user",
                },
                "op": "=",
                "right": {
                  "type": "literal",
                  "value": "admin",
                },
                "type": "simple",
              },
            },
            "system": "client",
          },
        ],
        "table": "adminReadable",
        "where": {
          "left": {
            "type": "literal",
            "value": "user",
          },
          "op": "=",
          "right": {
            "type": "literal",
            "value": "admin",
          },
          "type": "simple",
        },
      }
    `);

    // all levels of the query have the admin policy applied while preserving existing `wheres`
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'adminReadable')
            .related('self1', q => q.where('id', '1'))
            .related('self2', q =>
              q.where('id', '2').related('self1', q => q.where('id', '3')),
            )
            .where('id', '4'),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "id",
              ],
            },
            "subquery": {
              "alias": "self1",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "adminReadable",
              "where": {
                "conditions": [
                  {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "1",
                    },
                    "type": "simple",
                  },
                  {
                    "left": {
                      "type": "literal",
                      "value": "user",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "admin",
                    },
                    "type": "simple",
                  },
                ],
                "type": "and",
              },
            },
            "system": "client",
          },
          {
            "correlation": {
              "childField": [
                "id",
              ],
              "parentField": [
                "id",
              ],
            },
            "subquery": {
              "alias": "self2",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": [
                {
                  "correlation": {
                    "childField": [
                      "id",
                    ],
                    "parentField": [
                      "id",
                    ],
                  },
                  "subquery": {
                    "alias": "self1",
                    "orderBy": [
                      [
                        "id",
                        "asc",
                      ],
                    ],
                    "related": undefined,
                    "table": "adminReadable",
                    "where": {
                      "conditions": [
                        {
                          "left": {
                            "name": "id",
                            "type": "column",
                          },
                          "op": "=",
                          "right": {
                            "type": "literal",
                            "value": "3",
                          },
                          "type": "simple",
                        },
                        {
                          "left": {
                            "type": "literal",
                            "value": "user",
                          },
                          "op": "=",
                          "right": {
                            "type": "literal",
                            "value": "admin",
                          },
                          "type": "simple",
                        },
                      ],
                      "type": "and",
                    },
                  },
                  "system": "client",
                },
              ],
              "table": "adminReadable",
              "where": {
                "conditions": [
                  {
                    "left": {
                      "name": "id",
                      "type": "column",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "2",
                    },
                    "type": "simple",
                  },
                  {
                    "left": {
                      "type": "literal",
                      "value": "user",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "admin",
                    },
                    "type": "simple",
                  },
                ],
                "type": "and",
              },
            },
            "system": "client",
          },
        ],
        "table": "adminReadable",
        "where": {
          "conditions": [
            {
              "left": {
                "name": "id",
                "type": "column",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "4",
              },
              "type": "simple",
            },
            {
              "left": {
                "type": "literal",
                "value": "user",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "admin",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);
  });

  test('exists have the rules applied', () => {
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'adminReadable').whereExists('self1'),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "adminReadable",
        "where": {
          "conditions": [
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "id",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_self1",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "adminReadable",
                  "where": {
                    "left": {
                      "type": "literal",
                      "value": "user",
                    },
                    "op": "=",
                    "right": {
                      "type": "literal",
                      "value": "admin",
                    },
                    "type": "simple",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
            {
              "left": {
                "type": "literal",
                "value": "user",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "admin",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'adminReadable').whereExists(
            'self1',
            q => q.where('id', '1'),
          ),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "adminReadable",
        "where": {
          "conditions": [
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "id",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_self1",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "adminReadable",
                  "where": {
                    "conditions": [
                      {
                        "left": {
                          "name": "id",
                          "type": "column",
                        },
                        "op": "=",
                        "right": {
                          "type": "literal",
                          "value": "1",
                        },
                        "type": "simple",
                      },
                      {
                        "left": {
                          "type": "literal",
                          "value": "user",
                        },
                        "op": "=",
                        "right": {
                          "type": "literal",
                          "value": "admin",
                        },
                        "type": "simple",
                      },
                    ],
                    "type": "and",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
            {
              "left": {
                "type": "literal",
                "value": "user",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "admin",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema, 'adminReadable').whereExists(
            'self1',
            q => q.whereExists('self2'),
          ),
        ),
        permissionRules,
        authData,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "adminReadable",
        "where": {
          "conditions": [
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": [
                    "id",
                  ],
                  "parentField": [
                    "id",
                  ],
                },
                "subquery": {
                  "alias": "zsubq_self1",
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "table": "adminReadable",
                  "where": {
                    "conditions": [
                      {
                        "op": "EXISTS",
                        "related": {
                          "correlation": {
                            "childField": [
                              "id",
                            ],
                            "parentField": [
                              "id",
                            ],
                          },
                          "subquery": {
                            "alias": "zsubq_self2",
                            "orderBy": [
                              [
                                "id",
                                "asc",
                              ],
                            ],
                            "related": undefined,
                            "table": "adminReadable",
                            "where": {
                              "left": {
                                "type": "literal",
                                "value": "user",
                              },
                              "op": "=",
                              "right": {
                                "type": "literal",
                                "value": "admin",
                              },
                              "type": "simple",
                            },
                          },
                          "system": "client",
                        },
                        "type": "correlatedSubquery",
                      },
                      {
                        "left": {
                          "type": "literal",
                          "value": "user",
                        },
                        "op": "=",
                        "right": {
                          "type": "literal",
                          "value": "admin",
                        },
                        "type": "simple",
                      },
                    ],
                    "type": "and",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
            },
            {
              "left": {
                "type": "literal",
                "value": "user",
              },
              "op": "=",
              "right": {
                "type": "literal",
                "value": "admin",
              },
              "type": "simple",
            },
          ],
          "type": "and",
        },
      }
    `);
  });
});
