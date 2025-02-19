import {describe, expect, test} from 'vitest';
import {createSilentLogContext} from '../../../shared/src/logging-test-utils.ts';
import {must} from '../../../shared/src/must.ts';
import {relationships} from '../../../zero-schema/src/builder/relationship-builder.ts';
import type {Schema as ZeroSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {createSchema} from '../../../zero-schema/src/builder/schema-builder.ts';
import {string, table} from '../../../zero-schema/src/builder/table-builder.ts';
import {
  ANYONE_CAN,
  definePermissions,
} from '../../../zero-schema/src/permissions.ts';
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

const lc = createSilentLogContext();

function ast(q: Query<ZeroSchema, string>) {
  return (q as QueryImpl<ZeroSchema, string>)[astForTestingSymbol];
}

const unreadable = table('unreadable')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const unreadable2 = table('unreadable2')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const unreadable3 = table('unreadable3')
  .columns({
    id: string(),
  })
  .primaryKey('id');

const unreadable4 = table('unreadable4')
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
  tables: [
    unreadable,
    unreadable2,
    unreadable3,
    unreadable4,
    readable,
    adminReadable,
    readableThruUnreadable,
  ],
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
    readable: {
      row: {
        select: ANYONE_CAN,
      },
    },
    unreadable2: {},
    unreadable3: {
      row: {},
    },
    unreadable4: {
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
  const unreadables: Array<keyof Schema['tables']> = [
    'unreadable',
    'unreadable2',
    'unreadable3',
    'unreadable4',
  ];
  test('top-level', () => {
    for (const tableName of unreadables) {
      const query = newQuery(mockDelegate, schema, tableName);
      expect(
        transformQuery(lc, ast(query), permissionRules, authData),
      ).toStrictEqual({
        related: undefined,
        table: tableName,
        where: {
          type: 'or',
          conditions: [],
        },
      });
    }
  });

  test('related', () => {
    const query = newQuery(mockDelegate, schema, 'readable')
      .related('unreadable')
      .related('readable');

    expect(transformQuery(lc, ast(query), permissionRules, authData))
      .toMatchInlineSnapshot(`
        {
          "related": [
            {
              "correlation": {
                "childField": [
                  "id",
                ],
                "parentField": [
                  "unreadableId",
                ],
              },
              "subquery": {
                "alias": "unreadable",
                "orderBy": [
                  [
                    "id",
                    "asc",
                  ],
                ],
                "related": undefined,
                "table": "unreadable",
                "where": {
                  "conditions": [],
                  "type": "or",
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
                "where": {
                  "conditions": [],
                  "type": "and",
                },
              },
              "system": "client",
            },
          ],
          "table": "readable",
          "where": {
            "conditions": [],
            "type": "and",
          },
        }
      `);
    expect(transformQuery(lc, ast(query), permissionRules, undefined))
      .toMatchInlineSnapshot(`
        {
          "related": [
            {
              "correlation": {
                "childField": [
                  "id",
                ],
                "parentField": [
                  "unreadableId",
                ],
              },
              "subquery": {
                "alias": "unreadable",
                "orderBy": [
                  [
                    "id",
                    "asc",
                  ],
                ],
                "related": undefined,
                "table": "unreadable",
                "where": {
                  "conditions": [],
                  "type": "or",
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
                "where": {
                  "conditions": [],
                  "type": "and",
                },
              },
              "system": "client",
            },
          ],
          "table": "readable",
          "where": {
            "conditions": [],
            "type": "and",
          },
        }
      `);

    // no matter how nested
    expect(
      transformQuery(
        lc,
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
                    "related": [
                      {
                        "correlation": {
                          "childField": [
                            "id",
                          ],
                          "parentField": [
                            "unreadableId",
                          ],
                        },
                        "subquery": {
                          "alias": "unreadable",
                          "orderBy": [
                            [
                              "id",
                              "asc",
                            ],
                          ],
                          "related": undefined,
                          "table": "unreadable",
                          "where": {
                            "conditions": [],
                            "type": "or",
                          },
                        },
                        "system": "client",
                      },
                    ],
                    "table": "readable",
                    "where": {
                      "conditions": [],
                      "type": "and",
                    },
                  },
                  "system": "client",
                },
              ],
              "table": "readable",
              "where": {
                "conditions": [],
                "type": "and",
              },
            },
            "system": "client",
          },
        ],
        "table": "readable",
        "where": {
          "conditions": [],
          "type": "and",
        },
      }
    `);

    expect(
      transformQuery(
        lc,
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
                    "related": [
                      {
                        "correlation": {
                          "childField": [
                            "id",
                          ],
                          "parentField": [
                            "unreadableId",
                          ],
                        },
                        "subquery": {
                          "alias": "unreadable",
                          "orderBy": [
                            [
                              "id",
                              "asc",
                            ],
                          ],
                          "related": undefined,
                          "table": "unreadable",
                          "where": {
                            "conditions": [],
                            "type": "or",
                          },
                        },
                        "system": "client",
                      },
                    ],
                    "table": "readable",
                    "where": {
                      "conditions": [],
                      "type": "and",
                    },
                  },
                  "system": "client",
                },
              ],
              "table": "readable",
              "where": {
                "conditions": [],
                "type": "and",
              },
            },
            "system": "client",
          },
        ],
        "table": "readable",
        "where": {
          "conditions": [],
          "type": "and",
        },
      }
    `);

    expect(
      transformQuery(
        lc,
        ast(newQuery(mockDelegate, schema, 'readable').related('unreadable')),
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
                "unreadableId",
              ],
            },
            "subquery": {
              "alias": "unreadable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "unreadable",
              "where": {
                "conditions": [],
                "type": "or",
              },
            },
            "system": "client",
          },
        ],
        "table": "readable",
        "where": {
          "conditions": [],
          "type": "and",
        },
      }
    `);
  });

  test('subqueries in conditions are replaced by `const true` or `const false` expressions', () => {
    const query = newQuery(mockDelegate, schema, 'readable').whereExists(
      'unreadable',
    );

    // `unreadable` should be replaced by `false` condition.
    expect(transformQuery(lc, ast(query), permissionRules, undefined))
      .toMatchInlineSnapshot(`
        {
          "related": undefined,
          "table": "readable",
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
                    "where": {
                      "conditions": [],
                      "type": "or",
                    },
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
    expect(transformQuery(lc, ast(query), permissionRules, authData))
      .toMatchInlineSnapshot(`
        {
          "related": undefined,
          "table": "readable",
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
                    "where": {
                      "conditions": [],
                      "type": "or",
                    },
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

    // unreadable whereNotExists should be replaced by a `true` condition
    expect(
      transformQuery(
        lc,
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
          "conditions": [
            {
              "op": "NOT EXISTS",
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
                  "where": {
                    "conditions": [],
                    "type": "or",
                  },
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
        lc,
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
          "conditions": [
            {
              "op": "NOT EXISTS",
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
                  "where": {
                    "conditions": [],
                    "type": "or",
                  },
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

    // works no matter how nested
    expect(
      transformQuery(
        lc,
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
          "conditions": [
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
                            "where": {
                              "conditions": [],
                              "type": "or",
                            },
                          },
                          "system": "client",
                        },
                        "type": "correlatedSubquery",
                      },
                    ],
                    "type": "and",
                  },
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
        lc,
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
          "conditions": [
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
                            "where": {
                              "conditions": [],
                              "type": "or",
                            },
                          },
                          "system": "client",
                        },
                        "type": "correlatedSubquery",
                      },
                    ],
                    "type": "and",
                  },
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

    // having siblings doesn't break it
    expect(
      transformQuery(
        lc,
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
              "op": "NOT EXISTS",
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
                  "where": {
                    "conditions": [],
                    "type": "or",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
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
                  "where": {
                    "conditions": [],
                    "type": "and",
                  },
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
        lc,
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
              "op": "NOT EXISTS",
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
                  "where": {
                    "conditions": [],
                    "type": "or",
                  },
                },
                "system": "client",
              },
              "type": "correlatedSubquery",
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
                  "where": {
                    "conditions": [],
                    "type": "and",
                  },
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
      lc,
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
            "limit": undefined,
            "orderBy": [
              [
                "id",
                "asc",
              ],
            ],
            "related": undefined,
            "schema": undefined,
            "start": undefined,
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
      lc,
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
                  "limit": undefined,
                  "orderBy": [
                    [
                      "id",
                      "asc",
                    ],
                  ],
                  "related": undefined,
                  "schema": undefined,
                  "start": undefined,
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
      "where": {
        "conditions": [],
        "type": "and",
      },
    }
  `);
});

describe('admin readable', () => {
  test('relationships have the rules applied', () => {
    expect(
      transformQuery(
        lc,
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
        lc,
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
        lc,
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
        lc,
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
        lc,
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
