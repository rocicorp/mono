import {describe, expect, test} from 'vitest';
import {definePermissions} from '../../../zero-schema/src/permissions.js';
import {createSchema} from '../../../zero-schema/src/schema.js';
import {
  createTableSchema,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.js';
import type {ExpressionBuilder} from '../../../zql/src/query/expression.js';
import {
  astForTestingSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.js';
import type {Query, QueryType} from '../../../zql/src/query/query.js';
import {transformQuery} from './read-authorizer.js';
import {must} from '../../../shared/src/must.js';

const mockDelegate = {} as QueryDelegate;

function ast(q: Query<TableSchema, QueryType>) {
  return (q as QueryImpl<TableSchema, QueryType>)[astForTestingSymbol];
}

const unreadable = createTableSchema({
  tableName: 'unreadable',
  columns: {
    id: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {},
});
const readable = {
  tableName: 'readable',
  columns: {
    id: {type: 'string'},
    unreadableId: {type: 'string'},
    readableId: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    readable: {
      sourceField: ['readableId'],
      destField: ['id'],
      destSchema: () => readable,
    },
    unreadable: {
      sourceField: ['unreadableId'],
      destField: ['id'],
      destSchema: unreadable,
    },
  },
} as const;
const adminReadable = {
  tableName: 'adminReadable',
  columns: {
    id: {type: 'string'},
  },
  primaryKey: ['id'],
  relationships: {
    self1: {
      sourceField: ['id'],
      destField: ['id'],
      destSchema: () => adminReadable,
    },
    self2: {
      sourceField: ['id'],
      destField: ['id'],
      destSchema: () => adminReadable,
    },
  },
} as const;

const schema = createSchema({
  version: 1,
  tables: {
    readable,
    unreadable,
    adminReadable,
  },
});

type AuthData = {
  sub: string;
  role: string;
};

const authData: AuthData = {
  sub: '001',
  role: 'user',
};
const permissionRules = must(
  await definePermissions<AuthData, typeof schema>(schema, () => ({
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
            eb: ExpressionBuilder<typeof adminReadable>,
          ) => eb.cmpLit(authData.role, '=', 'admin'),
        ],
      },
    },
  })),
);

describe('unreadable tables', () => {
  test('nuke top level queries', () => {
    const query = newQuery(mockDelegate, schema.tables.unreadable);
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
    const query = newQuery(mockDelegate, schema.tables.readable)
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
          newQuery(mockDelegate, schema.tables.readable).related(
            'readable',
            q => q.related('readable', q => q.related('unreadable')),
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
                },
              ],
              "table": "readable",
              "where": undefined,
            },
          },
        ],
        "table": "readable",
        "where": undefined,
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).related(
            'readable',
            q => q.related('readable', q => q.related('unreadable')),
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
                },
              ],
              "table": "readable",
              "where": undefined,
            },
          },
        ],
        "table": "readable",
        "where": undefined,
      }
    `);

    // also nukes those tables with empty row policies
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).related('unreadable'),
        ),
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
    const query = newQuery(mockDelegate, schema.tables.readable).whereExists(
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
          newQuery(mockDelegate, schema.tables.readable).where(
            ({not, exists}) => not(exists('unreadable')),
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
          newQuery(mockDelegate, schema.tables.readable).where(
            ({not, exists}) => not(exists('unreadable')),
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
          newQuery(mockDelegate, schema.tables.readable).whereExists(
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
          },
          "type": "correlatedSubquery",
        },
      }
    `);

    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).whereExists(
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
          },
          "type": "correlatedSubquery",
        },
      }
    `);

    // having siblings doesn't break it
    expect(
      transformQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable)
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
          newQuery(mockDelegate, schema.tables.readable)
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

describe('tables with no read policies', () => {
  function checkWithAndWithoutAuthData(
    cb: (authData: AuthData | undefined) => void,
  ) {
    cb(authData);
    cb(undefined);
  }
  test('top level query is unmodified', () => {
    checkWithAndWithoutAuthData(authData => {
      const query = newQuery(mockDelegate, schema.tables.readable);
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );
    });
  });
  test('related queries are unmodified', () => {
    checkWithAndWithoutAuthData(authData => {
      let query = newQuery(mockDelegate, schema.tables.readable).related(
        'readable',
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );

      query = newQuery(mockDelegate, schema.tables.readable).related(
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
      let query = newQuery(mockDelegate, schema.tables.readable).whereExists(
        'readable',
      );
      expect(transformQuery(ast(query), permissionRules, authData)).toEqual(
        ast(query),
      );

      query = newQuery(mockDelegate, schema.tables.readable).whereExists(
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
          newQuery(mockDelegate, adminReadable)
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
          newQuery(mockDelegate, adminReadable)
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
        ast(newQuery(mockDelegate, adminReadable).whereExists('self1')),
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
          newQuery(mockDelegate, adminReadable).whereExists('self1', q =>
            q.where('id', '1'),
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
          newQuery(mockDelegate, adminReadable).whereExists('self1', q =>
            q.whereExists('self2'),
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
