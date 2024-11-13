import {describe, expect, test} from 'vitest';
import {defineAuthorization} from '../../../zero-schema/src/authorization.js';
import {createSchema} from '../../../zero-schema/src/schema.js';
import {
  astForTestingSymbol,
  newQuery,
  QueryImpl,
  type QueryDelegate,
} from '../../../zql/src/query/query-impl.js';
import {augmentQuery} from './read-authorizer.js';
import type {Query, QueryType} from '../../../zql/src/query/query.js';
import {
  createTableSchema,
  type TableSchema,
} from '../../../zero-schema/src/table-schema.js';
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
const unreadableRows = createTableSchema({
  tableName: 'unreadableRows',
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
    unreadable: {
      dest: {
        field: 'id',
        schema: unreadable,
      },
      source: 'unreadableId',
    },
    readable: {
      dest: {
        field: 'id',
        schema: () => readable,
      },
      source: 'readableId',
    },
    unreadableRows: {
      dest: {
        field: 'id',
        schema: unreadableRows,
      },
      source: 'unreadableId',
    },
  },
} as const;

const schema = createSchema({
  version: 1,
  tables: {
    unreadable,
    readable,
    unreadableRows,
  },
});

const auth = must(
  await defineAuthorization<Record<string, never>, typeof schema>(
    schema,
    () => ({
      unreadable: {
        table: {
          select: [],
        },
      },
      unreadableRows: {
        row: {
          select: [],
        },
      },
    }),
  ),
);

describe('unreadable tables', () => {
  test('nuke top level queries', () => {
    const query = newQuery(mockDelegate, schema.tables.unreadable);
    // If a top-level query tries to query a table that cannot be read,
    // that query is set to `undefined`.
    expect(augmentQuery(ast(query), auth)).toBe(undefined);

    expect(
      augmentQuery(
        ast(newQuery(mockDelegate, schema.tables.unreadableRows)),
        auth,
      ),
    ).toBe(undefined);
  });

  test('nuke `related` queries', () => {
    const query = newQuery(mockDelegate, schema.tables.readable)
      .related('unreadable')
      .related('readable');

    // any related calls to unreadable tables are removed.
    expect(augmentQuery(ast(query), auth)).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "readableId",
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
      augmentQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).related(
            'readable',
            q => q.related('readable', q => q.related('unreadable')),
          ),
        ),
        auth,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": [
          {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "readableId",
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
                    "childField": "id",
                    "op": "=",
                    "parentField": "readableId",
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
      augmentQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).related(
            'unreadableRows',
          ),
        ),
        auth,
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
    expect(augmentQuery(ast(query), auth)).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "type": "const",
          "value": false,
        },
      }
    `);

    // unreadable whereNotExists should be replaced by a `true` condition
    expect(
      augmentQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).where(
            ({not, exists}) => not(exists('unreadable')),
          ),
        ),
        auth,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "type": "const",
          "value": true,
        },
      }
    `);

    // works no matter how nested
    expect(
      augmentQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable).whereExists(
            'readable',
            q => q.whereExists('unreadable', q => q.where('id', '1')),
          ),
        ),
        auth,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "op": "EXISTS",
          "related": {
            "correlation": {
              "childField": "id",
              "op": "=",
              "parentField": "readableId",
            },
            "subquery": {
              "alias": "zsubq_3_readable",
              "orderBy": [
                [
                  "id",
                  "asc",
                ],
              ],
              "related": undefined,
              "table": "readable",
              "where": {
                "type": "const",
                "value": false,
              },
            },
          },
          "type": "correlatedSubquery",
        },
      }
    `);

    // having siblings doesn't break it
    expect(
      augmentQuery(
        ast(
          newQuery(mockDelegate, schema.tables.readable)
            .where(({not, exists}) => not(exists('unreadable')))
            .whereExists('readable'),
        ),
        auth,
      ),
    ).toMatchInlineSnapshot(`
      {
        "related": undefined,
        "table": "readable",
        "where": {
          "conditions": [
            {
              "type": "const",
              "value": true,
            },
            {
              "op": "EXISTS",
              "related": {
                "correlation": {
                  "childField": "id",
                  "op": "=",
                  "parentField": "readableId",
                },
                "subquery": {
                  "alias": "zsubq_6_readable",
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
