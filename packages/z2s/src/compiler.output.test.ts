/* eslint-disable @typescript-eslint/naming-convention */
import {expect, test} from 'vitest';
import {Compiler} from './compiler.ts';
import {formatPgInternalConvert} from './sql.ts';
import {
  boolean,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {ServerSchema} from './schema.ts';

// Tests the output of basic primitives.
// Top-level things like `SELECT` are tested by actually executing the SQL as inspecting
// the output there is not easy and not as useful when we know each sub-component is generating
// the correct output.

const user = table('user')
  .columns({
    id: string(),
    name: string(),
    age: number(),
  })
  .primaryKey('id');

const issue = table('issue')
  .columns({
    id: string(),
    title: string(),
    description: string(),
    closed: boolean(),
    ownerId: string().optional(),
    created: number(),
  })
  .primaryKey('id');

const issueLabel = table('issueLabel')
  .from('issue_label')
  .columns({
    issueId: string().from('issue_id'),
    labelId: string().from('label_id'),
  })
  .primaryKey('issueId', 'labelId');

const label = table('label')
  .columns({
    id: string(),
    name: string(),
  })
  .primaryKey('id');

const parentTable = table('parent_table')
  .columns({
    id: string(),
    other_id: string(),
  })
  .primaryKey('id');

const childTable = table('child_table')
  .columns({
    id: string(),
    parent_id: string(),
    parent_other_id: string(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [user, issue, issueLabel, label, parentTable, childTable],
});

const serverSchema: ServerSchema = {
  user: {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
    age: {type: 'numeric', isEnum: false},
  },
  issue: {
    id: {type: 'text', isEnum: false},
    title: {type: 'text', isEnum: false},
    description: {type: 'text', isEnum: false},
    closed: {type: 'boolean', isEnum: false},
    ownerId: {type: 'text', isEnum: false},
    created: {type: 'timestamp', isEnum: false},
  },
  issueLabel: {
    issue_id: {type: 'text', isEnum: false},
    label_id: {type: 'text', isEnum: false},
  },
  label: {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
  },
  parentTable: {
    id: {type: 'text', isEnum: false},
    other_id: {type: 'text', isEnum: false},
  },
  childTable: {
    id: {type: 'text', isEnum: false},
    parent_id: {type: 'text', isEnum: false},
    parent_other_id: {type: 'text', isEnum: false},
  },
};

test('limit', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(formatPgInternalConvert(compiler.limit(10))).toMatchInlineSnapshot(`
    {
      "text": "LIMIT $1::text::numeric",
      "values": [
        "10",
      ],
    }
  `);
  expect(formatPgInternalConvert(compiler.limit(undefined)))
    .toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('orderBy', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(formatPgInternalConvert(compiler.orderBy([], 'user')))
    .toMatchInlineSnapshot(`
    {
      "text": "ORDER BY",
      "values": [],
    }
  `);
  expect(
    formatPgInternalConvert(
      compiler.orderBy(
        [
          ['name', 'asc'],
          ['age', 'desc'],
        ],
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "ORDER BY "user"."name" COLLATE "ucs_basic" ASC, "user"."age" DESC",
      "values": [],
    }
  `);
  expect(
    formatPgInternalConvert(
      compiler.orderBy(
        [
          ['name', 'asc'],
          ['age', 'desc'],
          ['id', 'asc'],
        ],
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "ORDER BY "user"."name" COLLATE "ucs_basic" ASC, "user"."age" DESC, "user"."id" COLLATE "ucs_basic" ASC",
      "values": [],
    }
  `);
  expect(formatPgInternalConvert(compiler.orderBy(undefined, 'user')))
    .toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('any', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.any(
        {
          type: 'simple',
          op: 'IN',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" = ANY (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.any(
        {
          type: 'simple',
          op: 'NOT IN',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" != ANY (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))",
      "values": [
        "[1,2,3]",
      ],
    }
  `);
});

// test('valuePosition', () => {
//   const compiler = new Compiler(schema.tables, serverSchema);
//   expect(
//     formatPgInternalConvert(
//       compiler.valuePosition(
//         {type: 'column', name: 'name'},
//         'user',
//         'string',
//         false,
//       ),
//     ),
//   ).toMatchInlineSnapshot(`
//     {
//       "text": ""name"",
//       "values": [],
//     }
//   `);
//   expect(
//     formatPgInternalConvert(
//       compiler.valuePosition(
//         {type: 'literal', value: 'hello'},
//         'user',
//         'string',
//         false,
//       ),
//     ),
//   ).toMatchInlineSnapshot(`
//     {
//       "text": "$1::text",
//       "values": [
//         "hello",
//       ],
//     }
//   `);
//   expect(() =>
//     formatPgInternalConvert(
//       compiler.valuePosition(
//         {
//           type: 'static',
//           anchor: 'authData',
//           field: 'name',
//         },
//         'user',
//         'string',
//         false,
//       ),
//     ),
//   ).toThrow(
//     'Static parameters must be bound to a value before compiling to SQL',
//   );
// });

test('distinctFrom', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.distinctFrom(
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" IS NOT DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.distinctFrom(
        {
          type: 'simple',
          op: 'IS NOT',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" IS DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);
});

test('correlate', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.correlate(
        'parent_table',
        'parent_table',
        ['id', 'other_id'],
        'child_table',
        'child_table',
        ['parent_id', 'parent_other_id'],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""parent_table"."id" = "child_table"."parent_id" AND "parent_table"."other_id" = "child_table"."parent_other_id"",
      "values": [],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.correlate(
        'parent_table',
        'parent_table',
        ['id'],
        'child_table',
        'child_table',
        ['parent_id'],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""parent_table"."id" = "child_table"."parent_id"",
      "values": [],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.correlate(
        'parent_table',
        'parent_table',
        [],
        'child_table',
        'child_table',
        [],
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);

  expect(() =>
    formatPgInternalConvert(
      compiler.correlate(
        'parent_table',
        'parent_table',
        ['id', 'other_id'],
        'child_table',
        'child_table',
        ['parent_id'],
      ),
    ),
  ).toThrow('Assertion failed');
});

test('simple', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: 'test'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" = $1::text COLLATE "ucs_basic"",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '!=',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: 'test'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" != $1::text COLLATE "ucs_basic"",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '>',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""age" > $1::text::numeric",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '>=',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""age" >= $1::text::numeric",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '<',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""age" < $1::text::numeric",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: '<=',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""age" <= $1::text::numeric",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'LIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" LIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'NOT LIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" NOT LIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'ILIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" ILIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'NOT ILIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" NOT ILIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'IN',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""id" = ANY (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'NOT IN',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""id" != ANY (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" IS NOT DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      compiler.simple(
        {
          type: 'simple',
          op: 'IS NOT',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        'user',
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name" IS DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);
});

test('pull tables for junction', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    compiler.pullTablesForJunction({
      correlation: {
        parentField: ['id'],
        childField: ['issue_id'],
      },
      subquery: {
        table: 'issue_label',
        alias: 'labels',
        related: [
          {
            correlation: {
              parentField: ['label_id'],
              childField: ['id'],
            },
            subquery: {
              table: 'label',
              alias: 'labels',
            },
          },
        ],
      },
    }),
  ).toMatchInlineSnapshot(`
    [
      [
        "issue_label",
        {
          "childField": [
            "issue_id",
          ],
          "parentField": [
            "id",
          ],
        },
        undefined,
      ],
      [
        "label",
        {
          "childField": [
            "id",
          ],
          "parentField": [
            "label_id",
          ],
        },
        undefined,
      ],
    ]
  `);
});

test('make junction join', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.makeJunctionJoin({
        correlation: {
          parentField: ['id'],
          childField: ['issueId'],
        },
        subquery: {
          table: 'issueLabel',
          alias: 'labels',
          related: [
            {
              correlation: {
                parentField: ['labelId'],
                childField: ['id'],
              },
              subquery: {
                table: 'label',
                alias: 'labels',
              },
            },
          ],
        },
      })[0],
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""issue_label" as "issueLabel" JOIN "label" as "table_1" ON "issueLabel"."label_id" = "table_1"."id"",
      "values": [],
    }
  `);
});

test('related thru junction edge', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.compile({
        table: 'issue',
        related: [
          {
            correlation: {
              parentField: ['id'],
              childField: ['issueId'],
            },
            hidden: true,
            subquery: {
              table: 'issueLabel',
              alias: 'labels',
              related: [
                {
                  correlation: {
                    parentField: ['labelId'],
                    childField: ['id'],
                  },
                  subquery: {
                    table: 'label',
                    alias: 'labels',
                  },
                },
              ],
            },
          },
        ],
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT COALESCE(json_agg(row_to_json("root")), '[]'::json)::text as "zql_result" FROM (SELECT (
            SELECT COALESCE(json_agg(row_to_json("inner_labels")), '[]'::json) FROM (SELECT "table_1"."id","table_1"."name" FROM "issue_label" as "issueLabel" JOIN "label" as "table_1" ON "issueLabel"."label_id" = "table_1"."id" WHERE ("issue"."id" = "issueLabel"."issue_id")    ) "inner_labels"
          ) as "labels","issue"."id","issue"."title","issue"."description","issue"."closed","issue"."ownerId",EXTRACT(EPOCH FROM "issue"."created"::timestamp AT TIME ZONE 'UTC') * 1000 as "created" FROM "issue"    )"root"",
      "values": [],
    }
  `);
});

test('related w/o junction edge', () => {
  const compiler = new Compiler(schema.tables, serverSchema);
  expect(
    formatPgInternalConvert(
      compiler.compile({
        table: 'issue',
        related: [
          {
            correlation: {
              parentField: ['ownerId'],
              childField: ['id'],
            },
            subquery: {
              table: 'user',
              alias: 'owner',
            },
          },
        ],
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT COALESCE(json_agg(row_to_json("root")), '[]'::json)::text as "zql_result" FROM (SELECT (
          SELECT COALESCE(json_agg(row_to_json("inner_owner")) , '[]'::json) FROM (SELECT "user"."id","user"."name","user"."age" FROM "user"  WHERE ("issue"."ownerId" = "user"."id")  ) "inner_owner"
        ) as "owner","issue"."id","issue"."title","issue"."description","issue"."closed","issue"."ownerId",EXTRACT(EPOCH FROM "issue"."created"::timestamp AT TIME ZONE 'UTC') * 1000 as "created" FROM "issue"    )"root"",
      "values": [],
    }
  `);
});
