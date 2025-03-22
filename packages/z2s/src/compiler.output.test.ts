/* eslint-disable @typescript-eslint/naming-convention */
import {expect, test} from 'vitest';
import {Compiler} from './compiler.ts';
import {formatPg} from './sql.ts';
import {
  boolean,
  number,
  string,
  table,
  timestamp,
} from '../../zero-schema/src/builder/table-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';

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
    created: timestamp(),
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

test('limit', () => {
  const compiler = new Compiler(schema.tables);
  expect(formatPg(compiler.limit(10))).toMatchInlineSnapshot(`
    {
      "text": "LIMIT $1",
      "values": [
        10,
      ],
    }
  `);
  expect(formatPg(compiler.limit(undefined))).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('orderBy', () => {
  const compiler = new Compiler(schema.tables);
  expect(formatPg(compiler.orderBy([], 'user'))).toMatchInlineSnapshot(`
    {
      "text": "ORDER BY",
      "values": [],
    }
  `);
  expect(
    formatPg(
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
      "text": "ORDER BY "user"."name" ASC, "user"."age" DESC",
      "values": [],
    }
  `);
  expect(
    formatPg(
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
      "text": "ORDER BY "user"."name" ASC, "user"."age" DESC, "user"."id" ASC",
      "values": [],
    }
  `);
  expect(formatPg(compiler.orderBy(undefined, 'user'))).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('any', () => {
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
      "text": ""name" = ANY ($1)",
      "values": [
        [
          1,
          2,
          3,
        ],
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" != ANY ($1)",
      "values": [
        [
          1,
          2,
          3,
        ],
      ],
    }
  `);
});

test('valuePosition', () => {
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(compiler.valuePosition({type: 'column', name: 'name'}, 'user')),
  ).toMatchInlineSnapshot(`
    {
      "text": ""name"",
      "values": [],
    }
  `);
  expect(
    formatPg(compiler.valuePosition({type: 'literal', value: 'hello'}, 'user')),
  ).toMatchInlineSnapshot(`
    {
      "text": "$1",
      "values": [
        "hello",
      ],
    }
  `);
  expect(() =>
    formatPg(
      compiler.valuePosition(
        {
          type: 'static',
          anchor: 'authData',
          field: 'name',
        },
        'user',
      ),
    ),
  ).toThrow(
    'Static parameters must be bound to a value before compiling to SQL',
  );
});

test('distinctFrom', () => {
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
      "text": ""name" IS NOT DISTINCT FROM $1",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" IS DISTINCT FROM $1",
      "values": [
        null,
      ],
    }
  `);
});

test('correlate', () => {
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
    formatPg(
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
    formatPg(
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
    formatPg(
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
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
      "text": ""name" = $1",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" != $1",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""age" > $1",
      "values": [
        21,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""age" >= $1",
      "values": [
        21,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""age" < $1",
      "values": [
        21,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""age" <= $1",
      "values": [
        21,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" LIKE $1",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" NOT LIKE $1",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" ILIKE $1",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" NOT ILIKE $1",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""id" = ANY ($1)",
      "values": [
        [
          1,
          2,
          3,
        ],
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""id" != ANY ($1)",
      "values": [
        [
          1,
          2,
          3,
        ],
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" IS NOT DISTINCT FROM $1",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPg(
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
      "text": ""name" IS DISTINCT FROM $1",
      "values": [
        null,
      ],
    }
  `);
});

test('pull tables for junction', () => {
  const compiler = new Compiler(schema.tables);
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
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
      "text": "SELECT COALESCE(json_agg(row_to_json("root")) , '[]'::json)::TEXT as "zql_result" FROM (SELECT (
            SELECT COALESCE(json_agg(row_to_json("inner_labels")) , '[]'::json) FROM (SELECT "table_1"."id","table_1"."name" FROM "issue_label" as "issueLabel" JOIN "label" as "table_1" ON "issueLabel"."label_id" = "table_1"."id" WHERE ("issue"."id" = "issueLabel"."issue_id")    ) "inner_labels"
          ) as "labels","issue"."id","issue"."title","issue"."description","issue"."closed","issue"."ownerId",EXTRACT(EPOCH FROM "issue"."created"::timestamp AT TIME ZONE 'UTC') * 1000 as "created" FROM "issue"    )"root"",
      "values": [],
    }
  `);
});

test('related w/o junction edge', () => {
  const compiler = new Compiler(schema.tables);
  expect(
    formatPg(
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
      "text": "SELECT COALESCE(json_agg(row_to_json("root")) , '[]'::json)::TEXT as "zql_result" FROM (SELECT (
          SELECT COALESCE(json_agg(row_to_json("inner_owner")) , '[]'::json) FROM (SELECT "user"."id","user"."name","user"."age" FROM "user"  WHERE ("issue"."ownerId" = "user"."id")  ) "inner_owner"
        ) as "owner","issue"."id","issue"."title","issue"."description","issue"."closed","issue"."ownerId",EXTRACT(EPOCH FROM "issue"."created"::timestamp AT TIME ZONE 'UTC') * 1000 as "created" FROM "issue"    )"root"",
      "values": [],
    }
  `);
});
