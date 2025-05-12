/* eslint-disable @typescript-eslint/naming-convention */
import {beforeEach, expect, test} from 'vitest';
import {formatPgInternalConvert} from './sql.ts';
import {
  boolean,
  enumeration,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {createSchema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {ServerSchema} from './schema.ts';
import {
  any,
  compile,
  distinctFrom,
  limit,
  makeCorrelator,
  makeJunctionJoin,
  orderBy,
  pullTablesForJunction,
  simple,
  type Spec,
} from './compiler.ts';
import {clientToServer} from '../../zero-schema/src/name-mapper.ts';

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

const enumTable = table('enumTable')
  .columns({
    id: string(),
    status: enumeration<'active' | 'inactive'>(),
  })
  .primaryKey('id');

const timestampsTable = table('timestampsTable')
  .columns({
    id: string(),
    timestampWithTz: number(),
    timestampWithoutTz: number(),
  })
  .primaryKey('id');

const alternateUser = table('alternate_user')
  .from('alternate_schema.user')
  .columns({
    id: string(),
    name: string(),
    age: number(),
  })
  .primaryKey('id');

const schema = createSchema({
  tables: [
    user,
    issue,
    issueLabel,
    label,
    parentTable,
    childTable,
    enumTable,
    timestampsTable,
    alternateUser,
  ],
});

const serverSchema: ServerSchema = {
  'user': {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
    age: {type: 'numeric', isEnum: false},
  },
  'issue': {
    id: {type: 'text', isEnum: false},
    title: {type: 'text', isEnum: false},
    description: {type: 'text', isEnum: false},
    closed: {type: 'boolean', isEnum: false},
    ownerId: {type: 'text', isEnum: false},
    created: {type: 'timestamp', isEnum: false},
  },
  'issueLabel': {
    issue_id: {type: 'text', isEnum: false},
    label_id: {type: 'text', isEnum: false},
  },
  'label': {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
  },
  'parentTable': {
    id: {type: 'text', isEnum: false},
    other_id: {type: 'text', isEnum: false},
  },
  'childTable': {
    id: {type: 'text', isEnum: false},
    parent_id: {type: 'text', isEnum: false},
    parent_other_id: {type: 'text', isEnum: false},
  },
  'enumTable': {
    id: {type: 'text', isEnum: false},
    status: {type: 'statusEnum', isEnum: true},
  },
  'timestampsTable': {
    id: {type: 'text', isEnum: false},
    timestampWithoutTz: {type: 'timestamp', isEnum: false},
    timestampWithTz: {type: 'timestamptz', isEnum: false},
  },
  'alternate_schema.user': {
    id: {type: 'text', isEnum: false},
    name: {type: 'text', isEnum: false},
    age: {type: 'numeric', isEnum: false},
  },
};

let spec: Spec;
beforeEach(() => {
  spec = {
    server: {
      schema: serverSchema,
      mapper: clientToServer(schema.tables),
    },
    aliasCount: 0,
    zql: schema.tables,
  };
});

test('limit', () => {
  expect(formatPgInternalConvert(limit(10))).toMatchInlineSnapshot(`
    {
      "text": "LIMIT $1::text::double precision",
      "values": [
        "10",
      ],
    }
  `);
  expect(formatPgInternalConvert(limit(undefined))).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('select from different schema', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'alternate_user',
        related: [],
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT "alternate_user_0"."id" as "id","alternate_user_0"."name" as "name","alternate_user_0"."age" as "age"
        FROM "alternate_schema"."user" AS "alternate_user_0"
         
         
        
        )"root"",
      "values": [],
    }
  `);
});

test('orderBy', () => {
  expect(
    formatPgInternalConvert(
      orderBy(spec, [], {
        zql: 'user',
        alias: 'user',
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "ORDER BY",
      "values": [],
    }
  `);
  expect(
    formatPgInternalConvert(
      orderBy(
        spec,
        [
          ['name', 'asc'],
          ['age', 'desc'],
        ],
        {
          zql: 'user',
          alias: 'user',
        },
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
      orderBy(
        spec,
        [
          ['name', 'asc'],
          ['age', 'desc'],
          ['id', 'asc'],
        ],
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "ORDER BY "user"."name" COLLATE "ucs_basic" ASC, "user"."age" DESC, "user"."id" COLLATE "ucs_basic" ASC",
      "values": [],
    }
  `);
  expect(
    formatPgInternalConvert(
      orderBy(spec, undefined, {
        zql: 'user',
        alias: 'user',
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);
});

test('compile with enum', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'enumTable',
        related: [],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'status'},
          right: {type: 'literal', value: 'active'},
        },
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT "enumTable_0"."id" as "id","enumTable_0"."status" as "status"
        FROM "enumTable" AS "enumTable_0"
        WHERE "enumTable_0"."status"::text = $1::text COLLATE "ucs_basic"
         
        
        )"root"",
      "values": [
        "active",
      ],
    }
  `);
});

test('compile with timestamp (with timezone)', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'timestampsTable',
        related: [],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'timestampWithTz'},
          right: {type: 'literal', value: 'abc'},
        },
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT "timestampsTable_0"."id" as "id",EXTRACT(EPOCH FROM "timestampsTable_0"."timestampWithTz") * 1000 as "timestampWithTz",EXTRACT(EPOCH FROM "timestampsTable_0"."timestampWithoutTz") * 1000 as "timestampWithoutTz"
        FROM "timestampsTable" AS "timestampsTable_0"
        WHERE "timestampsTable_0"."timestampWithTz" = to_timestamp($1::text::bigint / 1000.0)
         
        
        )"root"",
      "values": [
        ""abc"",
      ],
    }
  `);
});

test('compile with timestamp (without timezone)', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
        table: 'timestampsTable',
        related: [],
        where: {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'timestampWithoutTz'},
          right: {type: 'literal', value: 'abc'},
        },
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT "timestampsTable_0"."id" as "id",EXTRACT(EPOCH FROM "timestampsTable_0"."timestampWithTz") * 1000 as "timestampWithTz",EXTRACT(EPOCH FROM "timestampsTable_0"."timestampWithoutTz") * 1000 as "timestampWithoutTz"
        FROM "timestampsTable" AS "timestampsTable_0"
        WHERE "timestampsTable_0"."timestampWithoutTz" = to_timestamp($1::text::bigint / 1000.0) AT TIME ZONE 'UTC'
         
        
        )"root"",
      "values": [
        ""abc"",
      ],
    }
  `);
});

test('any', () => {
  expect(
    formatPgInternalConvert(
      any(
        spec,
        {
          type: 'simple',
          op: 'IN',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "(
      "user"."name" = ANY 
      (ARRAY(
          SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
        ))
    )",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      any(
        spec,
        {
          type: 'simple',
          op: 'NOT IN',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "NOT
        (
          "user"."name" = ANY 
          (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))
        )",
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
//       valuePosition(
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
//       valuePosition(
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
//       valuePosition(
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
  expect(
    formatPgInternalConvert(
      distinctFrom(
        spec,
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" IS NOT DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      distinctFrom(
        spec,
        {
          type: 'simple',
          op: 'IS NOT',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" IS DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);
});

test('correlate', () => {
  expect(
    formatPgInternalConvert(
      makeCorrelator(
        spec,
        [
          {
            table: {
              alias: 'parent_table',
              zql: 'parent_table',
            },
            zql: 'id',
          },
          {
            table: {
              alias: 'parent_table',
              zql: 'parent_table',
            },
            zql: 'other_id',
          },
        ],
        ['parent_id', 'parent_other_id'],
      )({
        alias: 'child_table',
        zql: 'child_table',
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""parent_table"."id" = "child_table"."parent_id" AND "parent_table"."other_id" = "child_table"."parent_other_id"",
      "values": [],
    }
  `);

  expect(
    formatPgInternalConvert(
      makeCorrelator(
        spec,
        [
          {
            table: {
              alias: 'parent_table',
              zql: 'parent_table',
            },
            zql: 'id',
          },
        ],
        ['parent_id'],
      )({
        alias: 'child_table',
        zql: 'child_table',
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""parent_table"."id" = "child_table"."parent_id"",
      "values": [],
    }
  `);

  expect(
    formatPgInternalConvert(
      makeCorrelator(
        spec,
        [],
        [],
      )({
        alias: 'child_table',
        zql: 'child_table',
      }),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "",
      "values": [],
    }
  `);

  expect(() =>
    formatPgInternalConvert(
      // mismatched field count
      makeCorrelator(
        spec,
        [
          {
            table: {
              alias: 'parent_table',
              zql: 'parent_table',
            },
            zql: 'id',
          },
          {
            table: {
              alias: 'parent_table',
              zql: 'parent_table',
            },
            zql: 'other_id',
          },
        ],
        ['parent_id'],
      )({
        alias: 'child_table',
        zql: 'child_table',
      }),
    ),
  ).toThrow('Assertion failed');
});

test('simple', () => {
  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '=',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: 'test'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" = $1::text COLLATE "ucs_basic"",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '!=',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: 'test'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" != $1::text COLLATE "ucs_basic"",
      "values": [
        "test",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '>',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."age" > $1::text::double precision",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '>=',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."age" >= $1::text::double precision",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '<',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."age" < $1::text::double precision",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: '<=',
          left: {type: 'column', name: 'age'},
          right: {type: 'literal', value: 21},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."age" <= $1::text::double precision",
      "values": [
        "21",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'LIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" LIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'NOT LIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" NOT LIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'ILIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" ILIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'NOT ILIKE',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: '%test%'},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" NOT ILIKE $1::text COLLATE "ucs_basic"",
      "values": [
        "%test%",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'IN',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "(
      "user"."id" = ANY 
      (ARRAY(
          SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
        ))
    )",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'NOT IN',
          left: {type: 'column', name: 'id'},
          right: {type: 'literal', value: [1, 2, 3]},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": "NOT
        (
          "user"."id" = ANY 
          (ARRAY(
              SELECT value::text COLLATE "ucs_basic" FROM jsonb_array_elements_text($1::text::jsonb)
            ))
        )",
      "values": [
        "[1,2,3]",
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'IS',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" IS NOT DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);

  expect(
    formatPgInternalConvert(
      simple(
        spec,
        {
          type: 'simple',
          op: 'IS NOT',
          left: {type: 'column', name: 'name'},
          right: {type: 'literal', value: null},
        },
        {
          zql: 'user',
          alias: 'user',
        },
      ),
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""user"."name" IS DISTINCT FROM $1::text COLLATE "ucs_basic"",
      "values": [
        null,
      ],
    }
  `);
});

test('pull tables for junction', () => {
  expect(
    pullTablesForJunction(spec, {
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
      {
        "correlation": {
          "childField": [
            "issue_id",
          ],
          "parentField": [
            "id",
          ],
        },
        "limit": undefined,
        "table": {
          "alias": "issue_label_0",
          "zql": "issue_label",
        },
      },
      {
        "correlation": {
          "childField": [
            "id",
          ],
          "parentField": [
            "label_id",
          ],
        },
        "limit": undefined,
        "table": {
          "alias": "label_1",
          "zql": "label",
        },
      },
    ]
  `);
});

test('make junction join', () => {
  expect(
    formatPgInternalConvert(
      makeJunctionJoin(spec, {
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
      }).join,
    ),
  ).toMatchInlineSnapshot(`
    {
      "text": ""issue_label" AS "issueLabel_0" JOIN "label" AS "label_1" ON "issueLabel_0"."label_id" = "label_1"."id"",
      "values": [],
    }
  `);
});

test('related thru junction edge', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
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
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT (
            SELECT COALESCE(json_agg(row_to_json("inner_labels")), '[]'::json) FROM (SELECT "label_2"."id" as "id","label_2"."name" as "name" FROM "issue_label" AS "issueLabel_1" JOIN "label" AS "label_2" ON "issueLabel_1"."label_id" = "label_2"."id" WHERE ("issue_0"."id" = "issueLabel_1"."issue_id")    ) "inner_labels"
          ) as "labels","issue_0"."id" as "id","issue_0"."title" as "title","issue_0"."description" as "description","issue_0"."closed" as "closed","issue_0"."ownerId" as "ownerId",EXTRACT(EPOCH FROM "issue_0"."created") * 1000 as "created"
        FROM "issue" AS "issue_0"
         
         
        
        )"root"",
      "values": [],
    }
  `);
});

test('related w/o junction edge', () => {
  expect(
    formatPgInternalConvert(
      compile(serverSchema, schema, {
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
      "text": "SELECT 
        COALESCE(json_agg(row_to_json("root")), '[]'::json)::text AS "zql_result"
        FROM (SELECT (
          SELECT COALESCE(json_agg(row_to_json("inner_owner")), '[]'::json) FROM (SELECT "user_1"."id" as "id","user_1"."name" as "name","user_1"."age" as "age"
        FROM "user" AS "user_1"
         
        WHERE "issue_0"."ownerId" = "user_1"."id"
        
        ) "inner_owner"
        ) as "owner","issue_0"."id" as "id","issue_0"."title" as "title","issue_0"."description" as "description","issue_0"."closed" as "closed","issue_0"."ownerId" as "ownerId",EXTRACT(EPOCH FROM "issue_0"."created") * 1000 as "created"
        FROM "issue" AS "issue_0"
         
         
        
        )"root"",
      "values": [],
    }
  `);
});
