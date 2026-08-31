import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.ts';
import {
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import type {AST, Condition, LiteralValue} from './ast.ts';
import {astSchema, mapAST, normalizeAST, normalizedAST} from './ast.ts';
import {PROTOCOL_VERSION} from './protocol-version.ts';

test('fields are placed into correct positions', () => {
  function normalizeAndStringify(ast: AST) {
    return JSON.stringify(normalizeAST(ast));
  }

  expect(
    normalizeAndStringify({
      alias: 'alias',
      table: 'table',
    }),
  ).toEqual(
    normalizeAndStringify({
      table: 'table',
      alias: 'alias',
    }),
  );

  expect(
    normalizeAndStringify({
      schema: 'schema',
      alias: 'alias',
      limit: 10,
      orderBy: [],
      related: [],
      where: undefined,
      table: 'table',
    }),
  ).toEqual(
    normalizeAndStringify({
      related: [],
      schema: 'schema',
      limit: 10,
      table: 'table',
      orderBy: [],
      where: undefined,
      alias: 'alias',
    }),
  );
});

test('conditions are sorted', () => {
  let ast: AST = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'b'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'value'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'b'},
        op: '=',
        right: {type: 'literal', value: 'value'},
      },
    ],
  });

  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'y'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '=',
          right: {type: 'literal', value: 'x'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'x'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '=',
        right: {type: 'literal', value: 'y'},
      },
    ],
  });

  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '<',
          right: {type: 'literal', value: 'x'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'a'},
          op: '>',
          right: {type: 'literal', value: 'y'},
        },
      ],
    },
  };

  expect(normalizeAST(ast).where).toEqual({
    type: 'and',
    conditions: [
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '<',
        right: {type: 'literal', value: 'x'},
      },
      {
        type: 'simple',
        left: {type: 'column', name: 'a'},
        op: '>',
        right: {type: 'literal', value: 'y'},
      },
    ],
  });

  // correlatedSubquery conditions differing only in flip sort deterministically
  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          flip: true,
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          flip: false,
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
      ],
    },
  };

  const flips = (
    normalizeAST(ast).where as unknown as {conditions: {flip?: boolean}[]}
  ).conditions.map(c => c.flip);
  // undefined < false < true
  expect(flips).toEqual([undefined, false, true]);

  // correlatedSubquery conditions differing only in scalar sort deterministically
  ast = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: true,
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
        {
          type: 'correlatedSubquery',
          op: 'EXISTS',
          scalar: false,
          related: {
            correlation: {parentField: ['id'], childField: ['id']},
            subquery: {table: 'other', alias: 'zsubq_rel'},
          },
        },
      ],
    },
  };

  const scalars = (
    normalizeAST(ast).where as unknown as {conditions: {scalar?: boolean}[]}
  ).conditions.map(c => c.scalar);
  // undefined < false < true
  expect(scalars).toEqual([undefined, false, true]);
});

test('related subqueries are sorted', () => {
  const ast: AST = {
    table: 'table',
    related: [
      {
        correlation: {parentField: ['a'], childField: ['a']},
        system: 'client',
        subquery: {
          table: 'table',
          alias: 'alias2',
        },
      },
      {
        correlation: {parentField: ['a'], childField: ['a']},
        system: 'client',
        subquery: {
          table: 'table',
          alias: 'alias1',
        },
      },
    ],
  };

  expect(normalizeAST(ast).related).toMatchInlineSnapshot(`
    [
      {
        "correlation": {
          "childField": [
            "a",
          ],
          "parentField": [
            "a",
          ],
        },
        "hidden": undefined,
        "subquery": {
          "alias": "alias1",
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "table",
          "where": undefined,
        },
        "system": "client",
      },
      {
        "correlation": {
          "childField": [
            "a",
          ],
          "parentField": [
            "a",
          ],
        },
        "hidden": undefined,
        "subquery": {
          "alias": "alias2",
          "limit": undefined,
          "orderBy": undefined,
          "related": undefined,
          "schema": undefined,
          "start": undefined,
          "table": "table",
          "where": undefined,
        },
        "system": "client",
      },
    ]
  `);
});

test('makeServerAST', () => {
  const ast: AST = {
    table: 'issue',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
        {
          type: 'simple',
          left: {type: 'column', name: 'ownerId'},
          op: '=',
          right: {type: 'literal', value: 'value'},
        },
        {
          type: 'correlatedSubquery',
          related: {
            correlation: {parentField: ['id'], childField: ['issueId']},
            system: 'client',
            subquery: {
              table: 'comment',
              alias: 'alias2',
            },
          },
          op: 'EXISTS',
        },
      ],
    },
    related: [
      {
        correlation: {parentField: ['id'], childField: ['issueId']},
        system: 'client',
        subquery: {
          table: 'comment',
          alias: 'alias2',
        },
      },
      {
        correlation: {parentField: ['ownerId'], childField: ['id']},
        system: 'client',
        subquery: {
          table: 'user',
          alias: 'alias1',
        },
      },
    ],
    start: {row: {id: '123'}, exclusive: true},
    orderBy: [
      ['modified', 'desc'],
      ['id', 'asc'],
    ],
  };

  const tables = {
    issue: table('issue')
      .from('issues')
      .columns({
        id: string().from('issue_id'),
        ownerId: string().from('owner_id'),
        modified: number(),
      })
      .primaryKey('id')
      .build(),

    comment: table('comment')
      .from('comments')
      .columns({
        id: string().from('comment_id'),
        issueId: string().from('issue_id'),
      })
      .primaryKey('id')
      .build(),

    user: table('user')
      .from('users')
      .columns({
        id: string().from('user_id'),
      })
      .primaryKey('id')
      .build(),
  };
  const serverAST = mapAST(ast, clientToServer(tables));

  const json = JSON.stringify(serverAST);
  expect(json).toMatch(/"issues"/);
  expect(json).toMatch(/"comments"/);
  expect(json).toMatch(/"users"/);
  expect(json).toMatch(/"issue_id"/);
  expect(json).toMatch(/"user_id"/);
  expect(json).toMatch(/"owner_id"/);
  expect(json).not.toMatch(/"issue"/);
  expect(json).not.toMatch(/"comment"/);
  expect(json).not.toMatch(/"user"/);
  expect(json).not.toMatch(/"id"/);
  expect(json).not.toMatch(/"ownerId"/);
  expect(json).not.toMatch(/"commentId"/);

  expect(serverAST).toMatchInlineSnapshot(`
    {
      "alias": undefined,
      "limit": undefined,
      "orderBy": [
        [
          "modified",
          "desc",
        ],
        [
          "issue_id",
          "asc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "issue_id",
            ],
            "parentField": [
              "issue_id",
            ],
          },
          "hidden": undefined,
          "subquery": {
            "alias": "alias2",
            "limit": undefined,
            "orderBy": undefined,
            "related": undefined,
            "schema": undefined,
            "start": undefined,
            "table": "comments",
            "where": undefined,
          },
          "system": "client",
        },
        {
          "correlation": {
            "childField": [
              "user_id",
            ],
            "parentField": [
              "owner_id",
            ],
          },
          "hidden": undefined,
          "subquery": {
            "alias": "alias1",
            "limit": undefined,
            "orderBy": undefined,
            "related": undefined,
            "schema": undefined,
            "start": undefined,
            "table": "users",
            "where": undefined,
          },
          "system": "client",
        },
      ],
      "schema": undefined,
      "start": {
        "exclusive": true,
        "row": {
          "issue_id": "123",
        },
      },
      "table": "issues",
      "where": {
        "conditions": [
          {
            "left": {
              "name": "issue_id",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": "value",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "owner_id",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": "value",
            },
            "type": "simple",
          },
          {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "issue_id",
                ],
                "parentField": [
                  "issue_id",
                ],
              },
              "subquery": {
                "alias": "alias2",
                "limit": undefined,
                "orderBy": undefined,
                "related": undefined,
                "schema": undefined,
                "start": undefined,
                "table": "comments",
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

  const clientAST = mapAST(serverAST, serverToClient(tables));
  expect(clientAST).toEqual(ast);
  expect(clientAST).toMatchInlineSnapshot(`
    {
      "alias": undefined,
      "limit": undefined,
      "orderBy": [
        [
          "modified",
          "desc",
        ],
        [
          "id",
          "asc",
        ],
      ],
      "related": [
        {
          "correlation": {
            "childField": [
              "issueId",
            ],
            "parentField": [
              "id",
            ],
          },
          "hidden": undefined,
          "subquery": {
            "alias": "alias2",
            "limit": undefined,
            "orderBy": undefined,
            "related": undefined,
            "schema": undefined,
            "start": undefined,
            "table": "comment",
            "where": undefined,
          },
          "system": "client",
        },
        {
          "correlation": {
            "childField": [
              "id",
            ],
            "parentField": [
              "ownerId",
            ],
          },
          "hidden": undefined,
          "subquery": {
            "alias": "alias1",
            "limit": undefined,
            "orderBy": undefined,
            "related": undefined,
            "schema": undefined,
            "start": undefined,
            "table": "user",
            "where": undefined,
          },
          "system": "client",
        },
      ],
      "schema": undefined,
      "start": {
        "exclusive": true,
        "row": {
          "id": "123",
        },
      },
      "table": "issue",
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
              "value": "value",
            },
            "type": "simple",
          },
          {
            "left": {
              "name": "ownerId",
              "type": "column",
            },
            "op": "=",
            "right": {
              "type": "literal",
              "value": "value",
            },
            "type": "simple",
          },
          {
            "op": "EXISTS",
            "related": {
              "correlation": {
                "childField": [
                  "issueId",
                ],
                "parentField": [
                  "id",
                ],
              },
              "subquery": {
                "alias": "alias2",
                "limit": undefined,
                "orderBy": undefined,
                "related": undefined,
                "schema": undefined,
                "start": undefined,
                "table": "comment",
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

test('protocol version', () => {
  const schemaJSON = JSON.stringify(astSchema);
  const hash = h64(schemaJSON).toString(36);

  // If this test fails because the AST schema has changed such that
  // old code will not understand the new schema, bump the
  // PROTOCOL_VERSION and update the expected values.
  expect(hash).toEqual('1dsf0svqtvyhv');
  expect(PROTOCOL_VERSION).toBe(52);
});

test('normalizedAST matches normalizeAST', () => {
  const ast: AST = {
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        {
          type: 'simple',
          left: {type: 'column', name: 'b'},
          op: '=',
          right: {type: 'literal', value: 1},
        },
        {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'a'},
              op: '=',
              right: {type: 'literal', value: 2},
            },
          ],
        },
      ],
    },
    related: [
      {
        correlation: {parentField: ['a'], childField: ['a']},
        subquery: {table: 'table', alias: 'b'},
      },
      {
        correlation: {parentField: ['a'], childField: ['a']},
        subquery: {table: 'table', alias: 'a'},
      },
    ],
    limit: 10,
  };

  // The subqueries of this AST are already normalized, so normalizing a single
  // level is enough.
  expect(JSON.stringify(normalizedAST(ast))).toBe(
    JSON.stringify(normalizeAST(ast)),
  );
});

test('normalizing a normalized AST changes nothing', () => {
  const normalized = normalizedAST({
    schema: 'schema',
    table: 'table',
    alias: 'alias',
    where: {
      type: 'simple',
      left: {type: 'column', name: 'a'},
      op: '=',
      right: {type: 'literal', value: 1},
    },
    limit: 10,
    orderBy: [['a', 'asc']],
  });
  expect(JSON.stringify(normalizeAST(normalized))).toBe(
    JSON.stringify(normalized),
  );
  expect(normalizeAST(normalized)).toEqual(normalized);
});

test('empty conjunctions are preserved', () => {
  // An empty 'and' is always true and an empty 'or' is always false, so
  // neither can be dropped.
  for (const type of ['and', 'or'] as const) {
    expect(
      normalizeAST({table: 'table', where: {type, conditions: []}}).where,
    ).toEqual({type, conditions: []});
  }

  // ... but a nested conjunction of the same type still collapses.
  expect(
    normalizeAST({
      table: 'table',
      where: {
        type: 'and',
        conditions: [
          {
            type: 'simple',
            left: {type: 'column', name: 'a'},
            op: '=',
            right: {type: 'literal', value: 1},
          },
          {type: 'and', conditions: []},
        ],
      },
    }).where,
  ).toEqual({
    type: 'simple',
    left: {type: 'column', name: 'a'},
    op: '=',
    right: {type: 'literal', value: 1},
  });
});

test('static parameters are ordered', () => {
  const cond = (
    anchor: 'authData' | 'preMutationRow',
    field: string | string[],
  ) =>
    ({
      type: 'simple',
      left: {type: 'static', anchor, field},
      op: '=',
      right: {type: 'literal', value: 1},
    }) as const;

  const {where} = normalizeAST({
    table: 'table',
    where: {
      type: 'and',
      conditions: [
        cond('preMutationRow', 'a'),
        cond('authData', ['a', 'b']),
        cond('authData', 'b'),
        cond('authData', 'a,b'),
        cond('authData', 'a'),
      ],
    },
  });

  expect(where).toEqual({
    type: 'and',
    conditions: [
      // Strings before paths, and 'a,b' is not the same field as ['a', 'b'].
      cond('authData', 'a'),
      cond('authData', 'a,b'),
      cond('authData', 'b'),
      cond('authData', ['a', 'b']),
      cond('preMutationRow', 'a'),
    ],
  });
});

test('values of different types are ordered', () => {
  const cond = (value: LiteralValue) =>
    ({
      type: 'simple',
      left: {type: 'column', name: 'a'},
      op: '=',
      right: {type: 'literal', value},
    }) as const;

  // Values whose string form is the same, and which therefore used to be
  // ordered by the order they were written in.
  const values: LiteralValue[] = [
    null,
    true,
    1,
    '1',
    'a,b',
    ['a', 'b'],
    ['a,b'],
  ];
  const conditions = values.map(cond);

  const normalized = (conditions: Condition[]) =>
    JSON.stringify(
      normalizeAST({table: 'table', where: {type: 'and', conditions}}).where,
    );

  // The order the conditions are written in does not matter.
  expect(normalized(conditions.toReversed())).toBe(normalized(conditions));

  expect(
    normalizeAST({table: 'table', where: {type: 'and', conditions}}).where,
  ).toEqual({
    type: 'and',
    conditions: values.map(cond),
  });
});
