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
import type {AST, Condition} from './ast.ts';
import {astSchema, mapAST, normalizeAST, simplifyCondition} from './ast.ts';
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

const simple = (
  col: string,
  op: '=' | '!=' | 'IN' | 'IS' | '<' | '>',
  value: string | number | boolean | null | readonly (string | number)[],
): Condition => ({
  type: 'simple',
  op,
  left: {type: 'column', name: col},
  right: {type: 'literal', value},
});

test('simplify: removes duplicate siblings in AND and OR', () => {
  const dup: Condition = {
    type: 'and',
    conditions: [simple('a', '=', 1), simple('a', '=', 1)],
  };
  expect(simplifyCondition(dup)).toEqual(simple('a', '=', 1));

  const orDup: Condition = {
    type: 'or',
    conditions: [simple('a', '=', 1), simple('b', '=', 2), simple('a', '=', 1)],
  };
  // The two `a = 1` dedup to one; `a = 1` and `b = 2` differ, so no OR->IN.
  expect(simplifyCondition(orDup)).toEqual({
    type: 'or',
    conditions: [simple('a', '=', 1), simple('b', '=', 2)],
  });
});

test('simplify: OR of col = x and col = y consolidates to IN', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [simple('a', '=', 2), simple('a', '=', 1)],
  };
  expect(simplifyCondition(cond)).toEqual(simple('a', 'IN', [1, 2]));
});

test('simplify: OR merges col = x with existing col IN [...]', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [simple('a', 'IN', [2, 3]), simple('a', '=', 1)],
  };
  expect(simplifyCondition(cond)).toEqual(simple('a', 'IN', [1, 2, 3]));
});

test('simplify: OR merges two col IN [...] into one', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [simple('a', 'IN', [1, 2]), simple('a', 'IN', [2, 3])],
  };
  expect(simplifyCondition(cond)).toEqual(simple('a', 'IN', [1, 2, 3]));
});

test('simplify: OR->IN leaves unrelated branches alone', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [simple('a', '=', 1), simple('a', '=', 2), simple('b', '=', 3)],
  };
  const result = simplifyCondition(cond);
  expect(result).toEqual({
    type: 'or',
    conditions: [simple('a', 'IN', [1, 2]), simple('b', '=', 3)],
  });
});

test('simplify: col = NULL is not eligible for OR->IN', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [simple('a', '=', null), simple('a', '=', 1)],
  };
  // No consolidation: `a = NULL` skipped, leaving one eligible branch.
  expect(simplifyCondition(cond)).toEqual({
    type: 'or',
    conditions: [simple('a', '=', 1), simple('a', '=', null)],
  });
});

test('simplify: absorption A AND (A OR B) -> A', () => {
  const cond: Condition = {
    type: 'and',
    conditions: [
      simple('a', '=', 1),
      {
        type: 'or',
        conditions: [simple('a', '=', 1), simple('b', '=', 2)],
      },
    ],
  };
  expect(simplifyCondition(cond)).toEqual(simple('a', '=', 1));
});

test('simplify: absorption A OR (A AND B) -> A', () => {
  const cond: Condition = {
    type: 'or',
    conditions: [
      simple('a', '=', 1),
      {
        type: 'and',
        conditions: [simple('a', '=', 1), simple('b', '=', 2)],
      },
    ],
  };
  expect(simplifyCondition(cond)).toEqual(simple('a', '=', 1));
});

test('simplify: absorption with compound siblings', () => {
  // (a AND b) OR ((a AND b) AND c) -> (a AND b)
  const ab: Condition = {
    type: 'and',
    conditions: [simple('a', '=', 1), simple('b', '=', 2)],
  };
  const cond: Condition = {
    type: 'or',
    conditions: [
      ab,
      {type: 'and', conditions: [...ab.conditions, simple('c', '=', 3)]},
    ],
  };
  // Nested AND is flattened and absorbed because its sub-condition matches
  // the `a = 1` sibling... wait, `a = 1` is not a sibling of the OR. The
  // sibling is the flat AND (a AND b). Absorption matches `a AND b` as a
  // whole against the inner AND's `a, b, c` children? No — absorption only
  // drops the inner compound if a whole sibling equals one of its sub-conds.
  // The inner AND's sub-conditions are [a, b, c], and no OR sibling equals
  // any of those individually. So we expect no absorption; both branches
  // remain.
  const result = simplifyCondition(cond);
  expect(result).toEqual({
    type: 'or',
    conditions: [
      ab,
      {
        type: 'and',
        conditions: [
          simple('a', '=', 1),
          simple('b', '=', 2),
          simple('c', '=', 3),
        ],
      },
    ],
  });
});

test('simplify: flattens nested same-type compounds', () => {
  const cond: Condition = {
    type: 'and',
    conditions: [
      {
        type: 'and',
        conditions: [simple('a', '=', 1), simple('b', '=', 2)],
      },
      simple('c', '=', 3),
    ],
  };
  expect(simplifyCondition(cond)).toEqual({
    type: 'and',
    conditions: [simple('a', '=', 1), simple('b', '=', 2), simple('c', '=', 3)],
  });
});

test('simplify: empty compound returns undefined, singleton unwraps', () => {
  expect(simplifyCondition({type: 'and', conditions: []})).toBeUndefined();
  expect(simplifyCondition({type: 'or', conditions: []})).toBeUndefined();
  expect(
    simplifyCondition({type: 'and', conditions: [simple('a', '=', 1)]}),
  ).toEqual(simple('a', '=', 1));
});

test('simplify: idempotent', () => {
  const cond: Condition = {
    type: 'and',
    conditions: [
      simple('x', '=', 1),
      {
        type: 'or',
        conditions: [
          simple('a', '=', 1),
          simple('a', '=', 2),
          simple('a', '=', 3),
        ],
      },
      simple('x', '=', 1),
    ],
  };
  const once = simplifyCondition(cond);
  const twice = simplifyCondition(once!);
  expect(twice).toEqual(once);
  // And the expected shape: dedup of `x = 1`, OR->IN of the 'a' branch.
  // Sorted by left column: 'a' before 'x'.
  expect(once).toEqual({
    type: 'and',
    conditions: [simple('a', 'IN', [1, 2, 3]), simple('x', '=', 1)],
  });
});

test('simplify: does not consolidate static parameters', () => {
  const staticParam: Condition = {
    type: 'simple',
    op: '=',
    left: {type: 'column', name: 'a'},
    right: {type: 'static', anchor: 'authData', field: 'userId'},
  };
  const cond: Condition = {
    type: 'or',
    conditions: [staticParam, simple('a', '=', 1)],
  };
  // The static-param branch is not eligible, so only one eligible branch
  // remains -> no consolidation.
  const result = simplifyCondition(cond);
  expect(result).toEqual({
    type: 'or',
    conditions: [simple('a', '=', 1), staticParam],
  });
});

test('protocol version', () => {
  const schemaJSON = JSON.stringify(astSchema);
  const hash = h64(schemaJSON).toString(36);

  // If this test fails because the AST schema has changed such that
  // old code will not understand the new schema, bump the
  // PROTOCOL_VERSION and update the expected values.
  expect(hash).toEqual('1dsf0svqtvyhv');
  expect(PROTOCOL_VERSION).toBe(50);
});
