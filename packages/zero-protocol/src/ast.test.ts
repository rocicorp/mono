import {expect, test} from 'vitest';
import {h64} from '../../shared/src/hash.ts';
import {
  json,
  number,
  string,
  table,
} from '../../zero-schema/src/builder/table-builder.ts';
import {
  clientToServer,
  serverToClient,
} from '../../zero-schema/src/name-mapper.ts';
import type {AST, LiteralValue} from './ast.ts';
import {astSchema, mapAST, normalizeAST} from './ast.ts';
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
  expect(hash).toEqual('1n2euh7jf7r2y');
  expect(PROTOCOL_VERSION).toBe(52);
});

test('json path column reference: hashing and name mapping', () => {
  const ast = (path?: (string | number)[]): AST => ({
    table: 'issue',
    where: {
      type: 'simple',
      op: '=',
      left: path
        ? {type: 'json', value: {type: 'column', name: 'metadata'}, path}
        : {type: 'column', name: 'metadata'},
      right: {type: 'literal', value: 'x'},
    },
  });

  // The path participates in the AST hash (so otherwise-identical queries on
  // different paths are distinct queries).
  const h = (a: AST) => h64(JSON.stringify(normalizeAST(a))).toString(36);
  expect(h(ast(['a']))).not.toEqual(h(ast()));
  expect(h(ast(['a']))).not.toEqual(h(ast(['b'])));
  expect(h(ast(['a', 0]))).not.toEqual(h(ast(['a', 1])));
  expect(h(ast(['a']))).toEqual(h(ast(['a'])));

  // Name mapping rewrites the column name but leaves the JSON path (data, not
  // a schema name) untouched.
  const tables = {
    issue: table('issue')
      .from('issues')
      .columns({id: string(), metadata: json().from('meta_data')})
      .primaryKey('id')
      .build(),
  };
  const mapped = mapAST(ast(['registrar']), clientToServer(tables));
  expect(mapped.where).toEqual({
    type: 'simple',
    op: '=',
    left: {
      type: 'json',
      value: {type: 'column', name: 'meta_data'},
      path: ['registrar'],
    },
    right: {type: 'literal', value: 'x'},
  });
});

test('json path: numeric segments must be non-negative integer indices', () => {
  const ast = (path: (string | number)[]): AST => ({
    table: 'issue',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'json', value: {type: 'column', name: 'metadata'}, path},
      right: {type: 'literal', value: 'x'},
    },
  });
  // Accepted: object keys and non-negative integer array indices.
  expect(() => astSchema.parse(ast(['tags', 0, 'a']))).not.toThrow();
  // Rejected at the wire boundary (a hand-built AST can't bypass the builder's
  // check): a negative index would mean "from the end" on Postgres but null on
  // the client/SQLite, and a fractional index is not an index at all.
  expect(() => astSchema.parse(ast(['tags', -1]))).toThrow(
    /non-negative integer/,
  );
  expect(() => astSchema.parse(ast(['tags', 1.5]))).toThrow(
    /non-negative integer/,
  );
  // Beyond the safe-integer range an index stringifies as `1e+21`, which
  // SQLite rejects as a bad JSON path (a query error, not a non-match).
  expect(() => astSchema.parse(ast(['tags', 1e21]))).toThrow(
    /non-negative integer/,
  );
  // Beyond int32 SQLite wraps the index modulo 2^32 and Postgres `->` cannot
  // take it as an operand.
  expect(() => astSchema.parse(ast(['tags', 2 ** 31]))).toThrow(
    /non-negative integer/,
  );
  expect(() => astSchema.parse(ast(['tags', 2 ** 31 - 1]))).not.toThrow();
});

test('json path: IN lists must be homogeneous; json refs must wrap json columns', () => {
  const cond = (value: LiteralValue): AST => ({
    table: 'issue',
    where: {
      type: 'simple',
      op: 'IN',
      left: {
        type: 'json',
        value: {type: 'column', name: 'metadata'},
        path: ['k'],
      },
      right: {type: 'literal', value},
    },
  });
  expect(() => astSchema.parse(cond(['a', 'b']))).not.toThrow();
  expect(() => astSchema.parse(cond([1, 2]))).not.toThrow();
  // The engines compare the leaf against the type of the first element, so a
  // mixed list has no consistent meaning: rejected at the wire.
  expect(() => astSchema.parse(cond([1, 'a']))).toThrow(/one type/);

  // A JSON path on a non-json column would make the replica's json_type()
  // throw at fetch time; a type-aware mapper rejects it at the query boundary.
  const tables = {
    issue: table('issue')
      .columns({id: string(), title: string(), metadata: json()})
      .primaryKey('id')
      .build(),
  };
  const jsonRef = (column: string): AST => ({
    table: 'issue',
    where: {
      type: 'simple',
      op: '=',
      left: {type: 'json', value: {type: 'column', name: column}, path: ['k']},
      right: {type: 'literal', value: 'x'},
    },
  });
  expect(() =>
    mapAST(jsonRef('metadata'), clientToServer(tables)),
  ).not.toThrow();
  expect(() => mapAST(jsonRef('title'), clientToServer(tables))).toThrow(
    /not a json column/,
  );
});
