import {describe, expect, test} from '@jest/globals';
import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {JSONValue} from 'postgres';
import {
  ASTWithPrimaryKeys,
  getNormalized,
  selectPrimaryKeysForExplosion,
} from './normalize.js';

describe('zql/normalize-query-hash', () => {
  type Case = {
    name: string;
    asts: AST[];
    query: string;
    values?: JSONValue[];
  };

  const cases: Case[] = [
    {
      name: 'simplest statement',
      asts: [
        {table: 'issues', select: [['id', 'alias']], orderBy: [['id'], 'asc']},
      ],
      query: 'SELECT id FROM issues ORDER BY id asc',
    },
    {
      name: 'column selection',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'id'],
            ['name', 'name'],
          ],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['name', 'ignored'],
            ['id', 'alias'],
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT id, name FROM issues ORDER BY id asc',
    },
    {
      name: 'aggregation, aliases ignored',
      asts: [
        {
          table: 'issues',
          aggregate: [{aggregate: 'count', alias: 'num'}],
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT count(*) AS "count(*)" FROM issues ORDER BY id asc',
    },
    {
      name: 'multiple aggregates',
      asts: [
        {
          table: 'issues',
          aggregate: [
            {aggregate: 'count', alias: 'num'},
            {aggregate: 'max', field: 'priority', alias: 'maxPri'},
          ],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          aggregate: [
            {aggregate: 'max', field: 'priority', alias: 'maxPri'},
            {aggregate: 'count', alias: 'num'},
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT count(*) AS "count(*)", max(priority) AS "max(priority)" ' +
        'FROM issues ORDER BY id asc',
    },
    {
      name: 'group by',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          groupBy: ['id', 'name'],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          groupBy: ['name', 'id'],
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT id, name FROM issues GROUP BY id, name ORDER BY id asc',
    },
    {
      name: 'group by, order by',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          groupBy: ['id', 'name'],
          orderBy: [['id', 'name'], 'desc'],
        },
      ],
      query:
        'SELECT id, name FROM issues GROUP BY id, name ORDER BY id, name desc',
    },
    {
      name: 'group by, order by, limit',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          groupBy: ['name', 'id'],
          // ORDER BY expression order must be preserved.
          orderBy: [['dueDate', 'priority'], 'desc'],
          limit: 10,
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'GROUP BY id, name ORDER BY "dueDate", priority desc LIMIT 10',
    },
    {
      name: 'group by, order by (ordering preserved), limit',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          groupBy: ['name', 'id'],
          // ORDER BY expression order must be preserved.
          orderBy: [['priority', 'dueDate'], 'desc'],
          limit: 10,
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'GROUP BY id, name ORDER BY priority, "dueDate" desc LIMIT 10',
    },
    {
      name: 'quoted identifiers',
      asts: [
        {
          table: 'camelCaseTable',
          select: [
            ['userID', 'u'],
            ['name', 'n'],
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT name, "userID" FROM "camelCaseTable" ORDER BY id asc',
    },
    {
      name: 'simple condition',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'simple',
            field: 'id',
            op: '=',
            value: {type: 'literal', value: 1234},
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT id, name FROM issues WHERE id = $1 ORDER BY id asc',
      values: [1234],
    },
    {
      name: 'simple condition (value types affect hash)',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'simple',
            field: 'id',
            op: '=',
            value: {type: 'literal', value: '1234'},
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT id, name FROM issues WHERE id = $1 ORDER BY id asc',
      values: ['1234'],
    },
    {
      name: 'multiple conditions',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: 'id',
                op: '=',
                value: {type: 'literal', value: 1234},
              },
              {
                type: 'simple',
                field: 'name',
                op: '=',
                value: {type: 'literal', value: 'foobar'},
              },
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 5},
              },
              {
                type: 'simple',
                field: 'priority',
                op: '>',
                value: {type: 'literal', value: 2},
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: 'priority',
                op: '>',
                value: {type: 'literal', value: 2},
              },
              {
                type: 'simple',
                field: 'id',
                op: '=',
                value: {type: 'literal', value: 1234},
              },
              {
                type: 'simple',
                field: 'name',
                op: '=',
                value: {type: 'literal', value: 'foobar'},
              },
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 5},
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE (id = $1 AND name = $2 AND priority < $3 AND priority > $4) ' +
        'ORDER BY id asc',
      values: [1234, 'foobar', 5, 2],
    },
    {
      name: 'empty conjunctions removed',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['priority', 'p'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['priority', 'p'],
          ],
          where: {
            type: 'conjunction',
            op: 'OR',
            conditions: [],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query: 'SELECT id, priority FROM issues ORDER BY id asc',
    },
    {
      name: 'multiple conditions with same fields and operator',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 5},
              },
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 3},
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 3},
              },
              {
                type: 'simple',
                field: 'priority',
                op: '<',
                value: {type: 'literal', value: 5},
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE (priority < $1 AND priority < $2) ' +
        'ORDER BY id asc',
      values: [3, 5],
    },
    {
      name: 'nested conditions',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: 'id',
                op: '=',
                value: {type: 'literal', value: 1234},
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
              {
                type: 'simple',
                field: 'id',
                op: '=',
                value: {type: 'literal', value: 1234},
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE (id = $1 AND (name = $2 OR priority > $3)) ' +
        'ORDER BY id asc',
      values: [1234, 'foobar', 2],
    },
    {
      name: 'equivalent nested conditions',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE ((name = $1 OR priority > $2) AND (name = $3 OR priority > $4)) ' +
        'ORDER BY id asc',
      values: ['foobar', 2, 'foobar', 2],
    },
    {
      name: 'conjunction comparison',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE ((id = $1 OR name = $2) AND (id = $3 OR priority > $4)) ' +
        'ORDER BY id asc',
      values: [1234, 'foobar', 1234, 2],
    },
    {
      name: 'conjunction fallback sorting to length',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: 'id',
                    op: '=',
                    value: {type: 'literal', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE ((id = $1 OR name = $2) AND (id = $3 OR name = $4 OR priority > $5)) ' +
        'ORDER BY id asc',
      values: [1234, 'foobar', 1234, 'foobar', 2],
    },
    {
      name: 'condition flattening',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: 'priority',
                    op: '>',
                    value: {type: 'literal', value: 2},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: 'a',
                        op: '=',
                        value: {type: 'literal', value: 'bc'},
                      },
                      {
                        type: 'conjunction',
                        op: 'OR',
                        conditions: [
                          {
                            type: 'simple',
                            field: 'doo',
                            op: '>',
                            value: {type: 'literal', value: '23'},
                          },
                          {
                            type: 'simple',
                            field: 'dah',
                            op: '<',
                            value: {type: 'literal', value: '56'},
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                type: 'simple',
                field: 'id',
                op: '=',
                value: {type: 'literal', value: 1234},
              },
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: 'foo',
                    op: '=',
                    value: {type: 'literal', value: 'bar'},
                  },
                  {
                    type: 'simple',
                    field: 'bar',
                    op: '>',
                    value: {type: 'literal', value: 23},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'conjunction',
                        op: 'AND',
                        conditions: [
                          {
                            type: 'simple',
                            field: 'zzz',
                            op: '!=',
                            value: {type: 'literal', value: 48},
                          },
                          {
                            type: 'simple',
                            field: 'xyz',
                            op: '!=',
                            value: {type: 'literal', value: 488},
                          },
                        ],
                      },
                      {
                        type: 'simple',
                        field: 'ac',
                        op: '>',
                        value: {type: 'literal', value: 'dc'},
                      },
                    ],
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
        {
          // AST with different but equivalent nesting of AND's and OR's
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: 'name',
                    op: '=',
                    value: {type: 'literal', value: 'foobar'},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: 'dah',
                        op: '<',
                        value: {type: 'literal', value: '56'},
                      },
                      {
                        type: 'conjunction',
                        op: 'OR',
                        conditions: [
                          {
                            type: 'simple',
                            field: 'doo',
                            op: '>',
                            value: {type: 'literal', value: '23'},
                          },
                          {
                            type: 'simple',
                            field: 'a',
                            op: '=',
                            value: {type: 'literal', value: 'bc'},
                          },
                        ],
                      },
                      {
                        // Empty Conjunctions should be removed.
                        type: 'conjunction',
                        op: 'AND',
                        conditions: [],
                      },
                    ],
                  },
                  {
                    // Single-condition conjunctions should also be flattened.
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'conjunction',
                        op: 'AND',
                        conditions: [
                          {
                            type: 'simple',
                            field: 'id',
                            op: '=',
                            value: {type: 'literal', value: 1234},
                          },
                        ],
                      },
                    ],
                  },
                  {
                    type: 'simple',
                    field: 'bar',
                    op: '>',
                    value: {type: 'literal', value: 23},
                  },
                ],
              },
              {
                type: 'simple',
                field: 'priority',
                op: '>',
                value: {type: 'literal', value: 2},
              },
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: 'foo',
                    op: '=',
                    value: {type: 'literal', value: 'bar'},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: 'ac',
                        op: '>',
                        value: {type: 'literal', value: 'dc'},
                      },
                      {
                        type: 'conjunction',
                        op: 'AND',
                        conditions: [
                          {
                            type: 'simple',
                            field: 'zzz',
                            op: '!=',
                            value: {type: 'literal', value: 48},
                          },
                          {
                            type: 'simple',
                            field: 'xyz',
                            op: '!=',
                            value: {type: 'literal', value: 488},
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          orderBy: [['id'], 'asc'],
        },
      ],
      query:
        'SELECT id, name FROM issues ' +
        'WHERE (bar > $1 AND foo = $2 AND id = $3 AND name = $4 AND priority > $5 AND ' +
        '(a = $6 OR dah < $7 OR doo > $8) AND (ac > $9 OR (xyz != $10 AND zzz != $11))) ' +
        'ORDER BY id asc',
      values: [23, 'bar', 1234, 'foobar', 2, 'bc', '56', '23', 'dc', 488, 48],
    },
  ];

  const allHashes = new Set<string>();
  for (const c of cases) {
    test(c.name, () => {
      const hashes = new Set<string>();
      c.asts.forEach(ast => {
        const normalized = getNormalized(ast);
        expect(normalized.query()).toEqual({
          query: c.query,
          values: c.values ?? [],
        });
        const h = normalized.hash();
        hashes.add(h);
        allHashes.add(h);
      });
      expect(hashes.size).toBe(1);
    });
  }
  test('unique hashes', () => {
    expect(allHashes.size).toBe(cases.length);
  });
});

describe('zql/selectPrimaryKeysForExplosion', () => {
  test.each([
    {
      name: 'simple select',
      ast: {
        table: 'issue',
        select: [
          ['id', 'i'],
          ['a', 'a'],
        ],
      },
      expected: {
        table: 'issue',
        select: [
          ['id', 'i'],
          ['a', 'a'],
        ],
        joins: [],
        primaryKeys: [
          {
            from: 'issue',
            column: 'id',
            as: {
              sourceTable: 'issue',
              reAlias: 1,
              column: 'id',
            },
          },
        ],
      },
    },
    {
      name: 'simple select with foreign key join',
      ast: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['owner.name', 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'user',
            },
            as: 'owner',
            on: ['issue.user_id', 'user.id'],
          },
        ],
      },
      expected: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['owner.name', 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'user',
            },
            as: 'owner',
            on: ['issue.user_id', 'user.id'],
          },
        ],
        primaryKeys: [
          {
            from: 'issue',
            column: 'id',
            as: {
              sourceTable: 'issue',
              reAlias: 1,
              column: 'id',
            },
          },
          {
            from: 'owner',
            column: 'id',
            as: {
              sourceTable: 'user',
              reAlias: 2,
              column: 'id',
            },
          },
        ],
      },
    },
    {
      name: 'self join',
      ast: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['parent.title', 'parent_title'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'issue',
            },
            as: 'parent',
            on: ['issue.parent_id', 'parent.id'],
          },
        ],
      },
      expected: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['parent.title', 'parent_title'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'issue',
            },
            as: 'parent',
            on: ['issue.parent_id', 'parent.id'],
          },
        ],
        primaryKeys: [
          {
            from: 'issue',
            column: 'id',
            as: {
              sourceTable: 'issue',
              reAlias: 1,
              column: 'id',
            },
          },
          {
            from: 'parent',
            column: 'id',
            as: {
              sourceTable: 'issue',
              reAlias: 2,
              column: 'id',
            },
          },
        ],
      },
    },
    {
      // SELECT issue.title, owner.name FROM issue
      //  JOIN user AS owner ON owner.id = issue.user_id
      //  JOIN (SELECT * FROM issue JOIN user AS owner ON owner.id = issue.user_id) AS parent
      //  ON parent.id = issue.parent_id
      // ->
      // SELECT issue.id as issue_1_id,
      //        owner.id as user_2_id,
      //        parent.issue_3_id as issue_3_id,
      //        parent.user_4_id as user_4_id,
      //  JOIN user AS owner ON owner.id = issue.user_id
      //  JOIN (SELECT issue.id as issue_3_id, owner.id as user_4_id FROM issue JOIN user AS owner ON owner.id = issue.user_id)
      //    AS parent
      name: 'join on a query rather than table',
      ast: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['owner.name', 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'user',
            },
            as: 'owner',
            on: ['issue.user_id', 'user.id'],
          },
          {
            type: 'inner',
            other: {
              table: 'issue',
              joins: [
                {
                  type: 'inner',
                  other: {
                    table: 'user',
                  },
                  as: 'owner',
                  on: ['issue.user_id', 'user.id'],
                },
              ],
            },
            as: 'parent',
            on: ['issue.parent_id', 'parent.id'],
          },
        ],
      },
      expected: {
        table: 'issue',
        select: [
          ['issue.title', 'title'],
          ['owner.name', 'name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'user',
            },
            as: 'owner',
            on: ['issue.user_id', 'user.id'],
          },
          {
            type: 'inner',
            other: {
              table: 'issue',
              joins: [
                {
                  type: 'inner',
                  other: {
                    table: 'user',
                  },
                  as: 'owner',
                  on: ['issue.user_id', 'user.id'],
                },
              ],
              primaryKeys: [
                {
                  from: 'issue',
                  column: 'id',
                  as: {
                    sourceTable: 'issue',
                    reAlias: 3,
                    column: 'id',
                  },
                },
                {
                  from: 'owner',
                  column: 'id',
                  as: {
                    sourceTable: 'user',
                    reAlias: 4,
                    column: 'id',
                  },
                },
              ],
            },
            as: 'parent',
            on: ['issue.parent_id', 'parent.id'],
          },
        ],
        primaryKeys: [
          {
            from: 'issue',
            column: 'id',
            as: {
              sourceTable: 'issue',
              reAlias: 1,
              column: 'id',
            },
          },
          {
            from: 'owner',
            column: 'id',
            as: {
              sourceTable: 'user',
              reAlias: 2,
              column: 'id',
            },
          },
          {
            from: 'parent',
            column: 'issue_3_id',
            as: {
              sourceTable: 'issue',
              reAlias: 3,
              column: 'id',
            },
          },
          {
            from: 'parent',
            column: 'user_4_id',
            as: {
              sourceTable: 'user',
              reAlias: 4,
              column: 'id',
            },
          },
        ],
      },
    },
  ] satisfies {
    name: string;
    ast: AST;
    expected: ASTWithPrimaryKeys;
  }[])('$name', ({ast, expected}) => {
    const result = selectPrimaryKeysForExplosion(ast, 1);
    expect(result).toEqual(expected);
  });
});
