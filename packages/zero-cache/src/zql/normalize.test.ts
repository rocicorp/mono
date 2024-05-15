import type {AST} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {JSONValue} from 'postgres';
import {describe, expect, test} from 'vitest';
import {getNormalized} from './normalize.js';

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
        {
          table: 'issues',
          select: [[['issues', 'id'], 'id']],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query: 'SELECT id AS id FROM issues ORDER BY id asc',
    },
    {
      name: 'statement with schema',
      asts: [
        {
          schema: 'zero',
          table: 'clients',
          select: [
            [['clients', 'clientID'], 'clientID'],
            [['clients', 'lastMutationID'], 'lastMutationID'],
          ],
          orderBy: [[['clients', 'clientID']], 'asc'],
        },
      ],
      query:
        'SELECT clients."clientID" AS "clientID", ' +
        'zero.clients."lastMutationID" AS "lastMutationID" ' +
        'FROM zero.clients ORDER BY "clientID" asc',
    },
    {
      name: 'table alias',
      asts: [
        {
          table: 'issues',
          alias: 'Ishooz',
          select: [[['issues', 'id'], 'id']],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query: 'SELECT id AS id FROM issues AS "Ishooz" ORDER BY id asc',
    },
    {
      name: 'column selection',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'id'],
            [['issues', 'name'], 'name'],
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'name'], 'name'],
            [['issues', 'id'], 'id'],
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query: 'SELECT id AS id, name AS name FROM issues ORDER BY id asc',
    },
    {
      name: 'aggregation, aliases ignored',
      asts: [
        {
          table: 'issues',
          aggregate: [{aggregate: 'count', alias: 'num'}],
          orderBy: [[['issues', 'id']], 'asc'],
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
            {aggregate: 'max', field: ['issues', 'priority'], alias: 'maxPri'},
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          aggregate: [
            {aggregate: 'max', field: ['issues', 'priority'], alias: 'maxPri'},
            {aggregate: 'count', alias: 'num'},
          ],
          orderBy: [[['issues', 'id']], 'asc'],
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          groupBy: [
            ['issues', 'id'],
            ['issues', 'name'],
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          groupBy: [
            ['issues', 'name'],
            ['issues', 'id'],
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues GROUP BY id, name ORDER BY id asc',
    },
    {
      name: 'group by, order by',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          groupBy: [
            ['issues', 'id'],
            ['issues', 'name'],
          ],
          orderBy: [
            [
              ['issues', 'id'],
              ['issues', 'name'],
            ],
            'desc',
          ],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues GROUP BY id, name ORDER BY id desc, name desc',
    },
    {
      name: 'group by, order by, limit',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          groupBy: [
            ['issues', 'name'],
            ['issues', 'id'],
          ],
          // ORDER BY expression order must be preserved.
          orderBy: [
            [
              ['issues', 'dueDate'],
              ['issues', 'priority'],
            ],
            'desc',
          ],
          limit: 10,
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
        'GROUP BY issues.id, name ORDER BY "dueDate" desc, priority desc LIMIT 10',
    },
    {
      name: 'group by, order by (ordering preserved), limit',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          groupBy: [
            ['issues', 'name'],
            ['issues', 'id'],
          ],
          // ORDER BY expression order must be preserved.
          orderBy: [
            [
              ['issues', 'priority'],
              ['issues', 'dueDate'],
            ],
            'desc',
          ],
          limit: 10,
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
        'GROUP BY id, name ORDER BY priority desc, "dueDate" desc LIMIT 10',
    },
    {
      name: 'quoted identifiers',
      asts: [
        {
          table: 'camelCaseTable',
          select: [
            [['camelCaseTable', 'userID'], 'u'],
            [['issues', 'name'], 'n'],
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT name AS n, "userID" AS u FROM "camelCaseTable" ORDER BY id asc',
    },
    {
      name: 'quoted selector and alias',
      asts: [
        {
          table: 'camelCaseTable',
          select: [[['camelCaseTable', 'userID'], 'id']],
          orderBy: [[['camelCaseTable', 'userID']], 'asc'],
        },
      ],
      query:
        'SELECT "camelCaseTable"."userID" AS id FROM "camelCaseTable" ORDER BY "userID" asc',
    },
    {
      name: 'join table',
      asts: [
        {
          table: 'issues',
          select: [[['issues', 'id'], 'id']],
          joins: [
            {
              type: 'inner',
              other: {table: 'users'},
              as: 'owner',
              on: [
                ['issues', 'ownerID'],
                ['users', 'id'],
              ],
            },
          ],
          orderBy: [[['owner', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS id FROM issues INNER JOIN users AS owner' +
        ' ON issues."ownerID" = users.id ORDER BY owner.id asc',
    },
    {
      name: 'join subquery',
      asts: [
        {
          table: 'issues',
          select: [[['issues', 'id'], 'id']],
          joins: [
            {
              type: 'inner',
              other: {
                select: [
                  [['issues', 'id'], 'i'],
                  [['issues', 'name'], 'n'],
                ],
                table: 'users',
              },
              as: 'owner',
              on: [
                ['issues', 'ownerID'],
                ['users', 'id'],
              ],
            },
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [[['issues', 'id'], 'id']],
          joins: [
            {
              type: 'inner',
              other: {
                select: [
                  [['issues', 'name'], 'n'],
                  [['issues', 'id'], 'i'],
                ],
                table: 'users',
              },
              as: 'owner',
              on: [
                ['issues', 'ownerID'],
                ['users', 'id'],
              ],
            },
          ],
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS id FROM issues INNER JOIN (SELECT id AS i, name AS n FROM users)' +
        ' AS owner ON issues."ownerID" = users.id ORDER BY id asc',
    },
    {
      name: 'simple condition',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'simple',
            field: ['issues', 'id'],
            op: '=',
            value: {type: 'value', value: 1234},
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues WHERE id = $1 ORDER BY id asc',
      values: [1234],
    },
    {
      name: 'condition with selector',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'simple',
            field: ['issues', 'id'],
            op: '=',
            value: {type: 'value', value: 1234},
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues WHERE issues.id = $1 ORDER BY id asc',
      values: [1234],
    },
    {
      name: 'condition with array',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'simple',
            field: ['issues', 'id'],
            op: 'IN',
            value: {type: 'value', value: ['1234', '2345', '4567']},
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues WHERE issues.id IN ($1, $2, $3) ORDER BY id asc',
      values: ['1234', '2345', '4567'],
    },
    {
      name: 'simple condition (value types affect hash)',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'simple',
            field: ['issues', 'id'],
            op: '=',
            value: {type: 'value', value: '1234'},
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues WHERE id = $1 ORDER BY id asc',
      values: ['1234'],
    },
    {
      name: 'multiple conditions',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'id'],
                op: '=',
                value: {type: 'value', value: 1234},
              },
              {
                type: 'simple',
                field: ['issues', 'name'],
                op: '=',
                value: {type: 'value', value: 'foobar'},
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 5},
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '>',
                value: {type: 'value', value: 2},
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '>',
                value: {type: 'value', value: 2},
              },
              {
                type: 'simple',
                field: ['issues', 'id'],
                op: '=',
                value: {type: 'value', value: 1234},
              },
              {
                type: 'simple',
                field: ['issues', 'name'],
                op: '=',
                value: {type: 'value', value: 'foobar'},
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 5},
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'priority'], 'p'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'priority'], 'p'],
          ],
          where: {
            type: 'conjunction',
            op: 'OR',
            conditions: [],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query: 'SELECT id AS i, priority AS p FROM issues ORDER BY id asc',
    },
    {
      name: 'multiple conditions with same fields and operator',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 5},
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 3},
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 3},
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '<',
                value: {type: 'value', value: 5},
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'id'],
                op: '=',
                value: {type: 'value', value: 1234},
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
              {
                type: 'simple',
                field: ['issues', 'id'],
                op: '=',
                value: {type: 'value', value: 1234},
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                ],
              },
              {
                type: 'conjunction',
                op: 'OR',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'id'],
                    op: '=',
                    value: {type: 'value', value: 1234},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'priority'],
                    op: '>',
                    value: {type: 'value', value: 2},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: ['issues', 'a'],
                        op: '=',
                        value: {type: 'value', value: 'bc'},
                      },
                      {
                        type: 'conjunction',
                        op: 'OR',
                        conditions: [
                          {
                            type: 'simple',
                            field: ['issues', 'doo'],
                            op: '>',
                            value: {type: 'value', value: '23'},
                          },
                          {
                            type: 'simple',
                            field: ['issues', 'dah'],
                            op: '<',
                            value: {type: 'value', value: '56'},
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
              {
                type: 'simple',
                field: ['issues', 'id'],
                op: '=',
                value: {type: 'value', value: 1234},
              },
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'foo'],
                    op: '=',
                    value: {type: 'value', value: 'bar'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'bar'],
                    op: '>',
                    value: {type: 'value', value: 23},
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
                            field: ['issues', 'zzz'],
                            op: '!=',
                            value: {type: 'value', value: 48},
                          },
                          {
                            type: 'simple',
                            field: ['issues', 'xyz'],
                            op: '!=',
                            value: {type: 'value', value: 488},
                          },
                        ],
                      },
                      {
                        type: 'simple',
                        field: ['issues', 'ac'],
                        op: '>',
                        value: {type: 'value', value: 'dc'},
                      },
                    ],
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
        {
          // AST with different but equivalent nesting of AND's and OR's
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
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
                    field: ['issues', 'name'],
                    op: '=',
                    value: {type: 'value', value: 'foobar'},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: ['issues', 'dah'],
                        op: '<',
                        value: {type: 'value', value: '56'},
                      },
                      {
                        type: 'conjunction',
                        op: 'OR',
                        conditions: [
                          {
                            type: 'simple',
                            field: ['issues', 'doo'],
                            op: '>',
                            value: {type: 'value', value: '23'},
                          },
                          {
                            type: 'simple',
                            field: ['issues', 'a'],
                            op: '=',
                            value: {type: 'value', value: 'bc'},
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
                            field: ['issues', 'id'],
                            op: '=',
                            value: {type: 'value', value: 1234},
                          },
                        ],
                      },
                    ],
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'bar'],
                    op: '>',
                    value: {type: 'value', value: 23},
                  },
                ],
              },
              {
                type: 'simple',
                field: ['issues', 'priority'],
                op: '>',
                value: {type: 'value', value: 2},
              },
              {
                type: 'conjunction',
                op: 'AND',
                conditions: [
                  {
                    type: 'simple',
                    field: ['issues', 'foo'],
                    op: '=',
                    value: {type: 'value', value: 'bar'},
                  },
                  {
                    type: 'conjunction',
                    op: 'OR',
                    conditions: [
                      {
                        type: 'simple',
                        field: ['issues', 'ac'],
                        op: '>',
                        value: {type: 'value', value: 'dc'},
                      },
                      {
                        type: 'conjunction',
                        op: 'AND',
                        conditions: [
                          {
                            type: 'simple',
                            field: ['issues', 'zzz'],
                            op: '!=',
                            value: {type: 'value', value: 48},
                          },
                          {
                            type: 'simple',
                            field: ['issues', 'xyz'],
                            op: '!=',
                            value: {type: 'value', value: 488},
                          },
                        ],
                      },
                    ],
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id']], 'asc'],
        },
      ],
      query:
        'SELECT id AS i, name AS n FROM issues ' +
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
