import type {JSONValue} from 'postgres';
import {describe, expect, test} from 'vitest';
import {getNormalized} from './normalize.js';
import type {ServerAST} from './server-ast.js';

describe('zql/normalize-query-hash', () => {
  type Case = {
    name: string;
    asts: ServerAST[];
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query: 'SELECT issues.id AS id FROM issues ORDER BY issues.id asc',
    },
    {
      name: 'subquery',
      asts: [
        {
          table: 'issues',
          subQuery: {
            ast: {
              table: 'issues',
              select: [
                [['issues', 'id'], 'id'],
                [['issues', 'created'], 'created'],
              ],
              limit: 10,
            },
            alias: 'issues_alias',
          },
          select: [[['issues_alias', 'id'], 'id']],
          orderBy: [[['issues_alias', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues_alias.id AS id FROM ' +
        // Note: subquery is also normalized (e.g. fields are sorted)
        '(SELECT issues.created AS created, issues.id AS id FROM issues LIMIT 10) AS issues_alias ' +
        'ORDER BY issues_alias.id asc',
    },
    {
      name: 'statement with schema',
      asts: [
        {
          schema: 'zero',
          table: 'clients',
          select: [
            [['clients', 'col.with.dots'], 'clientID'],
            [['zero.clients', 'lastMutationID'], 'lastMutationID'],
          ],
          orderBy: [[['clients', 'col.with.dots'], 'asc']],
        },
      ],
      query:
        'SELECT clients."col.with.dots" AS "clientID", ' +
        'zero.clients."lastMutationID" AS "lastMutationID" ' +
        'FROM zero.clients ORDER BY clients."col.with.dots" asc',
    },
    {
      name: 'table alias',
      asts: [
        {
          table: 'issues',
          alias: 'Ishooz',
          select: [[['issues', 'id'], 'id']],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS id FROM issues AS "Ishooz" ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'name'], 'name'],
            [['issues', 'id'], 'id'],
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS id, issues.name AS name FROM issues ORDER BY issues.id asc',
    },
    {
      name: 'aggregation, aliases ignored',
      asts: [
        {
          table: 'issues',
          aggregate: [{aggregate: 'count', alias: 'num'}],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query: 'SELECT count(*) AS "count(*)" FROM issues ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          aggregate: [
            {aggregate: 'max', field: ['issues', 'priority'], alias: 'maxPri'},
            {aggregate: 'count', alias: 'num'},
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT count(*) AS "count(*)", max(issues.priority) AS "max(issues.priority)" ' +
        'FROM issues ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues GROUP BY issues.id, issues.name ORDER BY issues.id asc',
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
            [['issues', 'id'], 'desc'],
            [['issues', 'name'], 'desc'],
          ],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues GROUP BY issues.id, issues.name ORDER BY issues.id desc, issues.name desc',
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
            [['issues', 'dueDate'], 'desc'],
            [['issues', 'priority'], 'desc'],
          ],
          limit: 10,
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'GROUP BY issues.id, issues.name ORDER BY issues."dueDate" desc, issues.priority desc LIMIT 10',
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
            [['issues', 'priority'], 'desc'],
            [['issues', 'dueDate'], 'desc'],
          ],
          limit: 10,
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'GROUP BY issues.id, issues.name ORDER BY issues.priority desc, issues."dueDate" desc LIMIT 10',
    },
    {
      name: 'quoted identifiers',
      asts: [
        {
          table: 'camelCaseTable',
          select: [
            [['camelCaseTable', 'userID'], 'u'],
            [['camelCaseTable', 'name'], 'n'],
          ],
          orderBy: [[['camelCaseTable', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT "camelCaseTable".name AS n, "camelCaseTable"."userID" AS u FROM "camelCaseTable" ORDER BY "camelCaseTable".id asc',
    },
    {
      name: 'quoted selector and alias',
      asts: [
        {
          table: 'camelCaseTable',
          select: [[['camelCaseTable', 'userID'], 'id']],
          orderBy: [[['camelCaseTable', 'userID'], 'asc']],
        },
      ],
      query:
        'SELECT "camelCaseTable"."userID" AS id FROM "camelCaseTable" ORDER BY "camelCaseTable"."userID" asc',
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
          orderBy: [[['owner', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS id FROM issues INNER JOIN users AS owner' +
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
                  [['users', 'id'], 'i'],
                  [['users', 'name'], 'n'],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          select: [[['issues', 'id'], 'id']],
          joins: [
            {
              type: 'inner',
              other: {
                select: [
                  [['users', 'name'], 'n'],
                  [['users', 'id'], 'i'],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS id FROM issues INNER JOIN (SELECT users.id AS i, users.name AS n FROM users)' +
        ' AS owner ON issues."ownerID" = users.id ORDER BY issues.id asc',
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
            value: {type: 'value', value: 12345},
          },
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues WHERE issues.id = $1 ORDER BY issues.id asc',
      values: [12345],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues WHERE issues.id = $1 ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues WHERE issues.id IN ($1, $2, $3) ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues WHERE issues.id = $1 ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE (issues.id = $1 AND issues.name = $2 AND issues.priority < $3 AND issues.priority > $4) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.priority AS p FROM issues ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE (issues.priority < $1 AND issues.priority < $2) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE (issues.id = $1 AND (issues.name = $2 OR issues.priority > $3)) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE ((issues.name = $1 OR issues.priority > $2) AND (issues.name = $3 OR issues.priority > $4)) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE ((issues.id = $1 OR issues.name = $2) AND (issues.id = $3 OR issues.priority > $4)) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE ((issues.id = $1 OR issues.name = $2) AND (issues.id = $3 OR issues.name = $4 OR issues.priority > $5)) ' +
        'ORDER BY issues.id asc',
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
          orderBy: [[['issues', 'id'], 'asc']],
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
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      query:
        'SELECT issues.id AS i, issues.name AS n FROM issues ' +
        'WHERE (issues.bar > $1 AND issues.foo = $2 AND issues.id = $3 AND issues.name = $4 AND issues.priority > $5 AND ' +
        '(issues.a = $6 OR issues.dah < $7 OR issues.doo > $8) AND (issues.ac > $9 OR (issues.xyz != $10 AND issues.zzz != $11))) ' +
        'ORDER BY issues.id asc',
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
