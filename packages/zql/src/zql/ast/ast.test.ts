import {describe, expect, test} from 'vitest';
import type {AST} from './ast.js';
import {normalizeAST} from './ast.js';

describe('zql/ast', () => {
  type Case = {
    name: string;
    asts: AST[];
    normalized: AST;
  };

  const cases: Case[] = [
    {
      name: 'simplest statement',
      asts: [
        {table: 'issues', select: [['id', 'alias']], orderBy: [['id'], 'asc']},
      ],
      normalized: {
        table: 'issues',
        select: [['id', 'alias']],
        orderBy: [['id'], 'asc'],
      },
    },
    {
      name: 'column selection',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'id_alias'],
            ['name', 'a_name'],
          ],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['name', 'a_name'],
            ['id', 'id_alias'],
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      normalized: {
        table: 'issues',
        select: [
          ['id', 'id_alias'],
          ['name', 'a_name'],
        ],
        orderBy: [['id'], 'asc'],
      },
    },
    {
      name: 'single aggregation',
      asts: [
        {
          table: 'issues',
          aggregate: [{aggregate: 'count', alias: 'num'}],
          orderBy: [['id'], 'asc'],
        },
      ],
      normalized: {
        table: 'issues',
        aggregate: [{aggregate: 'count', alias: 'num'}],
        orderBy: [['id'], 'asc'],
      },
    },
    {
      name: 'multiple aggregates',
      asts: [
        {
          table: 'issues',
          aggregate: [
            {aggregate: 'count', alias: 'num'},
            {
              aggregate: 'max',
              field: 'priority',
              alias: 'maxPri',
            },
          ],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          aggregate: [
            {
              aggregate: 'max',
              field: 'priority',
              alias: 'maxPri',
            },
            {aggregate: 'count', alias: 'num'},
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      normalized: {
        table: 'issues',
        aggregate: [
          {aggregate: 'count', alias: 'num'},
          {
            aggregate: 'max',
            field: 'priority',
            alias: 'maxPri',
          },
        ],
        orderBy: [['id'], 'asc'],
      },
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
      normalized: {
        table: 'issues',
        select: [
          ['id', 'i'],
          ['name', 'n'],
        ],
        groupBy: ['id', 'name'],
        orderBy: [['id'], 'asc'],
      },
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
      normalized: {
        table: 'issues',
        select: [
          ['id', 'i'],
          ['name', 'n'],
        ],
        groupBy: ['id', 'name'],
        // ORDER BY expression order must be preserved.
        orderBy: [['dueDate', 'priority'], 'desc'],
        limit: 10,
      },
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
      normalized: {
        table: 'issues',
        select: [
          ['id', 'i'],
          ['name', 'n'],
        ],
        groupBy: ['id', 'name'],
        // ORDER BY expression order must be preserved.
        orderBy: [['priority', 'dueDate'], 'desc'],
        limit: 10,
      },
    },
    {
      name: 'joins',
      asts: [
        {
          table: 'issues',
          select: [
            ['id', 'id_alias'],
            ['name', 'a_name'],
          ],
          joins: [
            {
              type: 'inner',
              other: {
                table: 'users',
                select: [
                  ['id', 'id_alias'],
                  ['name', 'b_alias'],
                ],
                orderBy: [['id'], 'asc'],
              },
              as: 'owner',
              on: ['issues.owner_id', 'users.id'],
            },
          ],
          orderBy: [['id'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['name', 'a_name'],
            ['id', 'id_alias'],
          ],
          joins: [
            {
              type: 'inner',
              other: {
                table: 'users',
                select: [
                  ['name', 'b_alias'],
                  ['id', 'id_alias'],
                ],
                orderBy: [['id'], 'asc'],
              },
              as: 'owner',
              on: ['issues.owner_id', 'users.id'],
            },
          ],
          orderBy: [['id'], 'asc'],
        },
      ],
      normalized: {
        table: 'issues',
        select: [
          ['id', 'id_alias'],
          ['name', 'a_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
              select: [
                ['id', 'id_alias'],
                ['name', 'b_alias'],
              ],
              orderBy: [['id'], 'asc'],
            },
            as: 'owner',
            on: ['issues.owner_id', 'users.id'],
          },
        ],
        orderBy: [['id'], 'asc'],
      },
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
      normalized: {
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
      normalized: {
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
    },
    {
      name: 'empty conjunctions removed',
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
            conditions: [],
          },
          orderBy: [['id', 'name'], 'asc'],
        },
        {
          table: 'issues',
          select: [
            ['id', 'i'],
            ['name', 'n'],
          ],
          where: {
            type: 'conjunction',
            op: 'OR',
            conditions: [],
          },
          orderBy: [['id', 'name'], 'asc'],
        },
      ],
      normalized: {
        table: 'issues',
        select: [
          ['id', 'i'],
          ['name', 'n'],
        ],
        orderBy: [['id', 'name'], 'asc'],
      },
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
      normalized: {
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
      normalized: {
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
    },
    {
      name: 'equivalent nested conjunctions',
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
      normalized: {
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
      normalized: {
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
      normalized: {
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
      normalized: {
        // Flattened conditions.
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
              field: 'bar',
              op: '>',
              value: {type: 'literal', value: 23},
            },
            {
              type: 'simple',
              field: 'foo',
              op: '=',
              value: {type: 'literal', value: 'bar'},
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
                  type: 'simple',
                  field: 'dah',
                  op: '<',
                  value: {type: 'literal', value: '56'},
                },
                {
                  type: 'simple',
                  field: 'doo',
                  op: '>',
                  value: {type: 'literal', value: '23'},
                },
              ],
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
                      field: 'xyz',
                      op: '!=',
                      value: {type: 'literal', value: 488},
                    },
                    {
                      type: 'simple',
                      field: 'zzz',
                      op: '!=',
                      value: {type: 'literal', value: 48},
                    },
                  ],
                },
              ],
            },
          ],
        },
        orderBy: [['id'], 'asc'],
      },
    },
  ];

  for (const c of cases) {
    test(c.name, () => {
      c.asts.forEach(ast => {
        const normalized = normalizeAST(ast);
        expect(JSON.stringify(normalized, null, 2)).toBe(
          JSON.stringify(c.normalized, null, 2),
        );
      });
    });
  }
});
