import {describe, expect, test} from 'vitest';
import type {AST} from './ast.js';
import {normalizeAST} from './ast.js';

describe('zql/ast', () => {
  type Case = {
    name: string;
    asts: AST[];
    normalized: AST;
  };

  function conditionCases(conditionType: 'having' | 'where'): Case[] {
    return [
      {
        name: conditionType + ': simple condition',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
              type: 'simple',
              field: ['issues', 'id'],
              op: '=',
              value: {type: 'value', value: 1234},
            },
            orderBy: [[['issues', 'id'], 'asc']],
          },
        ],
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
            type: 'simple',
            field: ['issues', 'id'],
            op: '=',
            value: {type: 'value', value: 1234},
          },
          orderBy: [[['issues', 'id'], 'asc']],
        },
      },
      {
        name: conditionType + ': multiple conditions',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': empty conjunctions removed',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
              type: 'conjunction',
              op: 'AND',
              conditions: [],
            },
            orderBy: [
              [['issues', 'id'], 'asc'],
              [['issues', 'name'], 'asc'],
            ],
          },
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
              type: 'conjunction',
              op: 'OR',
              conditions: [],
            },
            orderBy: [
              [['issues', 'id'], 'asc'],
              [['issues', 'name'], 'asc'],
            ],
          },
        ],
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          orderBy: [
            [['issues', 'id'], 'asc'],
            [['issues', 'name'], 'asc'],
          ],
        },
      },
      {
        name:
          conditionType + ': multiple conditions with same fields and operator',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': nested conditions',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': equivalent nested conjunctions',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': conjunction comparison',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': conjunction fallback sorting to length',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
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
      },
      {
        name: conditionType + ': condition flattening',
        asts: [
          {
            table: 'issues',
            select: [
              [['issues', 'id'], 'i'],
              [['issues', 'name'], 'n'],
            ],
            [conditionType]: {
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
            [conditionType]: {
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
        normalized: {
          // Flattened conditions.
          table: 'issues',
          select: [
            [['issues', 'id'], 'i'],
            [['issues', 'name'], 'n'],
          ],
          [conditionType]: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'bar'],
                op: '>',
                value: {type: 'value', value: 23},
              },
              {
                type: 'simple',
                field: ['issues', 'foo'],
                op: '=',
                value: {type: 'value', value: 'bar'},
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
                    type: 'simple',
                    field: ['issues', 'dah'],
                    op: '<',
                    value: {type: 'value', value: '56'},
                  },
                  {
                    type: 'simple',
                    field: ['issues', 'doo'],
                    op: '>',
                    value: {type: 'value', value: '23'},
                  },
                ],
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
                        field: ['issues', 'xyz'],
                        op: '!=',
                        value: {type: 'value', value: 488},
                      },
                      {
                        type: 'simple',
                        field: ['issues', 'zzz'],
                        op: '!=',
                        value: {type: 'value', value: 48},
                      },
                    ],
                  },
                ],
              },
            ],
          },
          orderBy: [[['issues', 'id'], 'asc']],
        },
      },
    ];
  }

  const cases: Case[] = [
    {
      name: 'simplest statement',
      asts: [
        {
          table: 'issues',
          select: [[['issues', 'id'], 'alias']],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        table: 'issues',
        select: [[['issues', 'id'], 'alias']],
        orderBy: [[['issues', 'id'], 'asc']],
      },
    },
    {
      name: 'statement with schema',
      asts: [
        {
          schema: 'zero',
          table: 'clients',
          select: [[['issues', 'id'], 'alias']],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        schema: 'zero',
        table: 'clients',
        select: [[['issues', 'id'], 'alias']],
        orderBy: [[['issues', 'id'], 'asc']],
      },
    },
    {
      name: 'column selection',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'id_alias'],
            [['issues', 'name'], 'a_name'],
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'name'], 'a_name'],
            [['issues', 'id'], 'id_alias'],
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        table: 'issues',
        select: [
          [['issues', 'id'], 'id_alias'],
          [['issues', 'name'], 'a_name'],
        ],
        orderBy: [[['issues', 'id'], 'asc']],
      },
    },
    {
      name: 'single aggregation',
      asts: [
        {
          table: 'issues',
          aggregate: [{aggregate: 'count', alias: 'num'}],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        table: 'issues',
        aggregate: [{aggregate: 'count', alias: 'num'}],
        orderBy: [[['issues', 'id'], 'asc']],
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
              field: ['issues', 'priority'],
              alias: 'maxPri',
            },
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          aggregate: [
            {
              aggregate: 'max',
              field: ['issues', 'priority'],
              alias: 'maxPri',
            },
            {aggregate: 'count', alias: 'num'},
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        table: 'issues',
        aggregate: [
          {aggregate: 'count', alias: 'num'},
          {
            aggregate: 'max',
            field: ['issues', 'priority'],
            alias: 'maxPri',
          },
        ],
        orderBy: [[['issues', 'id'], 'asc']],
      },
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
      normalized: {
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
      normalized: {
        table: 'issues',
        select: [
          [['issues', 'id'], 'i'],
          [['issues', 'name'], 'n'],
        ],
        groupBy: [
          ['issues', 'id'],
          ['issues', 'name'],
        ],
        // ORDER BY expression order must be preserved.
        orderBy: [
          [['issues', 'dueDate'], 'desc'],
          [['issues', 'priority'], 'desc'],
        ],
        limit: 10,
      },
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
      normalized: {
        table: 'issues',
        select: [
          [['issues', 'id'], 'i'],
          [['issues', 'name'], 'n'],
        ],
        groupBy: [
          ['issues', 'id'],
          ['issues', 'name'],
        ],
        // ORDER BY expression order must be preserved.
        orderBy: [
          [['issues', 'priority'], 'desc'],
          [['issues', 'dueDate'], 'desc'],
        ],
        limit: 10,
      },
    },
    {
      name: 'joins',
      asts: [
        {
          table: 'issues',
          select: [
            [['issues', 'id'], 'id_alias'],
            [['issues', 'name'], 'a_name'],
          ],
          joins: [
            {
              type: 'inner',
              other: {
                table: 'users',
                select: [
                  [['issues', 'id'], 'id_alias'],
                  [['issues', 'name'], 'b_alias'],
                ],
                orderBy: [[['issues', 'id'], 'asc']],
              },
              as: 'owner',
              on: [
                ['issues', 'owner_id'],
                ['users', 'id'],
              ],
            },
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
        {
          table: 'issues',
          select: [
            [['issues', 'name'], 'a_name'],
            [['issues', 'id'], 'id_alias'],
          ],
          joins: [
            {
              type: 'inner',
              other: {
                table: 'users',
                select: [
                  [['issues', 'name'], 'b_alias'],
                  [['issues', 'id'], 'id_alias'],
                ],
                orderBy: [[['issues', 'id'], 'asc']],
              },
              as: 'owner',
              on: [
                ['issues', 'owner_id'],
                ['users', 'id'],
              ],
            },
          ],
          orderBy: [[['issues', 'id'], 'asc']],
        },
      ],
      normalized: {
        table: 'issues',
        select: [
          [['issues', 'id'], 'id_alias'],
          [['issues', 'name'], 'a_name'],
        ],
        joins: [
          {
            type: 'inner',
            other: {
              table: 'users',
              select: [
                [['issues', 'id'], 'id_alias'],
                [['issues', 'name'], 'b_alias'],
              ],
              orderBy: [[['issues', 'id'], 'asc']],
            },
            as: 'owner',
            on: [
              ['issues', 'owner_id'],
              ['users', 'id'],
            ],
          },
        ],
        orderBy: [[['issues', 'id'], 'asc']],
      },
    },
    ...conditionCases('where'),
    ...conditionCases('having'),
    {
      name: 'having: condition on aggregate',
      asts: [
        {
          table: 'issue',
          joins: [
            {
              type: 'left',
              other: {
                table: 'issueLabel',
                orderBy: [[['issueLabel', 'id'], 'asc']],
              },
              as: 'issueLabel',
              on: [
                ['issue', 'id'],
                ['issueLabel', 'issueID'],
              ],
            },
            {
              type: 'left',
              other: {
                table: 'label',
                orderBy: [[['label', 'id'], 'asc']],
              },
              as: 'label',
              on: [
                ['issueLabel', 'labelID'],
                ['label', 'id'],
              ],
            },
          ],
          groupBy: [['issue', 'id']],
          select: [
            [['issues', 'id'], 'id_alias'],
            [['issues', 'name'], 'a_name'],
          ],
          aggregate: [
            {
              field: ['label', 'name'],
              alias: 'labels',
              aggregate: 'array',
            },
          ],
          having: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                field: ['issues', 'bar'],
                op: '>',
                value: {type: 'value', value: 23},
              },
              {
                type: 'simple',
                op: 'INTERSECTS',
                field: [null, 'labels'],
                value: {
                  type: 'value',
                  value: ['crash'],
                },
              },
            ],
          },
        },
        {
          table: 'issue',
          joins: [
            {
              type: 'left',
              other: {
                table: 'issueLabel',
                orderBy: [[['issueLabel', 'id'], 'asc']],
              },
              as: 'issueLabel',
              on: [
                ['issue', 'id'],
                ['issueLabel', 'issueID'],
              ],
            },
            {
              type: 'left',
              other: {
                table: 'label',
                orderBy: [[['label', 'id'], 'asc']],
              },
              as: 'label',
              on: [
                ['issueLabel', 'labelID'],
                ['label', 'id'],
              ],
            },
          ],
          groupBy: [['issue', 'id']],
          select: [
            [['issues', 'id'], 'id_alias'],
            [['issues', 'name'], 'a_name'],
          ],
          aggregate: [
            {
              field: ['label', 'name'],
              alias: 'labels',
              aggregate: 'array',
            },
          ],
          having: {
            type: 'conjunction',
            op: 'AND',
            conditions: [
              {
                type: 'simple',
                op: 'INTERSECTS',
                field: [null, 'labels'],
                value: {
                  type: 'value',
                  value: ['crash'],
                },
              },
              {
                type: 'simple',
                field: ['issues', 'bar'],
                op: '>',
                value: {type: 'value', value: 23},
              },
            ],
          },
        },
      ],
      normalized: {
        table: 'issue',
        select: [
          [['issues', 'id'], 'id_alias'],
          [['issues', 'name'], 'a_name'],
        ],
        aggregate: [
          {
            field: ['label', 'name'],
            alias: 'labels',
            aggregate: 'array',
          },
        ],
        joins: [
          {
            type: 'left',
            other: {
              table: 'issueLabel',
              orderBy: [[['issueLabel', 'id'], 'asc']],
            },
            as: 'issueLabel',
            on: [
              ['issue', 'id'],
              ['issueLabel', 'issueID'],
            ],
          },
          {
            type: 'left',
            other: {
              table: 'label',
              orderBy: [[['label', 'id'], 'asc']],
            },
            as: 'label',
            on: [
              ['issueLabel', 'labelID'],
              ['label', 'id'],
            ],
          },
        ],
        groupBy: [['issue', 'id']],
        having: {
          type: 'conjunction',
          op: 'AND',
          conditions: [
            {
              type: 'simple',
              op: 'INTERSECTS',
              field: [null, 'labels'],
              value: {
                type: 'value',
                value: ['crash'],
              },
            },
            {
              type: 'simple',
              field: ['issues', 'bar'],
              op: '>',
              value: {type: 'value', value: 23},
            },
          ],
        },
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
