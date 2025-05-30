import {describe, test, expect} from 'vitest';
import {
  pullSimpleAndComponents,
  primaryKeyConstraintFromFilters,
  type SimpleOperator,
} from './constraint.ts';

describe('constraint', () => {
  describe('pullSimpleAndComponents', () => {
    test.each([
      {
        name: 'pulls simple conditions from AND',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'name'},
              op: '=',
              right: {type: 'literal', value: 'test'},
            },
          ],
        } as const,
        expected: [
          {
            type: 'simple',
            left: {type: 'column', name: 'id'},
            op: '=',
            right: {type: 'literal', value: 1},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'name'},
            op: '=',
            right: {type: 'literal', value: 'test'},
          },
        ],
      },
      {
        name: 'pulls from nested AND',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'name'},
                  op: '=',
                  right: {type: 'literal', value: 'test'},
                },
                {
                  type: 'simple',
                  left: {type: 'column', name: 'age'},
                  op: '=',
                  right: {type: 'literal', value: 30},
                },
              ],
            },
          ],
        } as const,
        expected: [
          {
            type: 'simple',
            left: {type: 'column', name: 'id'},
            op: '=',
            right: {type: 'literal', value: 1},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'name'},
            op: '=',
            right: {type: 'literal', value: 'test'},
          },
          {
            type: 'simple',
            left: {type: 'column', name: 'age'},
            op: '=',
            right: {type: 'literal', value: 30},
          },
        ],
      },
      {
        name: 'returns empty for OR at top level',
        condition: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 2},
            },
          ],
        } as const,
        expected: [],
      },
      {
        name: 'pulls from single condition OR',
        condition: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
          ],
        } as const,
        expected: [
          {
            type: 'simple',
            left: {type: 'column', name: 'id'},
            op: '=',
            right: {type: 'literal', value: 1},
          },
        ],
      },
    ])('$name', ({condition, expected}) => {
      const result = pullSimpleAndComponents(condition);
      expect(result).toEqual(expected);
    });
  });

  describe('primaryKeyConstraintFromFilters', () => {
    test.each([
      {
        name: 'returns undefined for no condition',
        condition: undefined,
        primary: ['id'] as const,
        expected: undefined,
      },
      {
        name: 'returns undefined for OR condition',
        condition: {
          type: 'or',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 2},
            },
          ],
        } as const,
        primary: ['id'] as const,
        expected: undefined,
      },
      {
        name: 'returns constraint for simple primary key lookup',
        condition: {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 1},
        } as const,
        primary: ['id'] as const,
        expected: {id: 1},
      },
      {
        name: 'returns constraint for composite primary key lookup',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'tenant'},
              op: '=',
              right: {type: 'literal', value: 'test'},
            },
          ],
        } as const,
        primary: ['id', 'tenant'] as const,
        expected: {id: 1, tenant: 'test'},
      },
      {
        name: 'returns undefined for partial primary key',
        condition: {
          type: 'simple',
          left: {type: 'column', name: 'id'},
          op: '=',
          right: {type: 'literal', value: 1},
        } as const,
        primary: ['id', 'tenant'] as const,
        expected: undefined,
      },
      {
        name: 'returns undefined for non-primary key columns',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'simple',
              left: {type: 'column', name: 'name'},
              op: '=',
              right: {type: 'literal', value: 'test'},
            },
          ],
        } as const,
        primary: ['id'] as const,
        expected: {id: 1},
      },
      {
        name: 'handles nested AND conditions',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'tenant'},
                  op: '=',
                  right: {type: 'literal', value: 'test'},
                },
              ],
            },
          ],
        } as const,
        primary: ['id', 'tenant'] as const,
        expected: {id: 1, tenant: 'test'},
      },
      {
        name: 'handles nested AND with OR conditions',
        condition: {
          type: 'and',
          conditions: [
            {
              type: 'simple',
              left: {type: 'column', name: 'id'},
              op: '=',
              right: {type: 'literal', value: 1},
            },
            {
              type: 'and',
              conditions: [
                {
                  type: 'simple',
                  left: {type: 'column', name: 'tenant'},
                  op: '=',
                  right: {type: 'literal', value: 'test'},
                },
                {
                  type: 'or',
                  conditions: [
                    {
                      type: 'simple',
                      left: {type: 'column', name: 'status'},
                      op: '=',
                      right: {type: 'literal', value: 'active'},
                    },
                    {
                      type: 'simple',
                      left: {type: 'column', name: 'status'},
                      op: '=',
                      right: {type: 'literal', value: 'pending'},
                    },
                  ],
                },
              ],
            },
          ],
        } as const,
        primary: ['id', 'tenant'] as const,
        expected: {id: 1, tenant: 'test'},
      },
    ])('$name', ({condition, primary, expected}) => {
      const result = primaryKeyConstraintFromFilters(condition, primary);
      expect(result).toEqual(expected);
    });
  });
});

test('returns undefined for operators other than equality', () => {
  const operators: SimpleOperator[] = [
    '>',
    '<',
    '>=',
    '<=',
    '!=',
    'LIKE',
    'NOT LIKE',
    'ILIKE',
    'NOT ILIKE',
    'IN',
    'NOT IN',
    'IS',
    'IS NOT',
  ];

  for (const op of operators) {
    const condition = {
      type: 'simple',
      left: {type: 'column', name: 'id'},
      op,
      right: {
        type: 'literal',
        value: op === 'IN' || op === 'NOT IN' ? [1, 2, 3] : 1,
      },
    } as const;

    const primary = ['id'] as const;
    const result = primaryKeyConstraintFromFilters(condition, primary);

    expect(result).toBeUndefined();
  }
});
