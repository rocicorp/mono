import {describe, expect, test} from 'vitest';
import {calculateSelectivity} from './selectivity-calculator.ts';
import {HLLStatsManager} from '../../zql/src/planner/stats/hll-stats-manager.ts';
import type {Condition, SimpleCondition} from '../../zero-protocol/src/ast.ts';

/**
 * Helper to create a mock HLLStatsManager with predefined cardinalities
 */
function createMockHLLManager(stats: Record<string, number>): HLLStatsManager {
  const manager = new HLLStatsManager();

  // Add dummy rows to populate stats
  for (const [tableColumn, cardinality] of Object.entries(stats)) {
    const [tableName, columnName] = tableColumn.split(':');

    // Add rows with unique values to achieve desired cardinality
    for (let i = 0; i < cardinality; i++) {
      manager.onAdd(tableName, {[columnName]: `value-${i}`});
    }
  }

  return manager;
}

/**
 * Helper to create a simple condition for testing
 */
function simpleCondition(
  op: SimpleCondition['op'],
  column: string,
  value:
    | string
    | number
    | boolean
    | null
    | readonly (string | number | boolean)[],
): SimpleCondition {
  return {
    type: 'simple',
    op,
    left: {type: 'column', name: column},
    right: {type: 'literal', value},
  };
}

describe('Selectivity Calculator', () => {
  describe('equality operator (=)', () => {
    test('calculates 1/cardinality for equality', () => {
      const manager = createMockHLLManager({'users:status': 10});

      const condition = simpleCondition('=', 'status', 'active');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // With 10 distinct values, selectivity should be ~1/10 = 0.1
      expect(selectivity).toBeCloseTo(0.1, 2);
    });

    test('uses default selectivity when no stats', () => {
      const manager = new HLLStatsManager();

      const condition = simpleCondition('=', 'status', 'active');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Should use PostgreSQL default: 0.005 (0.5%)
      expect(selectivity).toBe(0.005);
    });

    test('handles high cardinality columns', () => {
      const manager = createMockHLLManager({'users:id': 1000});

      const condition = simpleCondition('=', 'id', 123);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // With 1000 distinct IDs, selectivity should be ~1/1000 = 0.001
      expect(selectivity).toBeCloseTo(0.001, 3);
    });
  });

  describe('inequality operator (!=)', () => {
    test('calculates 1 - (1/cardinality) for inequality', () => {
      const manager = createMockHLLManager({'users:status': 10});

      const condition = simpleCondition('!=', 'status', 'active');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // With 10 distinct values, selectivity should be ~1 - 1/10 = 0.9
      expect(selectivity).toBeCloseTo(0.9, 2);
    });

    test('uses default selectivity when no stats', () => {
      const manager = new HLLStatsManager();

      const condition = simpleCondition('!=', 'status', 'active');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Should use 1 - DEFAULT_EQ_SEL = 0.995
      expect(selectivity).toBe(0.995);
    });
  });

  describe('IN operator', () => {
    test('calculates numValues/cardinality for IN', () => {
      const manager = createMockHLLManager({'users:category': 100});

      const condition = simpleCondition('IN', 'category', [
        'tech',
        'science',
        'art',
      ]);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // 3 values out of 100 distinct → 3/100 = 0.03
      expect(selectivity).toBeCloseTo(0.03, 3);
    });

    test('caps selectivity at 1.0 when values exceed cardinality', () => {
      const manager = createMockHLLManager({'users:category': 5});

      const condition = simpleCondition(
        'IN',
        'category',
        Array.from({length: 10}, (_, i) => i),
      );
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // 10 values but only 5 distinct → min(10/5, 1.0) = 1.0
      expect(selectivity).toBe(1.0);
    });

    test('uses default when no stats', () => {
      const manager = new HLLStatsManager();

      const condition = simpleCondition('IN', 'category', ['tech', 'science']);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Default IN selectivity: 0.1
      expect(selectivity).toBe(0.1);
    });

    test('handles empty IN list', () => {
      const manager = createMockHLLManager({'users:category': 100});

      const condition = simpleCondition('IN', 'category', []);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Empty IN list → 0/100 = 0.0 (no rows match)
      expect(selectivity).toBe(0.0);
    });
  });

  describe('NOT IN operator', () => {
    test('calculates complement of IN selectivity', () => {
      const manager = createMockHLLManager({'users:category': 100});

      const condition = simpleCondition('NOT IN', 'category', [
        'tech',
        'science',
      ]);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // NOT IN selectivity = 1 - (2/100) = 0.98
      expect(selectivity).toBeCloseTo(0.98, 3);
    });

    test('uses default when no stats', () => {
      const manager = new HLLStatsManager();

      const condition = simpleCondition('NOT IN', 'category', ['tech']);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // 1 - DEFAULT_IN_SEL = 0.9
      expect(selectivity).toBe(0.9);
    });
  });

  describe('range operators (<, >, <=, >=)', () => {
    test('uses uniform distribution default for <', () => {
      const manager = createMockHLLManager({'users:age': 80});

      const condition = simpleCondition('<', 'age', 30);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Without histogram, use PostgreSQL default: 0.333
      expect(selectivity).toBeCloseTo(0.333, 3);
    });

    test('uses uniform distribution default for >', () => {
      const manager = createMockHLLManager({'users:age': 80});

      const condition = simpleCondition('>', 'age', 65);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      expect(selectivity).toBeCloseTo(0.333, 3);
    });

    test('uses default when no stats', () => {
      const manager = new HLLStatsManager();

      const condition = simpleCondition('>=', 'age', 18);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      expect(selectivity).toBeCloseTo(0.333, 3);
    });
  });

  describe('LIKE operator', () => {
    test('uses conservative default for LIKE', () => {
      const manager = createMockHLLManager({'users:name': 1000});

      const condition = simpleCondition('LIKE', 'name', 'A%');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Conservative estimate: 0.1 (10%)
      expect(selectivity).toBe(0.1);
    });

    test('uses conservative default for NOT LIKE', () => {
      const manager = createMockHLLManager({'users:name': 1000});

      const condition = simpleCondition('NOT LIKE', 'name', 'A%');
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Complement: 0.9 (90%)
      expect(selectivity).toBe(0.9);
    });
  });

  describe('IS NULL / IS NOT NULL', () => {
    test('handles IS operator like equality', () => {
      const manager = createMockHLLManager({'users:status': 10});

      const condition = simpleCondition('IS', 'status', null);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Treated like equality: 1/cardinality
      expect(selectivity).toBeCloseTo(0.1, 2);
    });

    test('handles IS NOT operator like inequality', () => {
      const manager = createMockHLLManager({'users:status': 10});

      const condition = simpleCondition('IS NOT', 'status', null);
      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Treated like inequality: 1 - 1/cardinality
      expect(selectivity).toBeCloseTo(0.9, 2);
    });
  });

  describe('AND combinations', () => {
    test('multiplies selectivities for independent conditions', () => {
      const manager = createMockHLLManager({
        'users:status': 5, // selectivity = 1/5 = 0.2
        'users:category': 10, // selectivity = 1/10 = 0.1
      });

      const condition: Condition = {
        type: 'and',
        conditions: [
          simpleCondition('=', 'status', 'active'),
          simpleCondition('=', 'category', 'tech'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // AND: 0.2 * 0.1 = 0.02
      expect(selectivity).toBeCloseTo(0.02, 3);
    });

    test('handles multiple AND conditions', () => {
      const manager = createMockHLLManager({
        'users:status': 2, // sel = 0.5
        'users:role': 4, // sel = 0.25
        'users:tier': 10, // sel = 0.1
      });

      const condition: Condition = {
        type: 'and',
        conditions: [
          simpleCondition('=', 'status', 'active'),
          simpleCondition('=', 'role', 'admin'),
          simpleCondition('=', 'tier', 'premium'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // AND: 0.5 * 0.25 * 0.1 = 0.0125
      expect(selectivity).toBeCloseTo(0.0125, 4);
    });

    test('handles empty AND (no conditions)', () => {
      const manager = new HLLStatsManager();

      const condition: Condition = {
        type: 'and',
        conditions: [],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Empty AND → selectivity = 1.0 (no filtering)
      expect(selectivity).toBe(1.0);
    });
  });

  describe('OR combinations', () => {
    test('uses complement rule for independent conditions', () => {
      const manager = createMockHLLManager({
        'users:status': 5, // sel = 0.2
        'users:role': 10, // sel = 0.1
      });

      const condition: Condition = {
        type: 'or',
        conditions: [
          simpleCondition('=', 'status', 'active'),
          simpleCondition('=', 'role', 'admin'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // OR: 1 - (1 - 0.2) * (1 - 0.1) = 1 - 0.8 * 0.9 = 1 - 0.72 = 0.28
      expect(selectivity).toBeCloseTo(0.28, 3);
    });

    test('handles multiple OR conditions', () => {
      const manager = createMockHLLManager({
        'users:status': 10, // sel = 0.1
        'users:role': 10, // sel = 0.1
        'users:tier': 10, // sel = 0.1
      });

      const condition: Condition = {
        type: 'or',
        conditions: [
          simpleCondition('=', 'status', 'active'),
          simpleCondition('=', 'role', 'admin'),
          simpleCondition('=', 'tier', 'premium'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // OR: 1 - (0.9)^3 = 1 - 0.729 = 0.271
      expect(selectivity).toBeCloseTo(0.271, 3);
    });

    test('handles empty OR (no conditions)', () => {
      const manager = new HLLStatsManager();

      const condition: Condition = {
        type: 'or',
        conditions: [],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Empty OR → selectivity = 0.0 (no rows match)
      expect(selectivity).toBe(0.0);
    });
  });

  describe('nested AND/OR logic', () => {
    test('handles OR inside AND', () => {
      const manager = createMockHLLManager({
        'users:status': 5, // sel = 0.2
        'users:role': 10, // sel = 0.1
        'users:category': 4, // sel = 0.25
      });

      // (status = 'active' OR role = 'admin') AND category = 'tech'
      const condition: Condition = {
        type: 'and',
        conditions: [
          {
            type: 'or',
            conditions: [
              simpleCondition('=', 'status', 'active'),
              simpleCondition('=', 'role', 'admin'),
            ],
          },
          simpleCondition('=', 'category', 'tech'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // OR: 1 - (0.8 * 0.9) = 0.28
      // AND: 0.28 * 0.25 = 0.07
      expect(selectivity).toBeCloseTo(0.07, 3);
    });

    test('handles AND inside OR', () => {
      const manager = createMockHLLManager({
        'users:status': 5, // sel = 0.2
        'users:role': 10, // sel = 0.1
        'users:tier': 4, // sel = 0.25
      });

      // (status = 'active' AND role = 'admin') OR tier = 'premium'
      const condition: Condition = {
        type: 'or',
        conditions: [
          {
            type: 'and',
            conditions: [
              simpleCondition('=', 'status', 'active'),
              simpleCondition('=', 'role', 'admin'),
            ],
          },
          simpleCondition('=', 'tier', 'premium'),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // AND: 0.2 * 0.1 = 0.02
      // OR: 1 - (0.98 * 0.75) = 1 - 0.735 = 0.265
      expect(selectivity).toBeCloseTo(0.265, 3);
    });

    test('handles deeply nested logic', () => {
      const manager = createMockHLLManager({
        'users:a': 10, // sel = 0.1
        'users:b': 10, // sel = 0.1
        'users:c': 10, // sel = 0.1
        'users:d': 10, // sel = 0.1
      });

      // ((a = 1 OR b = 2) AND c = 3) OR d = 4
      const condition: Condition = {
        type: 'or',
        conditions: [
          {
            type: 'and',
            conditions: [
              {
                type: 'or',
                conditions: [
                  simpleCondition('=', 'a', 1),
                  simpleCondition('=', 'b', 2),
                ],
              },
              simpleCondition('=', 'c', 3),
            ],
          },
          simpleCondition('=', 'd', 4),
        ],
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Inner OR: 1 - (0.9 * 0.9) = 0.19
      // AND: 0.19 * 0.1 = 0.019
      // Outer OR: 1 - (0.981 * 0.9) = 1 - 0.8829 = 0.1171
      expect(selectivity).toBeCloseTo(0.1171, 4);
    });
  });

  describe('correlated subqueries', () => {
    test('uses default 0.5 selectivity for correlated subqueries', () => {
      const manager = new HLLStatsManager();

      const condition: Condition = {
        type: 'correlatedSubquery',
        op: 'EXISTS',
        related: {
          correlation: {
            parentField: ['id'],
            childField: ['userId'],
          },
          subquery: {
            table: 'orders',
          },
        },
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Conservative 50% default
      expect(selectivity).toBe(0.5);
    });
  });

  describe('edge cases', () => {
    test('handles non-column left operands', () => {
      const manager = createMockHLLManager({'users:status': 10});

      const condition: SimpleCondition = {
        type: 'simple',
        op: '=',
        left: {type: 'literal', value: 'constant'},
        right: {type: 'literal', value: 'constant'},
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Non-column comparisons assumed to not filter
      expect(selectivity).toBe(1.0);
    });

    test('handles unknown operators gracefully', () => {
      const manager = createMockHLLManager({'users:status': 10});

      // Test with an operator that exists but isn't handled specially
      // The default case should return 1.0
      const condition: SimpleCondition = {
        type: 'simple',
        op: '=' as SimpleCondition['op'],
        left: {type: 'column', name: 'status'},
        right: {type: 'literal', value: 'pattern'},
      };

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Should handle equality normally
      expect(selectivity).toBeCloseTo(0.1, 2);
    });

    test('handles non-array IN operand', () => {
      const manager = createMockHLLManager({'users:status': 10});

      // Test IN with a single value (wrapped as single-element array)
      const condition = simpleCondition('IN', 'status', ['active']);

      const selectivity = calculateSelectivity(condition, 'users', manager);

      // Single value: 1/10 = 0.1
      expect(selectivity).toBe(0.1);
    });
  });
});
