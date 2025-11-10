/**
 * Selectivity estimation for query filters using HyperLogLog statistics.
 *
 * This module implements PostgreSQL-style selectivity calculation for WHERE
 * clause conditions. It recursively walks the condition tree and combines
 * operator-specific selectivity estimates using probability rules.
 *
 * Selectivity = fraction of rows that satisfy the condition (0.0 to 1.0)
 *
 * References:
 * - PostgreSQL selfuncs.c: Selectivity functions for query optimizer
 * - "Database Systems: The Complete Book" by Garcia-Molina et al.
 */

import type {
  Condition,
  SimpleCondition,
  SimpleOperator,
} from '../../zero-protocol/src/ast.ts';
import type {HLLStatsManager} from '../../zql/src/planner/stats/hll-stats-manager.ts';

/**
 * PostgreSQL default selectivity constants from selfuncs.c
 */
const DEFAULT_EQ_SEL = 0.005; // 0.5% for equality when no stats
const DEFAULT_INEQ_SEL = 0.3333; // 33.3% for inequalities when no stats
const DEFAULT_IN_SEL = 0.1; // 10% for IN operator when no stats
const DEFAULT_LIKE_SEL = 0.1; // 10% for LIKE pattern matching
const DEFAULT_NOT_LIKE_SEL = 0.9; // 90% for NOT LIKE

/**
 * Calculate selectivity for a condition tree using HyperLogLog statistics.
 *
 * This function recursively walks the condition tree:
 * - Simple conditions: Use operator-specific formulas
 * - AND: Multiply selectivities (independent probability)
 * - OR: Use complement rule (at least one condition true)
 *
 * @param condition The filter condition from the query AST
 * @param tableName The table being queried
 * @param hllManager HLL statistics manager with cardinality estimates
 * @returns Selectivity value between 0.0 and 1.0
 */
export function calculateSelectivity(
  condition: Condition,
  tableName: string,
  hllManager: HLLStatsManager,
): number {
  switch (condition.type) {
    case 'simple':
      return calculateSimpleSelectivity(condition, tableName, hllManager);

    case 'and': {
      // AND: Multiply selectivities (assuming independence)
      // P(A AND B) = P(A) * P(B)
      let result = 1.0;
      for (const cond of condition.conditions) {
        result *= calculateSelectivity(cond, tableName, hllManager);
      }
      return result;
    }

    case 'or': {
      // OR: Use complement rule
      // P(A OR B) = 1 - P(NOT A AND NOT B) = 1 - (1-P(A)) * (1-P(B))
      let complement = 1.0;
      for (const cond of condition.conditions) {
        const sel = calculateSelectivity(cond, tableName, hllManager);
        complement *= 1.0 - sel;
      }
      return 1.0 - complement;
    }

    case 'correlatedSubquery':
      // Cannot estimate correlated subqueries with HLL alone
      // Use conservative 50% selectivity
      return 0.5;
  }
}

/**
 * Calculate selectivity for a simple comparison condition.
 *
 * Uses HyperLogLog cardinality estimates to apply operator-specific
 * formulas following PostgreSQL's approach.
 *
 * @param condition Simple condition (col op value)
 * @param tableName Table being queried
 * @param hllManager HLL statistics manager
 * @returns Selectivity value between 0.0 and 1.0
 */
function calculateSimpleSelectivity(
  condition: SimpleCondition,
  tableName: string,
  hllManager: HLLStatsManager,
): number {
  const {op, left, right} = condition;

  // Only handle column comparisons (left side must be a column)
  if (left.type !== 'column') {
    return 1.0;
  }

  const column = left.name;
  const {cardinality} = hllManager.getCardinality(tableName, column);

  // No statistics available? Use PostgreSQL defaults
  if (cardinality === 0) {
    return getDefaultSelectivity(op);
  }

  switch (op) {
    case '=':
    case 'IS': {
      // Equality: 1 / num_distinct_values
      // Example: 1000 distinct values → 0.001 selectivity
      return 1.0 / cardinality;
    }

    case '!=':
    case 'IS NOT': {
      // Inequality: 1 - equality_selectivity
      // Example: 1000 distinct → 0.999 selectivity
      return 1.0 - 1.0 / cardinality;
    }

    case 'IN': {
      // IN: min(num_values / num_distinct, 1.0)
      // Example: IN (1,2,3) with 100 distinct → 3/100 = 0.03
      if (right.type === 'literal' && Array.isArray(right.value)) {
        const numValues = right.value.length;
        return Math.min(numValues / cardinality, 1.0);
      }
      // Non-literal IN (parameter), use default
      return DEFAULT_IN_SEL;
    }

    case 'NOT IN': {
      // NOT IN: 1 - IN_selectivity
      if (right.type === 'literal' && Array.isArray(right.value)) {
        const numValues = right.value.length;
        const inSel = Math.min(numValues / cardinality, 1.0);
        return 1.0 - inSel;
      }
      return 1.0 - DEFAULT_IN_SEL;
    }

    case '<':
    case '>':
    case '<=':
    case '>=': {
      // Range operators without histogram: assume uniform distribution
      // PostgreSQL uses DEFAULT_INEQ_SEL = 0.333
      //
      // With histogram, would calculate: (value - min) / (max - min)
      // But HLL doesn't track min/max, so use default
      return DEFAULT_INEQ_SEL;
    }

    case 'LIKE':
    case 'ILIKE': {
      // Pattern matching is complex and pattern-dependent
      // PostgreSQL analyzes the pattern (e.g., 'abc%' vs '%abc%')
      //
      // For now, use conservative 10% estimate
      // Could be enhanced with pattern analysis in the future
      return DEFAULT_LIKE_SEL;
    }

    case 'NOT LIKE':
    case 'NOT ILIKE': {
      // NOT LIKE: complement of LIKE selectivity
      return DEFAULT_NOT_LIKE_SEL;
    }

    default: {
      // Unknown operator, assume no filtering
      return 1.0;
    }
  }
}

/**
 * Get default selectivity when HyperLogLog statistics are unavailable.
 *
 * These defaults come from PostgreSQL's selfuncs.c and represent
 * conservative estimates based on operator type.
 *
 * @param op The comparison operator
 * @returns Default selectivity value
 */
function getDefaultSelectivity(op: SimpleOperator): number {
  switch (op) {
    case '=':
    case 'IS':
      return DEFAULT_EQ_SEL; // 0.5%

    case '!=':
    case 'IS NOT':
      return 1.0 - DEFAULT_EQ_SEL; // 99.5%

    case '<':
    case '>':
    case '<=':
    case '>=':
      return DEFAULT_INEQ_SEL; // 33.3%

    case 'IN':
      return DEFAULT_IN_SEL; // 10%

    case 'NOT IN':
      return 1.0 - DEFAULT_IN_SEL; // 90%

    case 'LIKE':
    case 'ILIKE':
      return DEFAULT_LIKE_SEL; // 10%

    case 'NOT LIKE':
    case 'NOT ILIKE':
      return DEFAULT_NOT_LIKE_SEL; // 90%

    default:
      return 1.0; // No filtering
  }
}
