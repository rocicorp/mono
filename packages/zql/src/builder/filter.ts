import {assert, unreachable} from '../../../shared/src/asserts.js';
import type {
  Condition,
  SimpleCondition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.js';
import type {Row, Value} from '../../../zero-protocol/src/data.js';
import {getLikePredicate} from './like.js';

export type NonNullValue = Exclude<Value, null | undefined>;
export type SimplePredicate = (rhs: Value) => boolean;
export type SimplePredicateNoNull = (rhs: NonNullValue) => boolean;

export type NoSubqueryCondition =
  | SimpleCondition
  | {
      type: 'and';
      conditions: readonly NoSubqueryCondition[];
    }
  | {
      type: 'or';
      conditions: readonly NoSubqueryCondition[];
    };

export function createPredicate(
  condition: NoSubqueryCondition,
): (row: Row) => boolean {
  if (condition.type !== 'simple') {
    if (condition.conditions.length === 0) {
      return () => false;
    }
    const predicates = condition.conditions.map(c => createPredicate(c));
    return condition.type === 'and'
      ? (row: Row) => {
          // and
          for (const predicate of predicates) {
            if (!predicate(row)) {
              return false;
            }
          }
          return true;
        }
      : (row: Row) => {
          // or
          for (const predicate of predicates) {
            if (predicate(row)) {
              return true;
            }
          }
          return false;
        };
  }
  const {left} = condition;
  const {right} = condition;
  assert(
    right.type !== 'static',
    'static values should be resolved before creating predicates',
  );
  assert(
    left.type !== 'static',
    'static values should be resolved before creating predicates',
  );

  switch (condition.op) {
    case 'IS':
    case 'IS NOT': {
      const impl = createIsPredicate(right.value, condition.op);
      if (left.type === 'literal') {
        const result = impl(left.value);
        return () => result;
      }
      return (row: Row) => impl(row[left.name]);
    }
  }

  if (right.value === null || right.value === undefined) {
    return (_row: Row) => false;
  }

  const impl = createPredicateImpl(right.value, condition.op);
  if (left.type === 'literal') {
    if (left.value === null || left.value === undefined) {
      return (_row: Row) => false;
    }
    const result = impl(left.value);
    return () => result;
  }

  return (row: Row) => {
    const lhs = row[left.name];
    if (lhs === null || lhs === undefined) {
      return false;
    }
    return impl(lhs);
  };
}

function createIsPredicate(
  rhs: Value | readonly Value[],
  operator: 'IS' | 'IS NOT',
): SimplePredicate {
  switch (operator) {
    case 'IS':
      return lhs => lhs === rhs;
    case 'IS NOT':
      return lhs => lhs !== rhs;
  }
}

function createPredicateImpl(
  rhs: NonNullValue | readonly NonNullValue[],
  operator: Exclude<SimpleOperator, 'IS' | 'IS NOT'>,
): SimplePredicateNoNull {
  switch (operator) {
    case '=':
      return lhs => lhs === rhs;
    case '!=':
      return lhs => lhs !== rhs;
    case '<':
      return lhs => lhs < rhs;
    case '<=':
      return lhs => lhs <= rhs;
    case '>':
      return lhs => lhs > rhs;
    case '>=':
      return lhs => lhs >= rhs;
    case 'LIKE':
      return getLikePredicate(rhs, '');
    case 'NOT LIKE':
      return not(getLikePredicate(rhs, ''));
    case 'ILIKE':
      return getLikePredicate(rhs, 'i');
    case 'NOT ILIKE':
      return not(getLikePredicate(rhs, 'i'));
    case 'IN': {
      assert(Array.isArray(rhs));
      const set = new Set(rhs);
      return lhs => set.has(lhs);
    }
    case 'NOT IN': {
      assert(Array.isArray(rhs));
      const set = new Set(rhs);
      return lhs => !set.has(lhs);
    }
    default:
      operator satisfies never;
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

function not<T>(f: (lhs: T) => boolean) {
  return (lhs: T) => !f(lhs);
}

/**
 * Returns a transformed condition which contains no
 * CorrelatedSubqueryCondition(s) but which will filter a subset of the rows
 * that would be filtered by the original condition, or undefined
 * if no such transformation exists.
 *
 * Assumes Condition is in DNF.
 */
export function transformFilters(filters: Condition | undefined): {
  filters: NoSubqueryCondition | undefined;
  conditionsRemoved: boolean;
} {
  if (!filters) {
    return {filters: undefined, conditionsRemoved: false};
  }
  switch (filters.type) {
    case 'simple':
      return {filters, conditionsRemoved: false};
    case 'correlatedSubquery':
      return {filters: undefined, conditionsRemoved: true};
    case 'and': {
      const transformedConditions = [];
      for (const cond of filters.conditions) {
        assert(cond.type === 'simple' || cond.type === 'correlatedSubquery');
        if (cond.type === 'simple') {
          transformedConditions.push(cond);
        }
      }
      const conditionsRemoved =
        transformedConditions.length !== filters.conditions.length;
      if (transformedConditions.length === 0) {
        return {filters: undefined, conditionsRemoved};
      }
      if (transformedConditions.length === 1) {
        return {
          filters: transformedConditions[0],
          conditionsRemoved,
        };
      }
      return {
        filters: {
          type: 'and',
          conditions: transformedConditions,
        },
        conditionsRemoved,
      };
    }
    case 'or': {
      const transformedConditions: NoSubqueryCondition[] = [];
      let conditionsRemoved = false;
      for (const cond of filters.conditions) {
        assert(cond.type !== 'or');
        const transformed = transformFilters(cond);
        if (transformed.filters === undefined) {
          return {filters: undefined, conditionsRemoved: true};
        }
        conditionsRemoved = conditionsRemoved || transformed.conditionsRemoved;
        transformedConditions.push(transformed.filters);
      }
      return {
        filters: {type: 'or', conditions: transformedConditions},
        conditionsRemoved,
      };
    }
    default:
      unreachable(filters);
  }
}
