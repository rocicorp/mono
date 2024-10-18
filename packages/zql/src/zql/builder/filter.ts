import {assert} from '../../../../shared/src/asserts.js';
import type {
  Condition,
  LiteralValue,
  SimpleOperator,
} from '../../../../zero-protocol/src/ast.js';
import type {Row, Value} from '../../../../zero-protocol/src/data.js';
import {getLikePredicate} from './like.js';

export type NonNullValue = Exclude<Value, null | undefined>;
export type SimplePredicate = (rhs: NonNullValue) => boolean;

export function createPredicate(condition: Condition) {
  const impl = createPredicateImpl(
    condition.value as LiteralValue,
    condition.op,
  );
  return (row: Row) => {
    const lhs = row[condition.field];
    if (lhs === null || lhs === undefined) {
      return false;
    }
    return impl(lhs);
  };
}

function createPredicateImpl(
  rhs: NonNullValue | readonly NonNullValue[],
  operator: SimpleOperator,
): SimplePredicate {
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
      return getLikePredicate(rhs as NonNullValue, '');
    case 'NOT LIKE':
      return not(getLikePredicate(rhs as NonNullValue, ''));
    case 'ILIKE':
      return getLikePredicate(rhs as NonNullValue, 'i');
    case 'NOT ILIKE':
      return not(getLikePredicate(rhs as NonNullValue, 'i'));
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
