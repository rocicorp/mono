import {SimpleOperator, SimpleCondition} from '../ast2/ast.js';
import {Row, Value} from '../ivm2/data.js';
import {getLikePredicate} from './like.js';

export type NonNullValue = Exclude<Value, null | undefined>;
export type SimplePredicate = (rhs: NonNullValue) => boolean;

export function createPredicate(condition: SimpleCondition) {
  const impl = createPredicateImpl(condition.value, condition.op);
  return (row: Row) => {
    const rhs = row[condition.field];
    if (rhs === null || rhs === undefined) {
      return false;
    }
    return impl(rhs);
  };
}

function createPredicateImpl(
  rhs: NonNullValue,
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
      return getLikePredicate(rhs, '');
    case 'NOT LIKE':
      return not(getLikePredicate(rhs, ''));
    case 'ILIKE':
      return getLikePredicate(rhs, 'i');
    case 'NOT ILIKE':
      return not(getLikePredicate(rhs, 'i'));
    default:
      operator satisfies never;
      throw new Error(`Unexpected operator: ${operator}`);
  }
}

function not<T>(f: (lhs: T) => boolean) {
  return (lhs: T) => !f(lhs);
}
