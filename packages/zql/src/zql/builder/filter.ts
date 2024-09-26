import {assert} from 'shared/src/asserts.js';
import type {Condition, Parameter, SimpleOperator} from '../ast/ast.js';
import type {Row, Value} from '../ivm/data.js';
import type {StaticQueryParameters} from './builder.js';
import {getLikePredicate} from './like.js';

export type NonNullValue = Exclude<Value, null | undefined>;
export type SimplePredicate = (rhs: NonNullValue) => boolean;

export function createPredicate(
  condition: Condition,
  staticQueryParameters: StaticQueryParameters | undefined,
) {
  const {value} = condition;
  if (isParameter(value)) {
    assert(
      staticQueryParameters !== undefined,
      'Got a parameterized condition but no staticQueryParameters',
    );
    const anchor = staticQueryParameters[value.anchor];
    assert(anchor !== undefined, `Missing parameter: ${value.anchor}`);
    const impl = createPredicateImpl(
      anchor[value.field] as NonNullValue | readonly NonNullValue[],
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
  const impl = createPredicateImpl(value, condition.op);
  return (row: Row) => {
    const lhs = row[condition.field];
    if (lhs === null || lhs === undefined) {
      return false;
    }
    return impl(lhs);
  };
}

function isParameter(value: unknown): value is Parameter {
  return typeof value === 'object' && value !== null && 'type' in value;
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
