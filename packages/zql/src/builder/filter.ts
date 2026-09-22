import {assert, unreachable} from '../../../shared/src/asserts.ts';
import type {
  ColumnReference,
  Condition,
  JsonPathReference,
  SimpleCondition,
  SimpleOperator,
} from '../../../zero-protocol/src/ast.ts';
import type {Row, Value} from '../../../zero-protocol/src/data.ts';
import {compareValues} from '../ivm/data.ts';
import {simplifyCondition} from '../query/expression.ts';
import {getLikePredicate} from './like.ts';

export type NonNullValue = Exclude<Value, null | undefined>;
export type SimplePredicate = (rhs: Value) => boolean;
export type SimplePredicateNoNull = (rhs: NonNullValue) => boolean;

/**
 * Reads the left-hand value of a comparison out of a row, navigating into a
 * JSON column for a {@link JsonPathReference}.
 *
 * For path navigation, a missing key (or a null/undefined encountered partway
 * through the path) yields `null`. JSON has no `undefined`, so absence is
 * treated as null — this matches SQLite `json_extract` (which returns SQL NULL
 * for both a missing path and a JSON null), so `IS NULL` behaves identically
 * on the client and against the replica. Plain column reads are unchanged.
 */
function readColumn(row: Row, ref: ColumnReference | JsonPathReference): Value {
  if (ref.type === 'column') {
    return row[ref.name];
  }
  return valueAtPath(row[ref.value.name], ref.path);
}

/**
 * Navigates into a JSON value following `path`, strictly: a `number` segment
 * indexes an array and a `string` segment reads an own property of a plain
 * object — never a string's characters, an array's `length`, or an inherited
 * member such as `constructor`. Any other step (a null/undefined or scalar
 * intermediate, a segment of the wrong kind, a missing key) yields `null`.
 *
 * This is the rule SQLite `json_extract` applies (`$[i]` is array-only and
 * `$."k"` is object-only, anything else is NULL), so the client and the replica
 * it syncs from agree. See {@link readColumn} for why absence is treated as
 * null.
 */
function valueAtPath(value: Value, path: readonly (string | number)[]): Value {
  let v: Value = value;
  for (const seg of path) {
    if (v === null || typeof v !== 'object') {
      return null;
    }
    if (Array.isArray(v)) {
      if (typeof seg !== 'number') {
        return null;
      }
      v = v[seg];
    } else if (typeof seg === 'string' && Object.hasOwn(v, seg)) {
      v = (v as Record<string, Value>)[seg];
    } else {
      return null;
    }
  }
  return v ?? null;
}

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
      return (row: Row) => impl(readColumn(row, left));
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

  if (left.type === 'json') {
    // A JSON leaf is dynamically typed, unlike a schema-typed column, so the
    // comparison is type-strict: a leaf whose JSON type differs from the
    // literal's is never equal — a non-match for a positive operator and a
    // match for a negated one — and never an error. This mirrors the
    // `json_type` / `jsonb_typeof` gates in the SQLite and Postgres backends,
    // and keeps `compareValues` and the LIKE matcher (which assume operands of
    // one type) from throwing on user data.
    const literalType = jsonLiteralType(right.value);
    const negated = negatedOps.has(condition.op);
    return (row: Row) => {
      const lhs = readColumn(row, left);
      if (lhs === null || lhs === undefined) {
        return false;
      }
      // An empty IN/NOT IN list has no type to be strict about: `IN ()` never
      // matches and `NOT IN ()` always does (for a non-null leaf).
      if (literalType === undefined || typeof lhs !== literalType) {
        return negated;
      }
      return impl(lhs);
    };
  }

  return (row: Row) => {
    const lhs = readColumn(row, left);
    if (lhs === null || lhs === undefined) {
      return false;
    }
    return impl(lhs);
  };
}

const negatedOps: ReadonlySet<SimpleOperator> = new Set([
  '!=',
  'NOT LIKE',
  'NOT ILIKE',
  'NOT IN',
]);

/**
 * The JS type a JSON leaf must have to be compared against `literal` (for an
 * `IN`/`NOT IN` list, the type of its first element); `undefined` for an empty
 * list.
 */
function jsonLiteralType(
  literal: NonNullValue | readonly NonNullValue[],
): 'string' | 'number' | 'boolean' | undefined {
  const v = Array.isArray(literal) ? literal[0] : literal;
  switch (typeof v) {
    case 'string':
      return 'string';
    case 'number':
      return 'number';
    case 'boolean':
      return 'boolean';
    default:
      return undefined;
  }
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
    // Use compareValues (UTF-8 / code-point order for strings) so range
    // comparisons match ORDER BY and SQLite. Raw JS `<`/`>` compare strings by
    // UTF-16 code unit, which disagrees for non-BMP characters (e.g. emoji),
    // wrongly including/excluding rows relative to the sort order. Mixed-type
    // comparisons are unsupported (compareValues throws), matching SQLite.
    case '<':
      return lhs => compareValues(lhs, rhs) < 0;
    case '<=':
      return lhs => compareValues(lhs, rhs) <= 0;
    case '>':
      return lhs => compareValues(lhs, rhs) > 0;
    case '>=':
      return lhs => compareValues(lhs, rhs) >= 0;
    case 'LIKE':
      return getLikePredicate(rhs, '');
    case 'NOT LIKE':
      return not(getLikePredicate(rhs, ''));
    case 'ILIKE':
      return getLikePredicate(rhs, 'i');
    case 'NOT ILIKE':
      return not(getLikePredicate(rhs, 'i'));
    case 'IN': {
      assert(Array.isArray(rhs), 'Expected rhs to be an array for IN operator');
      const set = new Set(rhs);
      return lhs => set.has(lhs);
    }
    case 'NOT IN': {
      assert(
        Array.isArray(rhs),
        'Expected rhs to be an array for NOT IN operator',
      );
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
 * If the condition contains any CorrelatedSubqueryConditions, returns a
 * transformed condition which contains no CorrelatedSubqueryCondition(s) but
 * which will filter a subset of the rows that would be filtered by the original
 * condition, or undefined if no such transformation exists.
 *
 * If the condition does not contain any CorrelatedSubqueryConditions
 * returns the condition unmodified and `conditionsRemoved: false`.
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
    case 'and':
    case 'or': {
      const transformedConditions: NoSubqueryCondition[] = [];
      let conditionsRemoved = false;
      for (const cond of filters.conditions) {
        const transformed = transformFilters(cond);
        // If any branch of the OR ends up empty, the entire OR needs
        // to be removed.
        if (transformed.filters === undefined && filters.type === 'or') {
          return {filters: undefined, conditionsRemoved: true};
        }
        conditionsRemoved = conditionsRemoved || transformed.conditionsRemoved;
        if (transformed.filters) {
          transformedConditions.push(transformed.filters);
        }
      }
      return {
        filters: simplifyCondition({
          type: filters.type,
          conditions: transformedConditions,
        }) as NoSubqueryCondition,
        conditionsRemoved,
      };
    }
    default:
      unreachable(filters);
  }
}
