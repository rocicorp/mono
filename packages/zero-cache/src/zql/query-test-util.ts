import type {
  Condition,
  Conjunction,
  Primitive,
  Selector,
  SimpleCondition,
  SimpleOperator,
} from '@rocicorp/zql/src/zql/ast/ast.js';

// Readability helpers
export function and(...conditions: Condition[]): Conjunction {
  return {
    type: 'conjunction',
    op: 'AND',
    conditions,
  };
}

export function or(...conditions: Condition[]): Conjunction {
  return {
    type: 'conjunction',
    op: 'OR',
    conditions,
  };
}

export function cond(
  field: Selector,
  op: SimpleOperator,
  value: Primitive,
): SimpleCondition {
  return {
    type: 'simple',
    field,
    op,
    value: {
      type: 'value',
      value,
    },
  };
}
export function stripCommentsAndWhitespace(query: string = '') {
  return query
    .trim()
    .replaceAll(/--.*\n/g, '')
    .replaceAll(/\s+/g, ' ');
}
