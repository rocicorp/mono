import type {Condition} from '../ast/ast.js';
import type {FromSet, WhereCondition} from './entity-query.js';

export function conditionToString<F extends FromSet>(
  c: Condition | WhereCondition<F>,
  paren = false,
): string {
  if (c.op === 'AND' || c.op === 'OR') {
    let s = '';
    if (paren) {
      s += '(';
    }
    {
      const paren = c.op === 'AND' && c.conditions.length > 1;
      s += c.conditions.map(c => conditionToString(c, paren)).join(` ${c.op} `);
    }
    if (paren) {
      s += ')';
    }
    return s;
  }
  return `${
    c.type === 'simple'
      ? typeof c.field === 'string'
        ? c.field
        : c.field.join('.')
      : ''
  } ${c.op} ${(c as {value: {value: unknown}}).value.value}`;
}
