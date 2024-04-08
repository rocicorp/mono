import {EntitySchema} from '../schema/entity-schema.js';
import {WhereCondition} from './entity-query.js';

export function conditionToString<S extends EntitySchema>(
  c: WhereCondition<S>,
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
  return `${(c as {field: string}).field} ${c.op} ${(c as {value: {value: unknown}}).value.value}`;
}
