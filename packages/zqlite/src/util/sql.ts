import type {Ordering} from '@rocicorp/zql/src/zql/ast/ast.js';
import type {HoistedCondition} from '@rocicorp/zql/src/zql/ivm/graph/message.js';

export function conditionsAndSortToSQL(
  table: string,
  conditions: HoistedCondition[],
  sort: Ordering | undefined,
) {
  let sql = `SELECT * FROM ${table}`;
  if (conditions.length > 0) {
    sql += ' WHERE ';
    sql += conditions.map(c => `${c.selector[1]} ${c.op} ?`).join(' AND ');
  }
  if (sort) {
    sql += ' ORDER BY ';
    sql += sort[0].map(s => `${s[1]} ${sort[1]}`).join(', ');
  }
  return sql;
}

export function getConditionBindParams(conditions: HoistedCondition[]) {
  return conditions.map(c => c.value);
}
