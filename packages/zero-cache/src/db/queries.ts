import {compareUTF8} from 'compare-utf8';
import {ident as id} from 'pg-format';
import type postgres from 'postgres';
import {assert} from '../../../shared/src/asserts.js';
import {type PostgresDB, typeNameByOID} from '../types/pg.js';
import type {RowKey, RowKeyType, RowValue} from '../types/row-key.js';

/**
 * Efficient lookup of multiple rows from a table from row keys.
 *
 * This uses the temporary VALUES table strategy:
 *
 * ```sql
 * WITH keys(col1, col2) AS (VALUES
 *   (val1::type1, val2::type2),
 *   -- etc. for each key --
 * )
 * SELECT * from <table> JOIN keys USING (col1, col2);
 * ```
 *
 * which, as benchmarked by `EXPLAIN ANALYZE`, is faster than a
 * "WHERE IN (array or keys)" query when there is a large number of keys.
 */
export function lookupRowsWithKeys(
  db: PostgresDB,
  schema: string,
  table: string,
  rowKeyType: RowKeyType,
  rowKeys: Iterable<RowKey>,
): postgres.PendingQuery<postgres.Row[]> {
  const colNames = Object.keys(rowKeyType).sort(compareUTF8);
  const cols = colNames
    .map(col => db`${db(col)}`)
    .flatMap((c, i) => (i ? [db`,`, c] : c));
  // Explicit types must be declared for each value, e.g. `( $1::int4, $2::text )`.
  // See https://github.com/porsager/postgres/issues/842
  const colType = (col: string) =>
    db.unsafe(typeNameByOID[rowKeyType[col].typeOid]);
  const values = Array.from(rowKeys, row =>
    colNames
      .map(col => db`${row[col]}::${colType(col)}`)
      .flatMap((v, i) => (i ? [db`,`, v] : v)),
  ).flatMap((tuple, i) => (i ? [db`),(`, ...tuple.flat()] : tuple));

  return db`
    WITH keys (${cols}) AS (VALUES (${values}))
    SELECT * FROM ${db(schema)}.${db(table)} JOIN keys USING (${cols});
  `;
}

export function multiInsertStatement<R extends RowValue>(
  schema: string,
  table: string,
  columns: readonly (string & keyof R)[],
  count: number,
  postamble: string = '',
) {
  assert(count > 0, 'count must be > 0');
  const parts = [
    `INSERT INTO ${id(schema)}.${id(table)} `,
    `(${columns.map(col => id(col)).join(',')}) VALUES `,
  ];
  let p = 1;
  for (let i = 0; i < count; i++) {
    parts.push(i === 0 ? '(' : ',(');
    for (let col = 0; col < columns.length; col++) {
      parts.push(col === 0 ? `$${p}` : `,$${p}`);
      p++;
    }
    parts.push(')');
  }
  if (postamble.length) {
    parts.push(` ${postamble}`);
  }
  return parts.join('');
}

export function multiInsertParams<R extends RowValue>(
  columns: readonly (keyof R)[],
  rows: readonly R[],
) {
  return rows.map(row => columns.map(col => row[col])).flat();
}
