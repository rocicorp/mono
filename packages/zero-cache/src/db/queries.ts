import {compareUTF8} from 'compare-utf8';
import type postgres from 'postgres';
import {typeNameByOID} from '../types/pg.js';
import type {RowKeyType, RowKeyValue} from '../types/row-key.js';

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
  db: postgres.Sql,
  schema: string,
  table: string,
  rowKeyType: RowKeyType,
  rowKeys: RowKeyValue[],
): postgres.PendingQuery<postgres.Row[]> {
  const colNames = Object.keys(rowKeyType).sort(compareUTF8);
  const cols = colNames
    .map(col => db`${db(col)}`)
    .flatMap((c, i) => (i ? [db`,`, c] : c));
  // Explicit types must be declared for each value, e.g. `( $1::int4, $2::text )`.
  // See https://github.com/porsager/postgres/issues/842
  const colType = (col: string) =>
    db.unsafe(typeNameByOID[rowKeyType[col].typeOid]);
  const values = rowKeys
    .map(row =>
      colNames
        .map(col => db`${row[col]}::${colType(col)}`)
        .flatMap((v, i) => (i ? [db`,`, v] : v)),
    )
    .flatMap((tuple, i) => (i ? [db`),(`, ...tuple.flat()] : tuple));

  return db`
    WITH keys (${cols}) AS (VALUES (${values}))
    SELECT * FROM ${db(schema)}.${db(table)} JOIN keys USING (${cols});
  `;
}
