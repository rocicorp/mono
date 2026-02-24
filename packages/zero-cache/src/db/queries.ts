import {compareUTF8} from 'compare-utf8';
import type postgres from 'postgres';
import {type PostgresDB, typeNameByOID} from '../types/pg.ts';
import type {RowKey, RowKeyType} from '../types/row-key.ts';

/**
 * Efficient lookup of multiple rows from a table from row keys.
 *
 * This uses `unnest` to create a virtual keys table:
 *
 * ```sql
 * WITH keys(col1, col2) AS (
 *   SELECT * FROM unnest($1::type1[], $2::type2[])
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

  // Collect values into per-column arrays for unnest.
  const columnArrays: unknown[][] = colNames.map(() => []);
  for (const row of rowKeys) {
    for (let i = 0; i < colNames.length; i++) {
      columnArrays[i].push(row[colNames[i]]);
    }
  }

  const unnestArgs = colNames
    .map((col, i) => {
      const oid = rowKeyType[col].typeOid;
      const typeName = typeNameByOID[oid];
      return db`${db.array(columnArrays[i], oid)}::${db.unsafe(typeName)}[]`;
    })
    .flatMap((v, i) => (i ? [db`,`, v] : v));

  return db`
    WITH keys (${cols}) AS (SELECT * FROM unnest(${unnestArgs}))
    SELECT * FROM ${db(schema)}.${db(table)} JOIN keys USING (${cols});
  `;
}
