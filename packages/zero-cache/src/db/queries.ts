/* eslint-disable @typescript-eslint/no-unsafe-assignment, @typescript-eslint/restrict-template-expressions, @typescript-eslint/no-unsafe-member-access, @typescript-eslint/no-unused-expressions, @typescript-eslint/no-unsafe-argument, @typescript-eslint/no-unsafe-return, @typescript-eslint/no-unsafe-call, @typescript-eslint/no-base-to-string, @typescript-eslint/unbound-method, require-await, @typescript-eslint/require-await, @typescript-eslint/naming-convention, @typescript-eslint/no-floating-promises, @typescript-eslint/no-misused-promises, @typescript-eslint/prefer-promise-reject-errors, @typescript-eslint/only-throw-error, @typescript-eslint/no-empty-object-type, @typescript-eslint/await-thenable, @typescript-eslint/no-unnecessary-type-assertion, @typescript-eslint/no-unused-vars, @typescript-eslint/restrict-plus-operands, no-unused-private-class-members */
import {compareUTF8} from 'compare-utf8';
import type postgres from 'postgres';
import {type PostgresDB, typeNameByOID} from '../types/pg.ts';
import type {RowKey, RowKeyType} from '../types/row-key.ts';

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
