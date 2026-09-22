import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {AST} from '../../zero-protocol/src/ast.ts';
import type {Format} from '../../zero-types/src/format.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {ServerSchema} from '../../zero-types/src/server-schema.ts';
import {decodeQueryResult} from '../../zql/src/ivm/codec.ts';
import type {Queryable} from '../../zql/src/mutate/custom.ts';
import type {HumanReadable} from '../../zql/src/query/query.ts';

/**
 * Executes a query AST against a PostgreSQL database.
 */
export async function executePostgresQuery<TReturn>(
  queryable: Queryable,
  ast: AST,
  format: Format,
  schema: Schema,
  serverSchema: ServerSchema,
): Promise<HumanReadable<TReturn>> {
  const sqlQuery = formatPgInternalConvert(
    compile(serverSchema, schema, ast, format),
  );

  const pgIterableResult = await queryable.query(
    sqlQuery.text,
    sqlQuery.values,
  );

  const pgArrayResult = Array.isArray(pgIterableResult)
    ? pgIterableResult
    : [...pgIterableResult];

  if (pgArrayResult.length === 0 && format.singular) {
    return undefined as unknown as HumanReadable<TReturn>;
  }

  // Rows come straight from SQL, so codec columns are still in their stored
  // form; decode them so server-side `tx.run()` sees the same app-typed
  // values (e.g. `Date`) as the client's IVM views.
  return decodeQueryResult(
    extractZqlResult(pgArrayResult),
    ast,
    format,
    schema,
  ) as HumanReadable<TReturn>;
}
