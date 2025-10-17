import {compile, extractZqlResult} from '../../z2s/src/compiler.ts';
import {formatPgInternalConvert} from '../../z2s/src/sql.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {DBTransaction} from '../../zql/src/mutate/custom.ts';
import {queryWithContext} from '../../zql/src/query/query-internals.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import {getServerSchema} from './schema.ts';

export async function runQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
>(
  dbTransaction: DBTransaction<unknown>,
  schema: TSchema,
  context: TContext,
  query: Query<TSchema, TTable, TReturn, TContext>,
): Promise<HumanReadable<TReturn>> {
  const serverSchema = await getServerSchema(dbTransaction, schema);

  const internalQuery = queryWithContext(query, context);

  const sqlQuery = formatPgInternalConvert(
    compile(
      serverSchema,
      schema,
      internalQuery.completedAST,
      internalQuery.format,
    ),
  );

  const pgIterableResult = await dbTransaction.query(
    sqlQuery.text,
    sqlQuery.values,
  );

  const pgArrayResult = Array.isArray(pgIterableResult)
    ? pgIterableResult
    : [...pgIterableResult];
  if (pgArrayResult.length === 0 && internalQuery.format.singular) {
    return undefined as unknown as HumanReadable<TReturn>;
  }

  return extractZqlResult(pgArrayResult) as HumanReadable<TReturn>;
}

/**
 * QueryRunner executes ZQL queries using the provided schema, server schema,
 * context, and database transaction.
 */
export class QueryRunner<TSchema extends Schema, TContext> {
  readonly #schema: TSchema;
  readonly #context: TContext;
  readonly #dbTransaction: DBTransaction<unknown>;

  constructor(
    schema: TSchema,
    context: TContext,
    dbTransaction: DBTransaction<unknown>,
  ) {
    this.#schema = schema;
    this.#context = context;
    this.#dbTransaction = dbTransaction;
  }

  run<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TSchema, TTable, TReturn, TContext>,
  ): Promise<HumanReadable<TReturn>> {
    return runQuery(this.#dbTransaction, this.#schema, this.#context, query);
  }
}

/**
 * Factory function to create a query runner function for a given schema.
 * @returns A function that runs queries using the provided transaction, server
 * schema, and context.
 */
export function makeQueryRun<TSchema extends Schema, TContext>(
  schema: TSchema,
): (
  dbTransaction: DBTransaction<unknown>,
  context: TContext,
) => RunQueryFunc<TSchema, TContext> {
  return (transaction, context) =>
    <TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TSchema, TTable, TReturn, TContext>,
    ) =>
      runQuery(transaction, schema, context, query);
}

/**
 * Type for a function that runs a ZQL query and returns a promise of the result.
 * @returns Promise resolving to the query result in human-readable format.
 */
export type RunQueryFunc<TSchema extends Schema, TContext> = <
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: Query<TSchema, TTable, TReturn, TContext>,
) => Promise<HumanReadable<TReturn>>;
