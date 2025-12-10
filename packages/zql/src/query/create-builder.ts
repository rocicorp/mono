import {recordProxy} from '../../../shared/src/record-proxy.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {newRunnableQuery} from './runnable-query-impl.ts';
import type {SchemaQuery} from './schema-query.ts';

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(
  schema: S,
): SchemaQuery<S, true> {
  return createBuilderWithQueryFactory<S, true>(schema, table =>
    newQuery(schema, table),
  );
}

/** @deprecated Use {@linkcode createBuilder} with `tx.run(zql.table.where(...))` instead. */
export function createRunnableBuilder<S extends Schema>(
  delegate: QueryDelegate,
  schema: S,
): SchemaQuery<S> {
  if (!schema.enableLegacyQueries) {
    return undefined as SchemaQuery<S>;
  }

  return createBuilderWithQueryFactory<S, false>(schema, table =>
    newRunnableQuery(delegate, schema, table),
  );
}

function createBuilderWithQueryFactory<
  S extends Schema,
  ForceEnable extends boolean,
>(
  schema: S,
  queryFactory: (table: keyof S['tables'] & string) => Query<string, S>,
): SchemaQuery<S, ForceEnable> {
  return recordProxy(
    schema.tables,
    (_tableSchema, prop) => queryFactory(prop),
    prop => {
      throw new Error(`Table ${prop} does not exist in schema`);
    },
  ) as SchemaQuery<S, ForceEnable>;
}
