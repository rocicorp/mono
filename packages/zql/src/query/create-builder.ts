import {recordProxy} from '../../../shared/src/record-proxy.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {QueryDelegate} from './query-delegate.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import {newRunnableQuery} from './runnable-query-impl.ts';
import type {ConditionalSchemaQuery, SchemaQuery} from './schema-query.ts';

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(schema: S): SchemaQuery<S> {
  return createBuilderWithQueryFactory(schema, table =>
    newQuery(schema, table),
  );
}

/** @deprecated Use {@linkcode createBuilder} with `tx.run(zql.table.where(...))` instead. */
export function createRunnableBuilder<S extends Schema>(
  delegate: QueryDelegate,
  schema: S,
): ConditionalSchemaQuery<S> {
  if (!schema.enableLegacyQueries) {
    return undefined as ConditionalSchemaQuery<S>;
  }

  return createBuilderWithQueryFactory(schema, table =>
    newRunnableQuery(delegate, schema, table),
  ) as ConditionalSchemaQuery<S>;
}

function createBuilderWithQueryFactory<S extends Schema>(
  schema: S,
  queryFactory: (table: keyof S['tables'] & string) => Query<string, S>,
): SchemaQuery<S> {
  // Create a target with no prototype so accessing unknown properties returns
  // undefined instead of inherited Object.prototype methods (e.g., toString).
  // This fixes React 19 dev mode compatibility where accessing $$typeof should
  // return undefined rather than throwing.
  const target = Object.assign(
    Object.create(null),
    schema.tables,
  ) as Record<string, unknown>;

  return recordProxy(target, (_tableSchema, prop) =>
    queryFactory(prop),
  ) as SchemaQuery<S>;
}
