import {isIntrospectionProperty} from '../../../shared/src/introspection.ts';
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
  return recordProxy(
    schema.tables,
    (_tableSchema, prop) => queryFactory(prop),
    prop => {
      if (isIntrospectionProperty(prop)) {
        return;
      }
      throw new Error(`Table ${prop} does not exist in schema`);
    },
  ) as SchemaQuery<S>;
}
