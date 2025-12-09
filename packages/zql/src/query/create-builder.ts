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
export function createBuilder<S extends Schema>(schema: S): SchemaQuery<S> {
  return createBuilderWithQueryFactory(schema, table =>
    newQuery(schema, table),
  );
}

export function createRunnableBuilder<S extends Schema>(
  delegate: QueryDelegate,
  schema: S,
): SchemaQuery<S> {
  return createBuilderWithQueryFactory(schema, table =>
    newRunnableQuery(delegate, schema, table),
  );
}

function createBuilderWithQueryFactory<S extends Schema>(
  schema: S,
  queryFactory: (table: keyof S['tables'] & string) => Query<string, S>,
): SchemaQuery<S> {
  return recordProxy(
    schema.tables,
    (_tableSchema, prop) => {
      if (!schema.enableLegacyQueries) {
        throw new Error(
          `Legacy queries are disabled for your schema - use \`z.run(zql.${prop}.where(...))\` instead.`,
        );
      }
      return queryFactory(prop);
    },
    prop => {
      throw new Error(`Table ${prop} does not exist in schema`);
    },
  ) as unknown as SchemaQuery<S>;
}
