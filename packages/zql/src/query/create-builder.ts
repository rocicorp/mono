import type {Schema} from '../../../zero-types/src/schema.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import type {SchemaQuery} from './schema-query.ts';

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(schema: S): SchemaQuery<S> {
  const factory = (table: keyof S['tables'] & string) =>
    newQuery(schema, table);
  return createBuilderWithQueryFactory(schema, factory);
}

export function createBuilderWithQueryFactory<
  S extends Schema,
  // TQuery extends Query<keyof S['tables'] & string, S>,
>(
  schema: S,
  queryFactory: (table: keyof S['tables'] & string) => Query<string, S>,
): SchemaQuery<S> {
  const cache = new Map<string, Query<string, S>>();
  const {tables} = schema;

  function getQuery(prop: string) {
    const cached = cache.get(prop);
    if (cached) {
      return cached;
    }

    if (!Object.hasOwn(schema.tables, prop)) {
      return undefined;
    }

    const q = queryFactory(prop);
    cache.set(prop, q);
    return q;
  }

  return new Proxy(tables, {
    get: (_target, prop) => {
      if (typeof prop === 'symbol') {
        return undefined;
      }
      const q = getQuery(prop);
      if (!q) {
        throw new Error(`Table ${String(prop)} does not exist in schema`);
      }
      return q;
    },

    getOwnPropertyDescriptor: (_target, prop) => {
      if (typeof prop === 'symbol') {
        return undefined;
      }
      const value = getQuery(prop);
      if (!value) {
        return undefined;
      }
      const desc = Reflect.getOwnPropertyDescriptor(tables, prop);
      return {...desc, value};
    },
  }) as unknown as SchemaQuery<S>;
}
