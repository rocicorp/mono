import type {Schema} from '../../../zero-types/src/schema.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import type {SchemaQuery} from './schema-query.ts';

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(schema: S): SchemaQuery<S> {
  // oxlint-disable-next-line no-explicit-any
  const cache = new Map<string, Query<S, string, any>>();
  const {tables} = schema;

  function getQuery(prop: string | symbol) {
    if (typeof prop === 'symbol') {
      return undefined;
    }
    const cached = cache.get(prop);
    if (cached) {
      return cached;
    }

    if (!Object.hasOwn(schema.tables, prop)) {
      return undefined;
    }

    const q = newQuery(schema, prop);
    cache.set(prop, q);
    return q;
  }

  return new Proxy(tables, {
    get: (_target, prop) => {
      const q = getQuery(prop);
      if (!q) {
        throw new Error(`Table ${String(prop)} does not exist in schema`);
      }
      return q;
    },

    getOwnPropertyDescriptor: (_target, prop) => {
      const value = getQuery(prop);
      if (!value) {
        return undefined;
      }
      const desc = Reflect.getOwnPropertyDescriptor(tables, prop);
      return {...desc, value};
    },
  }) as unknown as SchemaQuery<S>;
}
