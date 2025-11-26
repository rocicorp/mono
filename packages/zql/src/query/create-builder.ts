import type {Schema} from '../../../zero-types/src/schema.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';
import type {SchemaQuery} from './schema-query.ts';

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(schema: S): SchemaQuery<S> {
  return new Proxy(
    {},
    {
      // oxlint-disable-next-line no-explicit-any
      get: (target: Record<string, Query<S, string, any>>, prop: string) => {
        if (prop in target) {
          return target[prop];
        }

        if (!(prop in schema.tables)) {
          throw new Error(`Table ${prop} does not exist in schema`);
        }

        const q = newQuery(schema, prop);
        target[prop] = q;
        return q;
      },
    },
  ) as SchemaQuery<S>;
}
