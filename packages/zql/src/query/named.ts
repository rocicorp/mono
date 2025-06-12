/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
> = (...args: TArg) => TReturnQuery;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

export type NamedQueryImpl<
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = (...arg: TArg) => TReturnQuery;

/**
 * Returns a set of query builders for the given schema.
 */
export function querify<S extends Schema>(s: S): SchemaQuery<S> {
  return makeQueryBuilders(s) as SchemaQuery<S>;
}

/**
 * Tags a query with a name and arguments.
 * Named queries are run on both the client and server.
 * The server will receive the name and arguments for a named query and can
 * either run the same query the client did or a completely different one.
 *
 * The main use case here is to apply permissions to the requested query or
 * to expand the scope of the query to include additional data. E.g., for preloading.
 */
export function namedQuery<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  fn: NamedQueryImpl<TArg, TReturnQuery>,
): NamedQuery<TArg, TReturnQuery> {
  return ((...args: TArg) => fn(...args).nameAndArgs(name, args)) as NamedQuery<
    TArg,
    TReturnQuery
  >;
}

/**
 * This produces the query builders for a given schema.
 * For use in Zero on the server to process custom queries.
 */
function makeQueryBuilders<S extends Schema>(schema: S): SchemaQuery<S> {
  return new Proxy(
    {},
    {
      get: (
        target: Record<
          string,
          Omit<Query<S, string, any>, 'materialize' | 'preload'>
        >,
        prop: string,
      ) => {
        if (prop in target) {
          return target[prop];
        }

        if (!(prop in schema.tables)) {
          throw new Error(`Table ${prop} does not exist in schema`);
        }

        const q = newQuery(undefined, schema, prop);
        target[prop] = q;
        return q;
      },
    },
  ) as SchemaQuery<S>;
}
