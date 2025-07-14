/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {mapEntries} from '../../../shared/src/objects.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = (...args: TArg) => TReturnQuery;

export type ContextualizedNamedQuery<
  TContext,
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = {
  (context: TContext, ...args: TArg): TReturnQuery;
  contextualized?: boolean;
};

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema>(s: S): SchemaQuery<S> {
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
function namedQuery(
  name: string,
  fn: NamedQuery<ReadonlyArray<ReadonlyJSONValue>, Query<any, any, any>>,
): NamedQuery<ReadonlyArray<ReadonlyJSONValue>, Query<any, any, any>> {
  return ((...args: ReadonlyArray<ReadonlyJSONValue>) =>
    fn(...args).nameAndArgs(name, args)) as NamedQuery<
    ReadonlyArray<ReadonlyJSONValue>,
    Query<any, any, any>
  >;
}

function contextualizedNamedQuery<TContext>(
  name: string,
  fn: ContextualizedNamedQuery<
    TContext,
    ReadonlyArray<ReadonlyJSONValue>,
    Query<any, any, any>
  >,
): ContextualizedNamedQuery<
  TContext,
  ReadonlyArray<ReadonlyJSONValue>,
  Query<any, any, any>
> {
  const ret = ((context: TContext, ...args: ReadonlyArray<ReadonlyJSONValue>) =>
    fn(context, ...args).nameAndArgs(name, args)) as ContextualizedNamedQuery<
    TContext,
    ReadonlyArray<ReadonlyJSONValue>,
    Query<any, any, any>
  >;
  ret.contextualized = true;

  return ret;
}

export function named<
  TQueries extends {
    [K in keyof TQueries]: TQueries[K] extends NamedQuery<
      infer TArgs,
      Query<any, any, any>
    >
      ? TArgs extends ReadonlyArray<ReadonlyJSONValue>
        ? NamedQuery<TArgs, Query<any, any, any>>
        : never
      : never;
  },
>(queries: TQueries): TQueries {
  return mapEntries(queries, (name, query) => [
    name,
    namedQuery(name, query as any),
  ]) as TQueries;
}

export function namedWithContext<
  TContext,
  TQueries extends {
    [K in keyof TQueries]: TQueries[K] extends ContextualizedNamedQuery<
      TContext,
      infer TArgs,
      Query<any, any, any>
    >
      ? TArgs extends ReadonlyArray<ReadonlyJSONValue>
        ? ContextualizedNamedQuery<TContext, TArgs, Query<any, any, any>>
        : never
      : never;
  },
>(queries: TQueries): TQueries {
  return mapEntries(queries, (name, query) => [
    name,
    contextualizedNamedQuery(name, query as any),
  ]) as TQueries;
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
