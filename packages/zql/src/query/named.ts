/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
> = (...args: TArg) => TReturnQuery;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

type NamedQueryImpl<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
> = (...arg: TArg) => TReturnQuery;

export function query<S extends Schema>(s: S): SchemaQuery<S>;
export function query<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
>(
  s: S,
  name: string,
  fn: NamedQueryImpl<S, TArg, TReturnQuery>,
): NamedQuery<S, TArg, TReturnQuery>;
export function query<
  S extends Schema,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<S, keyof S['tables'] & string>,
>(
  s: S,
  name?: string | undefined,
  fn?: NamedQueryImpl<S, TArg, TReturnQuery> | undefined,
): NamedQuery<S, TArg, TReturnQuery> | SchemaQuery<S> {
  if (name === undefined || fn === undefined) {
    return makeQueryBuilders(s) as SchemaQuery<S>;
  }

  return ((...args: TArg) => fn(...args).nameAndArgs(name, args)) as NamedQuery<
    S,
    TArg,
    TReturnQuery
  >;
}

query.bindTo =
  <S extends Schema>(s: S) =>
  <
    TArg extends ReadonlyArray<ReadonlyJSONValue>,
    TReturnQuery extends Query<S, keyof S['tables'] & string>,
  >(
    name: string,
    fn: NamedQueryImpl<S, TArg, TReturnQuery>,
  ): NamedQuery<S, TArg, TReturnQuery> =>
    query(s, name, fn);

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
