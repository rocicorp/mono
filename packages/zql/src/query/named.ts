/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import type {NamedQueryFunction} from './define-query.ts';
import {newQuery} from './query-impl.ts';
import {asQueryInternals} from './query-internals.ts';
import {type HumanReadable, type Query, type QueryReturn} from './query.ts';

export type QueryFn<
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
> = TTakesContext extends false
  ? {(...args: TArg): TReturnQuery}
  : {(context: TContext, ...args: TArg): TReturnQuery};

export type SyncedQuery<
  TName extends string,
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
> = QueryFn<TContext, TTakesContext, TArg, TReturnQuery> & {
  queryName: TName;
  parse: ParseFn<TArg> | undefined;
  takesContext: TTakesContext;
};

export type QueryFnReturn<TQuery extends QueryFn<any, any, any, any>> =
  HumanReadable<QueryReturn<ReturnType<TQuery>>>;

function normalizeParser<T extends ReadonlyJSONValue[]>(
  parser: ParseFn<T> | HasParseFn<T> | undefined,
): ParseFn<T> | undefined {
  if (parser) {
    if ('parse' in parser) {
      return parser.parse.bind(parser);
    }
    return parser;
  }
  return undefined;
}

/**
 * @deprecated Use {@linkcode defineQuery} instead.
 */
export function syncedQuery<
  TName extends string,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: QueryFn<unknown, false, TArg, TReturnQuery>,
): SyncedQuery<TName, unknown, false, TArg, TReturnQuery> {
  const impl = syncedQueryImpl(name, fn, false);
  const ret: any = (...args: TArg) => impl(undefined, args);
  ret.queryName = name;
  ret.parse = normalizeParser(parser);
  ret.takesContext = false;
  return ret;
}

/**
 * @deprecated Use {@linkcode defineQuery} instead.
 */
export function syncedQueryWithContext<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: QueryFn<TContext, true, TArg, TReturnQuery>,
): SyncedQuery<TName, TContext, true, TArg, TReturnQuery> {
  const impl = syncedQueryImpl(name, fn, true);
  const ret: any = (context: TContext, ...args: TArg) => impl(context, args);
  ret.queryName = name;
  ret.parse = normalizeParser(parser);
  ret.takesContext = true;
  return ret;
}

function syncedQueryImpl<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(name: TName, fn: any, takesContext: boolean) {
  return (context: TContext, args: TArg) => {
    const q = takesContext ? fn(context, ...args) : fn(...args);
    return asQueryInternals(q).nameAndArgs(name, args) as TReturnQuery;
  };
}

export function withValidation<F extends SyncedQuery<any, any, any, any, any>>(
  fn: F,
): F extends SyncedQuery<infer N, infer C, any, infer A, infer R>
  ? SyncedQuery<N, C, true, A, R>
  : never;

export function withValidation<
  F extends NamedQueryFunction<any, any, any, any, any, any, any>,
>(fn: F): F;

export function withValidation<
  F extends
    | SyncedQuery<any, any, any, any, any>
    | NamedQueryFunction<any, any, any, any, any, any, any>,
>(
  fn: F,
): F extends SyncedQuery<infer N, infer C, any, infer A, infer R>
  ? SyncedQuery<N, C, true, A, R>
  : F {
  // If we have a parse function this is a SyncedQuery
  if ('parse' in fn) {
    const {parse} = fn;
    if (!parse) {
      throw new Error('ret does not have a parse function defined');
    }
    const ret: any = (context: unknown, ...args: unknown[]) => {
      const parsed = parse(args);
      return fn.takesContext ? fn(context, ...parsed) : (fn as any)(...parsed);
    };
    ret.queryName = fn.queryName;
    ret.parse = fn.parse;
    ret.takesContext = true;

    return ret;
  }

  // Otherwise this is a NamedQueryFunction which always validates.
  return fn as any;
}

export type ParseFn<T extends ReadonlyJSONValue[]> = (args: unknown[]) => T;

export type HasParseFn<T extends ReadonlyJSONValue[]> = {
  parse: ParseFn<T>;
};

export type Parser<T extends ReadonlyJSONValue[]> = ParseFn<T> | HasParseFn<T>;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

/**
 * Returns a set of query builders for the given schema.
 */
export function createBuilder<S extends Schema, TContext>(
  s: S,
): SchemaQuery<S, TContext> {
  return makeQueryBuilders(s) as SchemaQuery<S, TContext>;
}

/**
 * This produces the query builders for a given schema.
 * For use in Zero on the server to process custom queries.
 */
function makeQueryBuilders<S extends Schema, TContext>(
  schema: S,
): SchemaQuery<S, TContext> {
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
