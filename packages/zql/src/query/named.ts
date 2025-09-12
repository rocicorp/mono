/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';

export type QueryFn<
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any> | Promise<Query<any, any, any>>,
> = TTakesContext extends false
  ? {(...args: TArg): TReturnQuery}
  : {(context: TContext, ...args: TArg): TReturnQuery};

export type SyncedQuery<
  TName extends string,
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any> | Promise<Query<any, any, any>>,
> = QueryFn<TContext, TTakesContext, TArg, TReturnQuery> & {
  queryName: TName;
  parse: ParseFn<TArg> | undefined;
  takesContext: TTakesContext;
};

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

// Overload for async functions
export function syncedQuery<
  TName extends string,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (...args: TArg) => Promise<TReturnQuery>,
): SyncedQuery<TName, unknown, false, TArg, Promise<TReturnQuery>>;

// Overload for sync functions
export function syncedQuery<
  TName extends string,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (...args: TArg) => TReturnQuery,
): SyncedQuery<TName, unknown, false, TArg, TReturnQuery>;

// Implementation
export function syncedQuery<
  TName extends string,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (...args: TArg) => TReturnQuery | Promise<TReturnQuery>,
): SyncedQuery<TName, unknown, false, TArg, TReturnQuery | Promise<TReturnQuery>> {
  const impl = syncedQueryImpl(name, fn, false);
  const ret: any = (...args: TArg) => impl(undefined, args);
  ret.queryName = name;
  ret.parse = normalizeParser(parser);
  ret.takesContext = false;
  return ret;
}

// Overload for async functions
export function syncedQueryWithContext<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (context: TContext, ...args: TArg) => Promise<TReturnQuery>,
): SyncedQuery<TName, TContext, true, TArg, Promise<TReturnQuery>>;

// Overload for sync functions
export function syncedQueryWithContext<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (context: TContext, ...args: TArg) => TReturnQuery,
): SyncedQuery<TName, TContext, true, TArg, TReturnQuery>;

// Implementation
export function syncedQueryWithContext<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends Query<any, any, any>,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: (context: TContext, ...args: TArg) => TReturnQuery | Promise<TReturnQuery>,
): SyncedQuery<TName, TContext, true, TArg, TReturnQuery | Promise<TReturnQuery>> {
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
  TReturnQuery extends Query<any, any, any> | Promise<Query<any, any, any>>,
>(name: TName, fn: any, takesContext: boolean) {
  return (context: TContext, args: TArg) => {
    const result = takesContext ? fn(context, ...args) : fn(...args);
    // Check if result is a Promise (not a Query with a then method)
    if (result instanceof Promise) {
      return result.then((q: Query<any, any, any>) => q.nameAndArgs(name, args));
    }
    return result.nameAndArgs(name, args) as TReturnQuery;
  };
}

export type WithValidationReturn<R> = R extends Promise<infer Q>
  ? Promise<{query: Q}>
  : {query: R};

// withValidation returns a function that returns wrapped queries, not a SyncedQuery
export function withValidation<T extends SyncedQuery<any, any, any, any, any>>(
  fn: T,
): T extends SyncedQuery<infer N, infer C, any, any, infer R>
  ? {
      (context: C, ...args: ReadonlyJSONValue[]): WithValidationReturn<R>;
      queryName: N;
      parse: typeof fn.parse;
      takesContext: true;
    }
  : never {
  if (!fn.parse) {
    throw new Error('ret does not have a parse function defined');
  }
  const ret: any = (context: unknown, ...args: unknown[]) => {
    const f = fn as any;
    const parsed = f.parse(args);
    const result = f.takesContext ? f(context, ...parsed) : f(...parsed);
    
    // Wrap the result in an object to prevent automatic promise unwrapping
    if (result instanceof Promise) {
      return result.then((query: any) => ({query}));
    }
    return {query: result};
  };
  ret.queryName = fn.queryName;
  ret.parse = fn.parse;
  ret.takesContext = true;

  return ret;
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
export function createBuilder<S extends Schema>(s: S): SchemaQuery<S> {
  return makeQueryBuilders(s) as SchemaQuery<S>;
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
