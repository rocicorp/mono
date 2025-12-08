import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {QueryParseError} from './error.ts';
import {type AnyQuery} from './query-builder.ts';
import {asQueryInternals} from './query-internals.ts';

/** @deprecated */
export type QueryFn<
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends AnyQuery,
> = TTakesContext extends false
  ? {(...args: TArg): TReturnQuery}
  : {(context: TContext, ...args: TArg): TReturnQuery};

/** @deprecated */
export type SyncedQuery<
  TName extends string,
  TContext,
  TTakesContext extends boolean,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends AnyQuery,
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

/**
 * @deprecated Use {@linkcode defineQuery} instead.
 */
export function syncedQuery<
  TName extends string,
  TArg extends ReadonlyJSONValue[],
  TReturnQuery extends AnyQuery,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: QueryFn<unknown, false, TArg, TReturnQuery>,
): SyncedQuery<TName, unknown, false, TArg, TReturnQuery> {
  const impl = syncedQueryImpl(name, fn, false);
  // oxlint-disable-next-line no-explicit-any
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
  TReturnQuery extends AnyQuery,
>(
  name: TName,
  parser: ParseFn<TArg> | HasParseFn<TArg> | undefined,
  fn: QueryFn<TContext, true, TArg, TReturnQuery>,
): SyncedQuery<TName, TContext, true, TArg, TReturnQuery> {
  const impl = syncedQueryImpl(name, fn, true);
  // oxlint-disable-next-line no-explicit-any
  const ret: any = (context: TContext, ...args: TArg) => impl(context, args);
  ret.queryName = name;
  ret.parse = normalizeParser(parser);
  ret.takesContext = true;
  return ret;
}

/** @deprecated */
function syncedQueryImpl<
  TName extends string,
  TContext,
  TArg extends ReadonlyJSONValue[],
  // oxlint-disable-next-line no-explicit-any
>(name: TName, fn: any, takesContext: boolean) {
  return (context: TContext, args: TArg) => {
    const q = takesContext ? fn(context, ...args) : fn(...args);
    return asQueryInternals(q).nameAndArgs(name, args);
  };
}

/** @deprecated */

// oxlint-disable-next-line no-explicit-any
type AnySyncedQuery = SyncedQuery<any, any, any, any, any>;

/** @deprecated */
export function withValidation<F extends AnySyncedQuery>(
  fn: F,
  // oxlint-disable-next-line no-explicit-any
): F extends SyncedQuery<infer N, infer C, any, any, infer R>
  ? SyncedQuery<N, C, true, ReadonlyJSONValue[], R>
  : F {
  // If we have a parse function this is a SyncedQuery
  if ('parse' in fn) {
    const {parse} = fn;
    if (!parse) {
      throw new Error('ret does not have a parse function defined');
    }
    // oxlint-disable-next-line no-explicit-any
    const ret: any = (context: unknown, ...args: unknown[]) => {
      let parsed;
      try {
        parsed = parse(args);
      } catch (error) {
        throw new QueryParseError({cause: error});
      }
      // oxlint-disable-next-line no-explicit-any
      return fn.takesContext ? fn(context, ...parsed) : (fn as any)(...parsed);
    };
    ret.queryName = fn.queryName;
    ret.parse = fn.parse;
    ret.takesContext = true;

    return ret;
  }

  // If we don't have a parse function, return the function as-is
  // (this shouldn't happen in practice)
  // oxlint-disable-next-line no-explicit-any
  return fn as any;
}

/** @deprecated */
export type ParseFn<T extends ReadonlyJSONValue[]> = (args: unknown[]) => T;

/** @deprecated */
export type HasParseFn<T extends ReadonlyJSONValue[]> = {
  parse: ParseFn<T>;
};

/** @deprecated */
export type Parser<T extends ReadonlyJSONValue[]> = ParseFn<T> | HasParseFn<T>;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};
