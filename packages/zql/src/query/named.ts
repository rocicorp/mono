/* eslint-disable @typescript-eslint/no-explicit-any */
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {SchemaQuery} from '../mutate/custom.ts';
import {newQuery} from './query-impl.ts';
import type {Query} from './query.ts';

export type NamedQuery<
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = {
  (...args: TArg): TReturnQuery;
};

export type NamedQueryWithContext<
  TContext,
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = {
  (context: TContext, ...args: TArg): TReturnQuery;
};

export type SyncedQuery<
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = NamedQuery<TArg, TReturnQuery> & {
  queryName: string;
  takesContext: boolean;
  validator?: Validator<TArg> | undefined;
};

export type SyncedQueryWithContext<
  TContext,
  TArg extends
    ReadonlyArray<ReadonlyJSONValue> = ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any> = Query<any, any, any>,
> = NamedQueryWithContext<TContext, TArg, TReturnQuery> & {
  queryName: string;
  takesContext: true;
  validator?: Validator<TArg> | undefined;
};

export type ValidatedSyncedQuery<TReturnQuery extends Query<any, any, any>> = (
  ...args: unknown[]
) => TReturnQuery;
export type ValidatedSyncedQueryWithContext<
  TContext,
  TReturnQuery extends Query<any, any, any>,
> = (context: TContext, ...args: unknown[]) => TReturnQuery;

export type CustomQueryID = {
  name: string;
  args: ReadonlyArray<ReadonlyJSONValue>;
};

export type Validator<T extends ReadonlyArray<ReadonlyJSONValue>> = (
  args: unknown,
) => T;

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
export function syncedQuery<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  validator: Validator<TArg>,
  fn: NamedQuery<TArg, TReturnQuery>,
): SyncedQuery<TArg, TReturnQuery> & {validator: Validator<TArg>};
export function syncedQuery<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  fn: NamedQuery<TArg, TReturnQuery>,
): SyncedQuery<TArg, TReturnQuery>;
export function syncedQuery<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  validatorOrQueryFn: Validator<TArg> | NamedQuery<TArg, TReturnQuery>,
  maybeQueryFn?: NamedQuery<TArg, TReturnQuery> | undefined,
): SyncedQuery<TArg, TReturnQuery> {
  let fn: NamedQuery<TArg, TReturnQuery>;
  let validator: Validator<TArg> | undefined;
  if (maybeQueryFn === undefined) {
    fn = validatorOrQueryFn as NamedQuery<TArg, TReturnQuery>;
  } else {
    fn = maybeQueryFn;
    validator = validatorOrQueryFn as Validator<TArg>;
  }
  const ret = ((...args: TArg) =>
    fn(...args).nameAndArgs(name, args)) as SyncedQuery<TArg, TReturnQuery>;
  ret.takesContext = false;
  ret.validator = validator;
  ret.queryName = name;
  return ret;
}

export function syncedQueryWithContext<
  TContext,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  validator: Validator<TArg>,
  fn: NamedQueryWithContext<TContext, TArg, TReturnQuery>,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery> & {
  validator: Validator<TArg>;
};
export function syncedQueryWithContext<
  TContext,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  fn: NamedQueryWithContext<TContext, TArg, TReturnQuery>,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery>;
export function syncedQueryWithContext<
  TContext,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  queryOrValidator:
    | NamedQueryWithContext<TContext, TArg, TReturnQuery>
    | Validator<TArg>,
  query?: NamedQueryWithContext<TContext, TArg, TReturnQuery> | undefined,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery> {
  let fn: NamedQueryWithContext<TContext, TArg, TReturnQuery>;
  let validator: Validator<TArg> | undefined;
  if (query === undefined) {
    fn = queryOrValidator as NamedQueryWithContext<
      TContext,
      TArg,
      TReturnQuery
    >;
  } else {
    fn = query;
    validator = queryOrValidator as Validator<TArg>;
  }

  return contextualizedSyncedQuery(name, validator, fn);
}

function contextualizedSyncedQuery<
  TContext,
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  name: string,
  validator: Validator<TArg> | undefined,
  fn: NamedQueryWithContext<TContext, TArg, TReturnQuery>,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery> {
  const ret = ((context: TContext, ...args: TArg) =>
    fn(context, ...args).nameAndArgs(name, args)) as SyncedQueryWithContext<
    TContext,
    TArg,
    TReturnQuery
  >;

  ret.takesContext = true;
  if (validator) {
    ret.validator = validator;
  }
  ret.queryName = name;

  return ret;
}

export function withContext<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
  TContext,
>(
  fn: SyncedQueryWithContext<TContext, TArg, TReturnQuery>,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery>;
export function withContext<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
>(
  fn: SyncedQuery<TArg, TReturnQuery>,
): SyncedQueryWithContext<any, TArg, TReturnQuery>;
export function withContext<
  TArg extends ReadonlyArray<ReadonlyJSONValue>,
  TReturnQuery extends Query<any, any, any>,
  TContext,
>(
  fn:
    | SyncedQuery<TArg, TReturnQuery>
    | SyncedQueryWithContext<TContext, TArg, TReturnQuery>,
): SyncedQueryWithContext<TContext, TArg, TReturnQuery> {
  if (fn.takesContext) {
    return fn as SyncedQueryWithContext<TContext, TArg, TReturnQuery>;
  }

  const contextualized = ((_context: TContext, ...args: TArg) =>
    fn(...args).nameAndArgs(fn.queryName, args)) as SyncedQueryWithContext<
    TContext,
    TArg,
    TReturnQuery
  >;
  contextualized.takesContext = true;

  return contextualized;
}

export function withValidation<TReturnQuery extends Query<any, any, any>>(
  fn: SyncedQuery<any, TReturnQuery>,
): ValidatedSyncedQuery<TReturnQuery>;
export function withValidation<
  TReturnQuery extends Query<any, any, any>,
  TContext,
>(
  fn: SyncedQueryWithContext<TContext, any, TReturnQuery>,
): ValidatedSyncedQueryWithContext<TContext, TReturnQuery>;
export function withValidation<
  TReturnQuery extends Query<any, any, any>,
  TContext = unknown,
>(
  fn:
    | SyncedQuery<ReadonlyArray<ReadonlyJSONValue>, TReturnQuery>
    | SyncedQueryWithContext<
        TContext,
        ReadonlyArray<ReadonlyJSONValue>,
        TReturnQuery
      >,
):
  | ValidatedSyncedQuery<TReturnQuery>
  | ValidatedSyncedQueryWithContext<TContext, TReturnQuery> {
  const {validator, takesContext} = fn;
  if (validator) {
    if (takesContext) {
      return ((context, ...args) =>
        (
          fn as SyncedQueryWithContext<
            TContext,
            ReadonlyArray<ReadonlyJSONValue>,
            TReturnQuery
          >
        )(context, ...validator(args))) as ValidatedSyncedQueryWithContext<
        TContext,
        TReturnQuery
      >;
    }
    return ((...args) =>
      fn(...validator(args))) as ValidatedSyncedQuery<TReturnQuery>;
  }

  throw new Error(fn.name + ' does not have a validator defined');
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
