import type {StandardSchemaV1} from '@standard-schema/spec';
import {getValueAtPath} from '../../../shared/src/get-value-at-path.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {asQueryInternals} from './query-internals.ts';
import type {Query} from './query.ts';
import {validateInput} from './validate-input.ts';

const customQueryTag = Symbol();

/**
 * CustomQuery is what is returned from defineQueries. It supports a builder
 * pattern where args is set before calling toQuery(context).
 *
 * const queries = defineQueries(...);
 * queries.foo.bar satisfies CustomQuery<...>
 *
 * Usage:
 *   queries.foo(args).toQuery(ctx)
 */
export type CustomQuery<
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
  Args extends ReadonlyJSONValue | undefined,
  HasArgs extends boolean = false,
> = {
  [customQueryTag]: true;
} & (HasArgs extends true
  ? unknown
  : undefined extends Args
    ? {
        (): CustomQuery<S, T, R, C, Args, true>;
        (args?: Args): CustomQuery<S, T, R, C, Args, true>;
      }
    : {
        (args: Args): CustomQuery<S, T, R, C, Args, true>;
      }) &
  (HasArgs extends true ? {toQuery(ctx: C): Query<S, T, R>} : unknown);

/**
 * A CustomQuery that has args bound/set. Can be passed to Zero's run/preload/materialize
 * methods, which will add context internally.
 */
export type BoundCustomQuery<
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
> = CustomQuery<S, T, R, C, ReadonlyJSONValue | undefined, true>;

/**
 * Checks if a value is a BoundCustomQuery (has args set).
 */
export function isBoundCustomQuery<
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
>(value: unknown): value is BoundCustomQuery<S, T, R, C> {
  // CustomQuery is a callable (function) with extra properties
  if (typeof value !== 'function') {
    return false;
  }

  // oxlint-disable-next-line no-explicit-any
  const obj = value as any;
  return Boolean(obj[customQueryTag] && obj[hasBoundArgs]);
}

// oxlint-disable-next-line no-explicit-any
export type CustomQueries<QD extends QueryDefinitions<Schema, any>> =
  QD extends QueryDefinitions<infer S, infer _C>
    ? CustomQueriesInner<QD, S>
    : never;

type CustomQueriesInner<MD, S extends Schema> = {
  readonly [K in keyof MD]: MD[K] extends QueryDefinition<
    S,
    infer TTable,
    infer TReturn,
    infer TContext,
    // oxlint-disable-next-line no-explicit-any
    any,
    infer TOutput
  >
    ? CustomQuery<S, TTable, TReturn, TContext, TOutput>
    : // oxlint-disable-next-line no-explicit-any
      MD[K] extends QueryDefinitions<S, any>
      ? CustomQueriesInner<MD[K], S>
      : never;
};

export type ContextTypeOfCustomQueries<CQ> =
  CQ extends CustomQueries<infer QD>
    ? QD extends QueryDefinitions<Schema, infer C>
      ? C
      : never
    : never;

const defineQueryTag = Symbol();

type QueryDefinitionFunction<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  Args extends ReadonlyJSONValue | undefined,
> = (options: {args: Args; ctx: TContext}) => Query<TSchema, TTable, TReturn>;

/**
 * A query definition is the function callback that you pass into defineQuery.
 */
export type QueryDefinition<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
> = QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput> & {
  [defineQueryTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
};

export function isQueryDefinition<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  f: unknown,
): f is QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput> {
  // oxlint-disable-next-line no-explicit-any
  return typeof f === 'function' && (f as any)[defineQueryTag];
}

export type QueryDefinitions<S extends Schema, Context> = {
  readonly [key: string]: // oxlint-disable-next-line no-explicit-any
  | QueryDefinition<S, any, any, Context, any, any>
    | QueryDefinitions<S, Context>;
};

// Overload for no validator parameter with default inference for untyped functions
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  queryFn: QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TArgs>,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TArgs, TArgs>;

// Overload for validator parameter - Input and Output can be different
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  queryFn: QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput>,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput>;

// Implementation
export function defineQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validatorOrQueryFn:
    | StandardSchemaV1<TInput, TOutput>
    | QueryDefinitionFunction<TSchema, TTable, TReturn, TContext, TOutput>,
  queryFn?: QueryDefinitionFunction<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput
  >,
): QueryDefinition<TSchema, TTable, TReturn, TContext, TInput, TOutput> {
  // Handle different parameter patterns
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualQueryFn: QueryDefinitionFunction<
    TSchema,
    TTable,
    TReturn,
    TContext,
    TOutput
  >;

  if (typeof validatorOrQueryFn === 'function') {
    // defineQuery(queryFn) - no validator
    validator = undefined;
    actualQueryFn = validatorOrQueryFn;
  } else {
    // defineQuery(validator, queryFn) - with validator
    validator = validatorOrQueryFn;
    actualQueryFn = must(queryFn);
  }

  // We wrap the function to add the tag and validator and ensure we do not mutate it in place.
  const f = (options: {args: TOutput; ctx: TContext}) => actualQueryFn(options);
  f.validator = validator;
  f[defineQueryTag] = true as const;
  return f;
}

// function wrap<TArgs, Context>(
//   queryName: string,
//   // oxlint-disable-next-line no-explicit-any
//   f: QueryDefinition<any, any, any, any, any, any>,
//   contextHolder: {context: Context},
// ): (args: TArgs) => AnyQuery {
//   const {validator} = f;
//   const validate = validator
//     ? (args: TArgs) =>
//         validateInput<TArgs, TArgs>(queryName, args, validator, 'query')
//     : (args: TArgs) => args;

//   return (args?: TArgs) => {
//     // The args that we send to the server is the args that the user passed in.
//     // This is what gets fed into the validator.
//     const q = f({
//       args: validate(args as TArgs),
//       ctx: contextHolder.context,
//     });
//     return asQueryInternals(q).nameAndArgs(
//       queryName,
//       // TODO(arv): Get rid of the array?
//       args === undefined ? [] : [args as unknown as ReadonlyJSONValue],
//     );
//   };
// }

interface CustomQueryState {
  args: ReadonlyJSONValue | undefined;
  hasArgs: boolean;
}

const hasBoundArgs = Symbol();

function createCustomQueryBuilder<
  S extends Schema,
  T extends keyof S['tables'] & string,
  R,
  C,
  Args extends ReadonlyJSONValue | undefined,
  HasArgs extends boolean,
>(
  // oxlint-disable-next-line no-explicit-any
  queryDef: QueryDefinition<S, T, R, C, any, Args>,
  name: string,
  state: CustomQueryState,
): CustomQuery<S, T, R, C, Args, HasArgs> {
  const {validator} = queryDef;

  // The callable function that sets args
  // oxlint-disable-next-line no-explicit-any
  const builder: any = (args: Args) => {
    if (state.hasArgs) {
      throw new Error('args already set');
    }
    const validatedArgs = validateInput(name, args, validator, 'query');
    return createCustomQueryBuilder<S, T, R, C, Args, true>(queryDef, name, {
      args: validatedArgs,
      hasArgs: true,
    });
  };

  // Add create method
  builder.toQuery = (ctx: C) => {
    if (!state.hasArgs) {
      throw new Error('args not set');
    }
    const {args} = state as unknown as {args: Args};
    return asQueryInternals(
      queryDef({
        args,
        ctx,
      }),
    ).nameAndArgs(
      name,
      // TODO(arv): Get rid of the array?
      args === undefined ? [] : [args as unknown as ReadonlyJSONValue],
    );
  };

  // Add the tag
  builder[customQueryTag] = true;

  builder[hasBoundArgs] = state.hasArgs;

  return builder as CustomQuery<S, T, R, C, Args, HasArgs>;
}

export function defineQueries<
  // oxlint-disable-next-line no-explicit-any
  QD extends QueryDefinitions<Schema, any>,
>(defs: QD): CustomQueries<QD> {
  function processDefinitions(
    definitions: QueryDefinitions<Schema, unknown>,
    path: string[],
    // oxlint-disable-next-line no-explicit-any
  ): Record<string, any> {
    // oxlint-disable-next-line no-explicit-any
    const result: Record<string, any> = {};

    for (const [key, value] of Object.entries(definitions)) {
      const currentPath = [...path, key];
      const defaultName = currentPath.join('.');

      if (isQueryDefinition(value)) {
        result[key] = createCustomQueryBuilder(value, defaultName, {
          args: undefined,
          hasArgs: false,
        });
      } else {
        // Nested definitions
        result[key] = processDefinitions(
          value as QueryDefinitions<Schema, unknown>,
          currentPath,
        );
      }
    }

    return result;
  }

  return processDefinitions(defs, []) as CustomQueries<QD>;
}

export function defineQueriesWithType<S extends Schema, C = unknown>(): <
  QD extends QueryDefinitions<S, C>,
>(
  defs: QD,
) => CustomQueries<QD>;

export function defineQueriesWithType<C>(): <
  QD extends QueryDefinitions<Schema, C>,
>(
  defs: QD,
) => CustomQueries<QD>;

export function defineQueriesWithType() {
  return defineQueries;
}

// oxlint-disable-next-line no-explicit-any
export function getQuery<S extends Schema, QD extends QueryDefinitions<S, any>>(
  queries: CustomQueries<QD>,
  name: string,
):
  | CustomQuery<
      S,
      keyof S['tables'] & string,
      unknown, // return
      unknown, // context
      ReadonlyJSONValue | undefined,
      false
    >
  | undefined {
  return getValueAtPath(queries, name, /[.|]/) as
    | CustomQuery<
        S,
        keyof S['tables'] & string,
        unknown, // return
        unknown, // context
        ReadonlyJSONValue | undefined,
        false
      >
    | undefined;
}

export function mustGetQuery<
  S extends Schema,
  // oxlint-disable-next-line no-explicit-any
  QD extends QueryDefinitions<S, any>,
>(
  queries: CustomQueries<QD>,
  name: string,
): CustomQuery<
  S,
  keyof S['tables'] & string,
  unknown, // return
  unknown, // context
  ReadonlyJSONValue | undefined,
  false
> {
  const v = getQuery(queries, name);
  if (!v) {
    throw new Error(`Query not found: ${name}`);
  }
  return v;
}
