import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Transaction} from './custom.ts';
import {validateInput} from '../query/validate-input.ts';

const defineMutatorTag = Symbol();

/**
 * A mutator definition function that has been wrapped by `defineMutator`.
 * Contains the original function plus metadata (validator and tag).
 */
export type MutatorDefinition<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
> = ((
  tx: Transaction<TSchema, TContext>,
  options: {
    args: TOutput;
    ctx: TContext;
  },
) => Promise<void>) & {
  [defineMutatorTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
};

export function isMutatorDefinition<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  f: unknown,
): f is MutatorDefinition<TSchema, TContext, TInput, TOutput> {
  // oxlint-disable-next-line no-explicit-any
  return typeof f === 'function' && (f as any)[defineMutatorTag];
}

/**
 * Tree of mutator definitions, supporting arbitrary nesting.
 */
export type MutatorDefinitions<S extends Schema, TContext> = {
  readonly [key: string]:
    | MutatorDefinition<S, TContext, any, any>
    | MutatorDefinitions<S, TContext>;
};

// Overload for no validator parameter
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
>(
  mutatorFn: (
    tx: Transaction<TSchema, TContext>,
    options: {
      args: TArgs;
      ctx: TContext;
    },
  ) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TArgs, TArgs>;

// Overload for validator parameter
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  mutatorFn: (
    tx: Transaction<TSchema, TContext>,
    options: {
      args: TOutput;
      ctx: TContext;
    },
  ) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TInput, TOutput>;

// Implementation
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
>(
  validatorOrMutatorFn:
    | StandardSchemaV1<TInput, TOutput>
    | ((
        tx: Transaction<TSchema, TContext>,
        options: {
          args: TOutput;
          ctx: TContext;
        },
      ) => Promise<void>),
  mutatorFn?: (
    tx: Transaction<TSchema, TContext>,
    options: {
      args: TOutput;
      ctx: TContext;
    },
  ) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TInput, TOutput> {
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualMutatorFn: (
    tx: Transaction<TSchema, TContext>,
    options: {
      args: TOutput;
      ctx: TContext;
    },
  ) => Promise<void>;

  if (typeof validatorOrMutatorFn === 'function') {
    validator = undefined;
    actualMutatorFn = validatorOrMutatorFn;
  } else {
    validator = validatorOrMutatorFn;
    actualMutatorFn = must(mutatorFn);
  }

  const f = actualMutatorFn as MutatorDefinition<
    TSchema,
    TContext,
    TInput,
    TOutput
  >;

  f[defineMutatorTag] = true;
  f.validator = validator;
  return f;
}

/**
 * Creates a type-safe mutator definition function parameterized by context type.
 */
export function defineMutatorWithContextType<TContext>(): {
  <TSchema extends Schema, TArgs extends ReadonlyJSONValue | undefined>(
    mutatorFn: (
      tx: Transaction<TSchema, TContext>,
      options: {
        args: TArgs;
        ctx: TContext;
      },
    ) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TArgs, TArgs>;

  <
    TSchema extends Schema,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    mutatorFn: (
      tx: Transaction<TSchema, TContext>,
      options: {
        args: TOutput;
        ctx: TContext;
      },
    ) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TInput, TOutput>;
} {
  return defineMutator as {
    <TSchema extends Schema, TArgs extends ReadonlyJSONValue | undefined>(
      mutatorFn: (
        tx: Transaction<TSchema, TContext>,
        options: {
          args: TArgs;
          ctx: TContext;
        },
      ) => Promise<void>,
    ): MutatorDefinition<TSchema, TContext, TArgs, TArgs>;

    <
      TSchema extends Schema,
      TInput extends ReadonlyJSONValue | undefined,
      TOutput extends ReadonlyJSONValue | undefined,
    >(
      validator: StandardSchemaV1<TInput, TOutput>,
      mutatorFn: (
        tx: Transaction<TSchema, TContext>,
        options: {
          args: TOutput;
          ctx: TContext;
        },
      ) => Promise<void>,
    ): MutatorDefinition<TSchema, TContext, TInput, TOutput>;
  };
}

/**
 * Base type for mutator registries used in constraints.
 * This is a simpler recursive type that doesn't require the full definition shape.
 */
export type MutatorRegistryBase<S extends Schema, TContext> = {
  readonly [key: string]:
    // oxlint-disable-next-line @typescript-eslint/no-explicit-any
    | ((args: any) => MutatorThunk<S, TContext>)
    | MutatorRegistryBase<S, TContext>;
};

/**
 * The type returned by defineMutators - same tree shape but each leaf
 * is now a function that takes args and returns a thunk (tx, ctx) => Promise<void>.
 */
export type MutatorRegistry<
  S extends Schema,
  TContext,
  T extends MutatorDefinitions<S, TContext>,
> = {
  readonly [K in keyof T]: T[K] extends MutatorDefinition<
    S,
    TContext,
    infer TInput,
    // oxlint-disable-next-line no-explicit-any
    any
  >
    ? TInput extends undefined
      ? (args?: TInput) => MutatorThunk<S, TContext>
      : (args: TInput) => MutatorThunk<S, TContext>
    : T[K] extends MutatorDefinitions<S, TContext>
      ? MutatorRegistry<S, TContext, T[K]>
      : never;
};

/**
 * A mutator thunk - takes transaction and context, executes the mutation.
 */
export type MutatorThunk<S extends Schema, TContext> = ((
  tx: Transaction<S, TContext>,
  ctx: TContext,
) => Promise<void>) & {
  readonly mutatorName: string;
  readonly mutatorArgs: ReadonlyJSONValue[];
};

/**
 * Wraps a tree of mutator definitions, stamping each with its fully-qualified
 * name derived from the object keys.
 *
 * @example
 * ```typescript
 * const mutators = defineMutators<typeof schema, AuthData>()({
 *   issue: {
 *     create: defineMutator(z.object({...}), (tx, {args, ctx}) => {
 *       await tx.mutate.issue.insert({...});
 *     }),
 *   },
 * });
 * ```
 */
export function defineMutators<
  S extends Schema,
  TContext,
>(): <T extends MutatorDefinitions<S, TContext>>(
  defs: T,
) => MutatorRegistry<S, TContext, T> {
  return <T extends MutatorDefinitions<S, TContext>>(
    defs: T,
  ): MutatorRegistry<S, TContext, T> => defineMutatorsImpl(defs, '');
}

function defineMutatorsImpl<
  S extends Schema,
  TContext,
  T extends MutatorDefinitions<S, TContext>,
>(defs: T, prefix: string): MutatorRegistry<S, TContext, T> {
  const result: Record<string, unknown> = {};

  for (const [key, value] of Object.entries(defs)) {
    const name = prefix ? `${prefix}.${key}` : key;

    if (isMutatorDefinition(value)) {
      result[key] = wrapMutatorDefinition(name, value);
    } else {
      // oxlint-disable-next-line no-explicit-any
      result[key] = defineMutatorsImpl<S, TContext, any>(
        value as MutatorDefinitions<S, TContext>,
        name,
      );
    }
  }

  return result as MutatorRegistry<S, TContext, T>;
}

/**
 * Wraps a mutator definition, creating a function that takes args and
 * returns a thunk needing transaction and context.
 */
function wrapMutatorDefinition<TArgs, TContext, S extends Schema>(
  mutatorName: string,
  // oxlint-disable-next-line no-explicit-any
  f: MutatorDefinition<S, TContext, any, any>,
): (args: TArgs) => MutatorThunk<S, TContext> {
  const {validator} = f;
  const validate = validator
    ? (args: TArgs) =>
        validateInput<TArgs, TArgs>(mutatorName, args, validator, 'mutator')
    : (args: TArgs) => args;

  return (args?: TArgs) => {
    const validatedArgs = validate(args as TArgs);

    const thunk = Object.assign(
      async (tx: Transaction<S, TContext>, ctx: TContext): Promise<void> => {
        await f(tx, {
          args: validatedArgs,
          ctx,
        });
      },
      {
        mutatorName,
        mutatorArgs:
          args === undefined ? [] : [args as unknown as ReadonlyJSONValue],
      },
    );

    return thunk as MutatorThunk<S, TContext>;
  };
}
