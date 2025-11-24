import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import {must} from '../../shared/src/must.ts';
import type {Transaction} from '../../zql/src/mutate/custom.ts';
import type {Schema} from './schema.ts';

const defineMutatorTag = Symbol();

export function isMutatorDefinition<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction = unknown,
>(
  f: unknown,
): f is MutatorDefinition<
  TSchema,
  TContext,
  TInput,
  TOutput,
  TWrappedTransaction
> {
  // oxlint-disable-next-line no-explicit-any
  return typeof f === 'function' && !!(f as any)[defineMutatorTag];
}

export type MutatorDefinition<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
> = ((options: {
  args: TOutput;
  ctx: TContext;
  tx: Transaction<TSchema, TWrappedTransaction>;
}) => Promise<void>) & {
  [defineMutatorTag]: true;
  validator: StandardSchemaV1<TInput, TOutput> | undefined;
};

// Overload 1: Call with validator
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  mutator: (options: {
    args: TOutput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TInput, TOutput, TWrappedTransaction>;

// Overload 2: Call without validator
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TArgs extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
>(
  mutator: (options: {
    args: TArgs;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TArgs, TArgs, TWrappedTransaction>;

// Implementation
export function defineMutator<
  TSchema extends Schema,
  TContext,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
>(
  validatorOrMutator:
    | StandardSchemaV1<TInput, TOutput>
    | ((options: {
        args: TOutput;
        ctx: TContext;
        tx: Transaction<TSchema, TWrappedTransaction>;
      }) => Promise<void>),
  mutator?: (options: {
    args: TOutput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TSchema, TContext, TInput, TOutput, TWrappedTransaction> {
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualMutator: (options: {
    args: TOutput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>;

  if (typeof validatorOrMutator === 'function') {
    // defineMutator(mutator) - no validator
    validator = undefined;
    actualMutator = validatorOrMutator;
  } else {
    // defineMutator(validator, mutator)
    validator = validatorOrMutator;
    actualMutator = must(mutator);
  }

  const f = actualMutator as MutatorDefinition<
    TSchema,
    TContext,
    TInput,
    TOutput,
    TWrappedTransaction
  >;
  f[defineMutatorTag] = true;
  f.validator = validator;
  return f;
}

// Overload 1: Just Schema
export function defineMutatorWithType<TSchema extends Schema>(): {
  <TContext, TArgs extends ReadonlyJSONValue | undefined, TWrappedTransaction>(
    mutator: (options: {
      args: TArgs;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TArgs, TArgs, TWrappedTransaction>;

  <
    TContext,
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TWrappedTransaction,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    mutator: (options: {
      args: TOutput;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TInput, TOutput, TWrappedTransaction>;
};

// Overload 2: Schema and Context
export function defineMutatorWithType<TSchema extends Schema, TContext>(): {
  <TArgs extends ReadonlyJSONValue | undefined, TWrappedTransaction>(
    mutator: (options: {
      args: TArgs;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TArgs, TArgs, TWrappedTransaction>;

  <
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
    TWrappedTransaction,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    mutator: (options: {
      args: TOutput;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TInput, TOutput, TWrappedTransaction>;
};

// Overload 3: Schema, Context, and WrappedTransaction
export function defineMutatorWithType<
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
>(): {
  <TArgs extends ReadonlyJSONValue | undefined>(
    mutator: (options: {
      args: TArgs;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TArgs, TArgs, TWrappedTransaction>;

  <
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    mutator: (options: {
      args: TOutput;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TInput, TOutput, TWrappedTransaction>;
};

// Implementation
export function defineMutatorWithType() {
  return defineMutator;
}
