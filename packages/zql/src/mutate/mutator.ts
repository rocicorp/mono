import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {AnyTransaction, Transaction} from './custom.ts';

// ----------------------------------------------------------------------------
// defineMutator
// ----------------------------------------------------------------------------

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
export function defineMutatorWithType<
  TSchema extends Schema,
>(): TypedDefineMutator<TSchema, unknown, unknown>;

// Overload 2: Schema and Context
export function defineMutatorWithType<
  TSchema extends Schema,
  TContext,
>(): TypedDefineMutator<TSchema, TContext, unknown>;

// Overload 3: Schema, Context, and WrappedTransaction
export function defineMutatorWithType<
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
>(): TypedDefineMutator<TSchema, TContext, TWrappedTransaction>;

// Implementation
export function defineMutatorWithType() {
  return defineMutator;
}

/**
 * The return type of defineMutatorWithType. A function matching the
 * defineMutator overloads but with Schema, Context, and WrappedTransaction
 * pre-bound.
 */
type TypedDefineMutator<
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = {
  // Without validator
  <TArgs extends ReadonlyJSONValue | undefined>(
    mutator: (options: {
      args: TArgs;
      ctx: TContext;
      tx: Transaction<TSchema, TWrappedTransaction>;
    }) => Promise<void>,
  ): MutatorDefinition<TSchema, TContext, TArgs, TArgs, TWrappedTransaction>;

  // With validator
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

// ----------------------------------------------------------------------------
// Mutator and MutationRequest types
// ----------------------------------------------------------------------------

/**
 * A callable wrapper around a MutatorDefinition, created by `defineMutators()`.
 *
 * Accessed like `mutators.foo.bar`, and called to create a MutationRequest:
 * `mutators.foo.bar(42)` returns a `MutationRequest`.
 *
 * The `fn` property is used for execution and takes raw JSON args (for rebase
 * and server wire format cases) that are validated internally.
 *
 * @template TSchema - The schema type
 * @template TContext - The context type available during mutation execution
 * @template TArgsInput - The argument type accepted by the callable (before validation)
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type Mutator<
  TSchema extends Schema,
  TContext,
  TArgsInput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction = unknown,
> = ((
  args: TArgsInput,
) => MutationRequest<TSchema, TContext, TArgsInput, TWrappedTransaction>) & {
  readonly mutatorName: string;
  /**
   * Execute the mutation. Args are ReadonlyJSONValue because this is called
   * during rebase (from stored JSON) and on the server (from wire format).
   * Validation happens internally before the recipe function runs.
   *
   * The tx parameter uses AnyTransaction to avoid contravariance issues when
   * calling from generic code. The implementation casts to the specific type.
   */
  readonly fn: (options: {
    args: TArgsInput;
    ctx: TContext;
    tx: AnyTransaction;
  }) => Promise<void>;
};

// oxlint-disable-next-line no-explicit-any
export type AnyMutator = Mutator<Schema, any, any, any>;

/**
 * The result of calling a Mutator with arguments.
 *
 * Created by `mutators.foo.bar(42)`, executed by `zero.mutate(mr)` on the client
 * or `mr.mutator.fn({tx, ctx, args: mr.args})` on the server.
 *
 * @template TSchema - The schema type
 * @template TContext - The context type available during mutation execution
 * @template TArgsInput - The argument type (before validation, sent to server)
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type MutationRequest<
  TSchema extends Schema,
  TContext,
  TArgsInput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
> = {
  readonly mutator: Mutator<TSchema, TContext, TArgsInput, TWrappedTransaction>;
  readonly args: TArgsInput;
};

/**
 * Checks if a value is a Mutator (the result of processing a MutatorDefinition
 * through defineMutators).
 */
export function isMutator(value: unknown): value is AnyMutator {
  return (
    typeof value === 'function' &&
    // oxlint-disable-next-line no-explicit-any
    typeof (value as any).mutatorName === 'string' &&
    // oxlint-disable-next-line no-explicit-any
    typeof (value as any).fn === 'function'
  );
}
