import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DefaultContext,
  DefaultSchema,
  DefaultWrappedTransaction,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {AnyTransaction, Transaction} from './custom.ts';

// ----------------------------------------------------------------------------
// defineMutator
// ----------------------------------------------------------------------------

const defineMutatorTag = Symbol();

export function isMutatorDefinition<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction = unknown,
>(
  f: unknown,
): f is MutatorDefinition<
  TInput,
  TOutput,
  TSchema,
  TContext,
  TWrappedTransaction
> {
  // oxlint-disable-next-line no-explicit-any
  return typeof f === 'function' && !!(f as any)[defineMutatorTag];
}

export type MutatorDefinition<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
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
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  mutator: (options: {
    args: TOutput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TInput, TOutput, TSchema, TContext, TWrappedTransaction>;

// Overload 2: Call without validator
export function defineMutator<
  TArgs extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  mutator: (options: {
    args: TArgs;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TArgs, TArgs, TSchema, TContext, TWrappedTransaction>;

// Implementation
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
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
): MutatorDefinition<TInput, TOutput, TSchema, TContext, TWrappedTransaction> {
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
    TInput,
    TOutput,
    TSchema,
    TContext,
    TWrappedTransaction
  >;
  f[defineMutatorTag] = true;
  f.validator = validator;
  return f;
}

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
 * @template TArgsInput - The argument type accepted by the callable (before validation)
 * @template TSchema - The schema type
 * @template TContext - The context type available during mutation execution
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type Mutator<
  TArgsInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = MutatorCallable<TSchema, TContext, TArgsInput, TWrappedTransaction> & {
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

// Helper type for the callable part of Mutator
// When TArgsInput is undefined, the function is callable with 0 args
// When TArgsInput includes undefined (optional), args is optional
// Otherwise, args is required
type MutatorCallable<
  TSchema extends Schema,
  TContext,
  TArgsInput extends ReadonlyJSONValue | undefined,
  TWrappedTransaction,
> = [TArgsInput] extends [undefined]
  ? () => MutationRequest<TArgsInput, TSchema, TContext, TWrappedTransaction>
  : undefined extends TArgsInput
    ? (
        args?: TArgsInput,
      ) => MutationRequest<TArgsInput, TSchema, TContext, TWrappedTransaction>
    : (
        args: TArgsInput,
      ) => MutationRequest<TArgsInput, TSchema, TContext, TWrappedTransaction>;

// oxlint-disable-next-line no-explicit-any
export type AnyMutator = Mutator<any, Schema, any, any>;

/**
 * The result of calling a Mutator with arguments.
 *
 * Created by `mutators.foo.bar(42)`, executed by `zero.mutate(mr)` on the client
 * or `mr.mutator.fn({tx, ctx, args: mr.args})` on the server.
 *
 * @template TArgsInput - The argument type (before validation, sent to server)
 * @template TSchema - The schema type
 * @template TContext - The context type available during mutation execution
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type MutationRequest<
  TArgsInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = {
  readonly mutator: Mutator<TArgsInput, TSchema, TContext, TWrappedTransaction>;
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
