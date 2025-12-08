import type {StandardSchemaV1} from '@standard-schema/spec';
import type {Expand} from '../../../shared/src/expand.ts';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DefaultContext,
  DefaultSchema,
  DefaultWrappedTransaction,
} from '../../../zero-types/src/default-types.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {AnyTransaction, Transaction} from './custom.ts';

// oxlint-disable no-explicit-any

// ----------------------------------------------------------------------------
// defineMutator
// ----------------------------------------------------------------------------

export type MutatorDefinitionTypes<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TContext,
  TWrappedTransaction,
> = 'MutatorDefinition' & {
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $context: TContext;
  readonly $wrappedTransaction: TWrappedTransaction;
};

export type MutateRequestTypes<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = 'MutateRequest' & {
  readonly $input: TInput;
  readonly $schema: TSchema;
  readonly $context: TContext;
  readonly $wrappedTransaction: TWrappedTransaction;
};

export function isMutatorDefinition(f: unknown): f is AnyMutatorDefinition {
  return typeof f === 'function' && (f as any)['~'] === 'MutatorDefinition';
}

export type MutatorDefinition<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = ((options: {
  args: TOutput;
  ctx: TContext;
  tx: AnyTransaction;
}) => Promise<void>) & {
  validator: StandardSchemaV1<TInput, TOutput> | undefined;

  /**
   * Type-only phantom property to surface mutator types in a covariant position.
   */
  ['~']: Expand<
    MutatorDefinitionTypes<TInput, TOutput, TContext, TWrappedTransaction>
  >;
};

// oxlint-disable-next-line no-explicit-any
export type AnyMutatorDefinition = MutatorDefinition<any, any, any, any>;

// Overload 1: Call with validator
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = undefined,
  TOutput extends ReadonlyJSONValue | undefined = TInput,
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
): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction>;

// Overload 2: Call without validator
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  mutator: (options: {
    args: TInput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>,
): MutatorDefinition<TInput, TInput, TContext, TWrappedTransaction>;

// Implementation
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = undefined,
  TOutput extends ReadonlyJSONValue | undefined = TInput,
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
): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction> {
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
    TContext,
    TWrappedTransaction
  >;
  f['~'] = 'MutatorDefinition' as unknown as MutatorDefinitionTypes<
    TInput,
    TOutput,
    TContext,
    TWrappedTransaction
  >;

  f.validator = validator;
  return f;
}

// intentionally not using DefaultSchema, DefaultContext, or DefaultWrappedTransaction
export function defineMutatorWithType<
  TSchema extends Schema,
  TContext = unknown,
  TWrappedTransaction = unknown,
>(): TypedDefineMutator<TSchema, TContext, TWrappedTransaction> {
  return defineMutator;
}

/**
 * The return type of defineMutatorWithType. A function matching the
 * defineMutator overloads but with Schema, Context, and WrappedTransaction
 * pre-bound.
 *
 * This is used as a workaround to using DefaultTypes (e.g. when using
 * multiple Zero instances).
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
  ): MutatorDefinition<TArgs, TArgs, TContext, TWrappedTransaction>;

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
  ): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction>;
};

// ----------------------------------------------------------------------------
// Mutator and MutateRequest types
// ----------------------------------------------------------------------------

export type MutatorTypes<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = 'Mutator' & {
  readonly $input: TInput;
  readonly $schema: TSchema;
  readonly $context: TContext;
  readonly $wrappedTransaction: TWrappedTransaction;
};

/**
 * A callable wrapper around a MutatorDefinition, created by `defineMutators()`.
 *
 * Accessed like `mutators.foo.bar`, and called to create a MutateRequest:
 * `mutators.foo.bar(42)` returns a `MutateRequest`.
 *
 * The `fn` property is used for execution and takes raw JSON args (for rebase
 * and server wire format cases) that are validated internally.
 *
 * @template TInput - The argument type accepted by the callable (before validation)
 * @template TContext - The context type available during mutation execution
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type Mutator<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = MutatorCallable<TInput, TSchema, TContext, TWrappedTransaction> & {
  readonly mutatorName: string;
  /**
   * Execute the mutation. Args are ReadonlyJSONValue because this is called
   * during rebase (from stored JSON) and on the server (from wire format).
   * Validation happens internally before the recipe function runs.
   */
  readonly fn: (options: {
    args: TInput;
    ctx: TContext;
    tx: Transaction<TSchema, TWrappedTransaction>;
  }) => Promise<void>;

  /**
   * Type-only phantom property to surface mutator types in a covariant position.
   */
  ['~']: Expand<MutatorTypes<TInput, TSchema, TContext, TWrappedTransaction>>;
};

// Helper type for the callable part of Mutator
// When TInput is undefined, the function is callable with 0 args
// When TInput includes undefined (optional), args is optional
// Otherwise, args is required
type MutatorCallable<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = [TInput] extends [undefined]
  ? () => MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>
  : undefined extends TInput
    ? (
        args?: TInput,
      ) => MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>
    : (
        args: TInput,
      ) => MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;

// oxlint-disable-next-line no-explicit-any
export type AnyMutator = Mutator<any, any, any, any>;

/**
 * The result of calling a Mutator with arguments.
 *
 * Created by `mutators.foo.bar(42)`, executed by `zero.mutate(mr)` on the client
 * or `mr.mutator.fn({tx, ctx, args: mr.args})` on the server.
 *
 * @template TInput - The argument type (before validation, sent to server)
 * @template TContext - The context type available during mutation execution
 * @template TWrappedTransaction - The wrapped transaction type
 */
export type MutateRequest<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = {
  readonly 'mutator': Mutator<TInput, TSchema, TContext, TWrappedTransaction>;
  readonly 'args': TInput;
  readonly '~': Expand<
    MutateRequestTypes<TInput, TSchema, TContext, TWrappedTransaction>
  >;
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
