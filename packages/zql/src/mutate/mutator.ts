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

export type MutatorDefinition<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = {
  readonly 'fn': MutatorDefinitionFunction<TOutput, TContext, AnyTransaction>;
  readonly 'validator': StandardSchemaV1<TInput, TOutput> | undefined;
  readonly '~': MutatorDefinitionTypes<
    TInput,
    TOutput,
    TContext,
    TWrappedTransaction
  >;
};

// oxlint-disable-next-line no-explicit-any
export type AnyMutatorDefinition = MutatorDefinition<any, any, any, any>;

export function isMutatorDefinition(f: unknown): f is AnyMutatorDefinition {
  return (
    typeof f === 'object' &&
    f !== null &&
    (f as {['~']?: unknown})['~'] === 'MutatorDefinition'
  );
}

// Overload for no validator
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  mutator: MutatorDefinitionFunction<
    TInput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >,
): MutatorDefinition<TInput, TInput, TContext, TWrappedTransaction>;

// Overload for validator
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = undefined,
  TOutput extends ReadonlyJSONValue | undefined = TInput,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  validator: StandardSchemaV1<TInput, TOutput>,
  mutator: MutatorDefinitionFunction<
    TOutput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >,
): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction>;

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
    | MutatorDefinitionFunction<
        TOutput,
        TContext,
        Transaction<TSchema, TWrappedTransaction>
      >,
  mutator?: MutatorDefinitionFunction<
    TOutput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >,
): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction> {
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let actualMutator: MutatorDefinitionFunction<
    TOutput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >;

  if (
    typeof validatorOrMutator === 'function' &&
    !('~standard' in validatorOrMutator)
  ) {
    // defineMutator(mutator) - no validator
    validator = undefined;
    actualMutator = validatorOrMutator;
  } else {
    // defineMutator(validator, mutator)
    // Cast needed because TS can't narrow based on '~standard' in check
    validator = validatorOrMutator as StandardSchemaV1<TInput, TOutput>;
    actualMutator = must(mutator);
  }

  const mutatorDefinition: MutatorDefinition<
    TInput,
    TOutput,
    TContext,
    TWrappedTransaction
  > = {
    'fn': actualMutator as MutatorDefinitionFunction<
      TOutput,
      TContext,
      AnyTransaction
    >,
    'validator': validator,
    '~': 'MutatorDefinition' as unknown as MutatorDefinitionTypes<
      TInput,
      TOutput,
      TContext,
      TWrappedTransaction
    >,
  };
  return mutatorDefinition;
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
    mutator: MutatorDefinitionFunction<
      TArgs,
      TContext,
      Transaction<TSchema, TWrappedTransaction>
    >,
  ): MutatorDefinition<TArgs, TArgs, TContext, TWrappedTransaction>;

  // With validator
  <
    TInput extends ReadonlyJSONValue | undefined,
    TOutput extends ReadonlyJSONValue | undefined,
  >(
    validator: StandardSchemaV1<TInput, TOutput>,
    mutator: MutatorDefinitionFunction<
      TOutput,
      TContext,
      Transaction<TSchema, TWrappedTransaction>
    >,
  ): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction>;
};

export type MutatorDefinitionFunction<
  TOutput extends ReadonlyJSONValue | undefined,
  TContext,
  TTransaction,
> = (options: {
  args: TOutput;
  ctx: TContext;
  tx: TTransaction;
}) => Promise<void>;

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
 */
export type Mutator<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = {
  readonly 'mutatorName': string;
  /**
   * Execute the mutation. Args are ReadonlyJSONValue because this is called
   * during rebase (from stored JSON) and on the server (from wire format).
   * Validation happens internally before the recipe function runs.
   */
  readonly 'fn': MutatorDefinitionFunction<
    TInput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >;
  readonly '~': MutatorTypes<TInput, TSchema, TContext, TWrappedTransaction>;
} & MutatorCallable<TInput, TSchema, TContext, TWrappedTransaction>;

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
    ? {
        (): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
        (
          args?: TInput,
        ): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
      }
    : {
        (
          args: TInput,
        ): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
      };

// oxlint-disable-next-line no-explicit-any
export type AnyMutator = Mutator<any, any, any, any>;

/**
 * Checks if a value is a Mutator (the result of processing a MutatorDefinition
 * through defineMutators).
 */
export function isMutator<S extends Schema>(
  value: unknown,
  // oxlint-disable-next-line no-explicit-any
): value is Mutator<any, S, any, any> {
  return (
    typeof value === 'function' &&
    typeof (value as {mutatorName?: unknown}).mutatorName === 'string' &&
    typeof (value as {fn?: unknown}).fn === 'function'
  );
}

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

/**
 * The result of calling a Mutator with arguments.
 *
 * Created by `mutators.foo.bar(42)`, executed by `zero.mutate(mr)` on the client
 * or `mr.mutator.fn({tx, ctx, args: mr.args})` on the server.
 */
export type MutateRequest<
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
> = {
  readonly 'mutator': Mutator<TInput, TSchema, TContext, TWrappedTransaction>;
  readonly 'args': TInput;
  readonly '~': MutateRequestTypes<
    TInput,
    TSchema,
    TContext,
    TWrappedTransaction
  >;
};
