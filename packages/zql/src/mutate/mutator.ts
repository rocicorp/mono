import type {StandardSchemaV1} from '@standard-schema/spec';
import type {ReadonlyJSONValue} from '../../../shared/src/json.ts';
import {must} from '../../../shared/src/must.ts';
import type {
  DefaultContext,
  DefaultSchema,
  DefaultWrappedTransaction,
  IsUnknown,
} from '../../../zero-types/src/default-types.ts';
import {isCodec, type Codec} from '../../../zero-types/src/schema-value.ts';
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
  // The type the generated mutator callable accepts. Equals TInput for plain /
  // validator mutators; equals the decoded TOutput for codec mutators.
  TCallArgs = TInput,
> = 'MutatorDefinition' & {
  readonly $input: TInput;
  readonly $output: TOutput;
  readonly $callArgs: TCallArgs;
  readonly $context: TContext;
  readonly $wrappedTransaction: TWrappedTransaction;
};

export type MutatorDefinition<
  TInput extends ReadonlyJSONValue | undefined,
  // TOutput (the decoded args type) is intentionally unconstrained: with a codec
  // it may be a non-JSON app type (e.g. `Date`). TInput (the wire type) stays
  // JSON-bound.
  TOutput,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
  TCallArgs = TInput,
> = {
  readonly 'fn': MutatorDefinitionFunction<TOutput, TContext, AnyTransaction>;
  readonly 'validator': StandardSchemaV1<TInput, TOutput> | undefined;
  readonly 'codec': Codec<TInput, TOutput> | undefined;
  readonly '~': MutatorDefinitionTypes<
    TInput,
    TOutput,
    TContext,
    TWrappedTransaction,
    TCallArgs
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

// Overload for codec. The codec encodes the decoded args (`TOutput`, e.g. a
// `Date`) to its JSON wire form (`TInput`) before the mutation is queued/sent,
// and decodes back before the recipe runs. Strictly an alternative to a
// validator.
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined,
  TOutput,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  codec: Codec<TInput, TOutput>,
  mutator: MutatorDefinitionFunction<
    TOutput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >,
): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction, TOutput>;

// Implementation
export function defineMutator<
  TInput extends ReadonlyJSONValue | undefined = undefined,
  TOutput = TInput,
  TSchema extends Schema = DefaultSchema,
  TContext = DefaultContext,
  TWrappedTransaction = DefaultWrappedTransaction,
>(
  validatorCodecOrMutator:
    | StandardSchemaV1<TInput, TOutput>
    | Codec<TInput, TOutput>
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
): MutatorDefinition<
  TInput,
  TOutput,
  TContext,
  TWrappedTransaction,
  TInput | TOutput
> {
  let validator: StandardSchemaV1<TInput, TOutput> | undefined;
  let codec: Codec<TInput, TOutput> | undefined;
  let actualMutator: MutatorDefinitionFunction<
    TOutput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >;

  if (isCodec(validatorCodecOrMutator)) {
    // defineMutator(codec, mutator)
    codec = validatorCodecOrMutator as Codec<TInput, TOutput>;
    actualMutator = must(mutator);
  } else if ('~standard' in validatorCodecOrMutator) {
    // defineMutator(validator, mutator)
    validator = validatorCodecOrMutator;
    actualMutator = must(mutator);
  } else {
    // defineMutator(mutator) - no validator or codec
    actualMutator = validatorCodecOrMutator as MutatorDefinitionFunction<
      TOutput,
      TContext,
      Transaction<TSchema, TWrappedTransaction>
    >;
  }

  const mutatorDefinition: MutatorDefinition<
    TInput,
    TOutput,
    TContext,
    TWrappedTransaction,
    TInput | TOutput
  > = {
    'fn': actualMutator as MutatorDefinitionFunction<
      TOutput,
      TContext,
      AnyTransaction
    >,
    'validator': validator,
    'codec': codec,
    '~': 'MutatorDefinition' as unknown as MutatorDefinitionTypes<
      TInput,
      TOutput,
      TContext,
      TWrappedTransaction,
      TInput | TOutput
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

  // With codec
  <TInput extends ReadonlyJSONValue | undefined, TOutput>(
    codec: Codec<TInput, TOutput>,
    mutator: MutatorDefinitionFunction<
      TOutput,
      TContext,
      Transaction<TSchema, TWrappedTransaction>
    >,
  ): MutatorDefinition<TInput, TOutput, TContext, TWrappedTransaction, TOutput>;
};

export type MutatorDefinitionFunction<
  // Unconstrained: the decoded args may be a non-JSON app type when a codec is
  // used.
  TOutput,
  TContext,
  TTransaction,
> = (options: {
  args: TOutput;
  ctx: TContext;
  tx: TTransaction;
}) => Promise<void>;

export type MutatorExecutionFunction<
  TOutput extends ReadonlyJSONValue | undefined,
  TContext,
  TTransaction,
> = (
  options: MutatorExecutionOptions<TOutput, TContext, TTransaction>,
) => Promise<void>;

type MutatorExecutionOptions<
  TOutput extends ReadonlyJSONValue | undefined,
  TContext,
  TTransaction,
> = undefined extends TOutput
  ? IsUnknown<TContext> extends true
    ? {args?: TOutput | undefined; tx: TTransaction; ctx?: TContext | undefined}
    : {args?: TOutput | undefined; tx: TTransaction; ctx: TContext}
  : IsUnknown<TContext> extends true
    ? {args: TOutput; tx: TTransaction; ctx?: TContext | undefined}
    : {args: TOutput; tx: TTransaction; ctx: TContext};

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
  // The type the callable accepts. Equals TInput for plain / validator
  // mutators; the decoded app type for codec mutators. The stored/wire args
  // (in MutateRequest) and the internally-invoked `fn` stay on TInput.
  TCallArgs = TInput,
> = {
  readonly 'mutatorName': string;
  /**
   * Execute the mutation. Args are ReadonlyJSONValue because this is called
   * during rebase (from stored JSON) and on the server (from wire format).
   * Validation / codec decoding happens internally before the recipe runs.
   */
  readonly 'fn': MutatorExecutionFunction<
    TInput,
    TContext,
    Transaction<TSchema, TWrappedTransaction>
  >;
  readonly '~': MutatorTypes<TInput, TSchema, TContext, TWrappedTransaction>;
} & MutatorCallable<TCallArgs, TInput, TSchema, TContext, TWrappedTransaction>;

// Helper type for the callable part of Mutator.
// `TCallArgs` is the type the user passes; `TInput` is the wire/stored type of
// the resulting MutateRequest. When there is no codec they are the same.
// When TCallArgs is undefined, the function is callable with 0 args;
// when it includes undefined (optional), args is optional; otherwise required.
type MutatorCallable<
  TCallArgs,
  TInput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema,
  TContext,
  TWrappedTransaction,
> = [TCallArgs] extends [undefined]
  ? () => MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>
  : undefined extends TCallArgs
    ? {
        (): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
        (
          args?: TCallArgs,
        ): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
      }
    : {
        (
          args: TCallArgs,
        ): MutateRequest<TInput, TSchema, TContext, TWrappedTransaction>;
      };

// oxlint-disable-next-line no-explicit-any
export type AnyMutator = Mutator<any, any, any, any, any>;

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
