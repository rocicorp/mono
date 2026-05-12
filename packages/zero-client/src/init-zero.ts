import type {
  BaseDefaultContext,
  BaseDefaultSchema,
  DefaultSchema,
} from '../../zero-types/src/default-types.ts';
import {
  defineMutatorsWithType,
  type TypedDefineMutators,
} from '../../zql/src/mutate/mutator-registry.ts';
import {
  defineMutatorWithType,
  type TypedDefineMutator,
} from '../../zql/src/mutate/mutator.ts';
import {
  defineQueriesWithType,
  defineQueryWithType,
  type TypedDefineQueries,
  type TypedDefineQuery,
} from '../../zql/src/query/query-registry.ts';
import type {ZeroOptions} from './client/options.ts';
import {Zero} from './client/zero.ts';

export type TypedZero<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
> = new (options: ZeroOptions<S, undefined, C>) => Zero<S, undefined, C>;

export type InitZeroResult<
  S extends BaseDefaultSchema,
  C extends BaseDefaultContext,
  TWrappedTransaction,
> = {
  readonly Zero: TypedZero<S, C>;
  readonly defineMutator: TypedDefineMutator<S, C, TWrappedTransaction>;
  readonly defineMutators: TypedDefineMutators<S>;
  readonly defineQuery: TypedDefineQuery<S, C>;
  readonly defineQueries: TypedDefineQueries<S>;
};

/**
 * Returns typed Zero helpers without relying on module augmentation.
 */
export function initZero<
  S extends BaseDefaultSchema = DefaultSchema,
  C extends BaseDefaultContext = unknown,
  TWrappedTransaction = unknown,
>(): InitZeroResult<S, C, TWrappedTransaction> {
  return {
    Zero: Zero as TypedZero<S, C>,
    defineMutator: defineMutatorWithType<S, C, TWrappedTransaction>(),
    defineMutators: defineMutatorsWithType<S>(),
    defineQuery: defineQueryWithType<S, C>(),
    defineQueries: defineQueriesWithType<S>(),
  };
}
