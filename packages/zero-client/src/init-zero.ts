import {
  defineMutatorWithType,
  type TypedDefineMutator,
} from '../../zql/src/mutate/mutator.ts';
import {
  defineMutatorsWithType,
  type TypedDefineMutators,
} from '../../zql/src/mutate/mutator-registry.ts';
import {
  defineQueryWithType,
  defineQueriesWithType,
  type TypedDefineQuery,
  type TypedDefineQueries,
} from '../../zql/src/query/query-registry.ts';
import type {Schema} from '../../zero-types/src/schema.ts';

/**
 * The result of calling initZero. Contains typed utilities for building
 * queries and defining mutators.
 */
export type InitZeroResult<S extends Schema, C, TWrappedTransaction> = {
  /**
   * Define a single mutator with schema, context, and wrapped transaction
   * types pre-bound.
   * @example
   * ```ts
   * const {defineMutator} = initZero<typeof schema, AuthData>({schema});
   * const createIssue = defineMutator(async ({tx, ctx, args}) => {
   *   // ctx is AuthData
   *   // tx is Transaction<typeof schema>
   * });
   * ```
   */
  readonly defineMutator: TypedDefineMutator<S, C, TWrappedTransaction>;

  /**
   * Define a mutator registry with schema type pre-bound.
   * @example
   * ```ts
   * const {defineMutators, defineMutator} = initZero({schema});
   * const mutators = defineMutators({
   *   issue: {
   *     create: defineMutator(async ({tx, args}) => { ... }),
   *   },
   * });
   * ```
   */
  readonly defineMutators: TypedDefineMutators<S>;

  /**
   * Define a single query with schema and context types pre-bound.
   * @example
   * ```ts
   * const {defineQuery, builder} = initZero<typeof schema, AuthData>({schema});
   * const myIssues = defineQuery(({ctx}) =>
   *   builder.issue.where('creatorID', ctx.userID)
   * );
   * ```
   */
  readonly defineQuery: TypedDefineQuery<S, C>;

  /**
   * Define a query registry with schema type pre-bound.
   * @example
   * ```ts
   * const {defineQueries, defineQuery, builder} = initZero({schema});
   * const queries = defineQueries({
   *   allIssues: defineQuery(() => builder.issue.orderBy('created', 'desc')),
   * });
   * ```
   */
  readonly defineQueries: TypedDefineQueries<S>;
};

/**
 * Initialize Zero with a schema and return typed utilities.
 *
 * This function provides an alternative to TypeScript module augmentation
 * for typing Zero. Instead of using `declare module '@rocicorp/zero'`,
 * you can use `initZero` to get typed utilities.
 *
 * @example
 * ```ts
 * // shared/zero.ts
 * import {initZero} from '@rocicorp/zero';
 * import type {Schema} from './schema';
 *
 * type AuthData = {userID: string; role: 'admin' | 'user'};
 *
 * export const {
 *   defineMutator,
 *   defineMutators,
 *   defineQuery,
 *   defineQueries,
 * } = initZero<Schema, AuthData | undefined>();
 * ```
 *
 * @returns Typed utilities for building queries and defining mutators
 */
export function initZero<
  S extends Schema = Schema,
  C = unknown,
  TWrappedTransaction = unknown,
>(): InitZeroResult<S, C, TWrappedTransaction> {
  return {
    defineMutator: defineMutatorWithType<S, C, TWrappedTransaction>(),
    defineMutators: defineMutatorsWithType<S>(),
    defineQuery: defineQueryWithType<S, C>(),
    defineQueries: defineQueriesWithType<S>(),
  };
}
