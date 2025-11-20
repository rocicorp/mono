import type {CustomMutatorDefs} from '../../../zero-client/src/client/custom.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Format, ViewFactory} from '../../../zql/src/ivm/view.ts';
import type {QueryDefinitions} from '../../../zql/src/query/query-definitions.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {
  AnyQuery,
  HumanReadable,
  MaterializeOptions,
  QueryReturn,
} from '../../../zql/src/query/query.ts';
import type {TypedView} from '../../../zql/src/query/typed-view.ts';
import type {Zero} from './zero.ts';

/**
 * Internal WeakMap to store QueryDelegate for each Zero instance.
 * This is populated by Zero's constructor and allows bindings to access
 * the delegate without exposing it as a public API.
 */
const zeroDelegates = new WeakMap<
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  Zero<any, any, any, any>,
  QueryDelegate
>();

export function registerZeroDelegate<
  TSchema extends Schema,
  MD extends CustomMutatorDefs | undefined,
  TContext,
  QD extends QueryDefinitions<TSchema, TContext> | undefined,
>(zero: Zero<TSchema, MD, TContext, QD>, delegate: QueryDelegate): void {
  zeroDelegates.set(zero, delegate);
}

function mustGetDelegate<
  TSchema extends Schema,
  MD extends CustomMutatorDefs | undefined,
  TContext,
  QD extends QueryDefinitions<TSchema, TContext> | undefined,
>(zero: Zero<TSchema, MD, TContext, QD>): QueryDelegate {
  const delegate = zeroDelegates.get(zero);
  if (!delegate) {
    throw new Error('Zero instance not registered with bindings');
  }
  return delegate;
}

/**
 * Bindings interface for Zero instances, providing methods to materialize queries
 * and extract query metadata.
 *
 * @internal This API is for bindings only, not end users.
 */
export interface BindingsForZero {
  /**
   * Materialize a query into a reactive view without a custom factory.
   * Returns a TypedView that automatically updates when underlying data changes.
   */
  materialize<TQuery extends AnyQuery>(
    query: TQuery,
    factory?: undefined,
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<QueryReturn<TQuery>>>;

  /**
   * Materialize a query into a reactive view using a custom factory.
   * The factory can transform the view into a framework-specific reactive object.
   */
  materialize<TQuery extends AnyQuery, T>(
    query: TQuery,
    factory: ViewFactory<TQuery, T>,
    options?: MaterializeOptions,
  ): T;

  /**
   * Compute the hash of a query for caching and deduplication purposes.
   */
  hash(query: AnyQuery): string;

  /**
   * Get the format/schema of a query's result set.
   */
  format(query: AnyQuery): Format;
}

/**
 * Create a bindings object for a Zero instance.
 * This provides low-level access to query materialization and metadata extraction.
 *
 * @internal This API is for bindings only, not end users.
 */
// oxlint-disable-next-line no-explicit-any
export function bindingsForZero<TZero extends Zero<any, any, any, any>>(
  zero: TZero,
): BindingsForZero {
  const delegate = mustGetDelegate(zero);

  return {
    materialize<TQuery extends AnyQuery, T>(
      query: TQuery,
      factory?: ViewFactory<TQuery, T>,
      options?: MaterializeOptions,
    ): T {
      return delegate.materialize(query, factory, options);
    },

    hash(query: AnyQuery): string {
      const queryInternals = asQueryInternals(query);
      return queryInternals.hash();
    },

    format(query: AnyQuery): Format {
      const queryInternals = asQueryInternals(query);
      return queryInternals.format;
    },
  };
}
