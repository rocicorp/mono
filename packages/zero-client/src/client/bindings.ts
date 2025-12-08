import type {Schema} from '../../../zero-types/src/schema.ts';
import type {Format, ViewFactory} from '../../../zql/src/ivm/view.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  Query,
} from '../../../zql/src/query/query-builder.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {asQueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {TypedView} from '../../../zql/src/query/typed-view.ts';
import type {CustomMutatorDefs} from './custom.ts';
import type {Zero} from './zero.ts';

/**
 * Internal WeakMap to store QueryDelegate for each Zero instance.
 * This is populated by Zero's constructor and allows bindings to access
 * the delegate without exposing it as a public API.
 */
const zeroDelegates = new WeakMap<
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  Zero<any, any, any>,
  QueryDelegate
>();

export function registerZeroDelegate<
  TSchema extends Schema,
  MD extends CustomMutatorDefs | undefined,
  TContext,
>(zero: Zero<TSchema, MD, TContext>, delegate: QueryDelegate): void {
  zeroDelegates.set(zero, delegate);
}

function mustGetDelegate<
  TSchema extends Schema,
  MD extends CustomMutatorDefs | undefined,
  TContext,
>(zero: Zero<TSchema, MD, TContext>): QueryDelegate {
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
export interface BindingsForZero<TSchema extends Schema> {
  /**
   * Materialize a query into a reactive view without a custom factory.
   * Returns a TypedView that automatically updates when underlying data changes.
   */
  materialize<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
    factory?: undefined,
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<TReturn>>;

  /**
   * Materialize a query into a reactive view using a custom factory.
   * The factory can transform the view into a framework-specific reactive object.
   */
  materialize<TTable extends keyof TSchema['tables'] & string, TReturn, T>(
    query: Query<TTable, TSchema, TReturn>,
    factory: ViewFactory<TTable, TSchema, TReturn, T>,
    options?: MaterializeOptions,
  ): T;

  /**
   * Compute the hash of a query for caching and deduplication purposes.
   */
  hash<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
  ): string;

  /**
   * Get the format/schema of a query's result set.
   */
  format<TTable extends keyof TSchema['tables'] & string, TReturn>(
    query: Query<TTable, TSchema, TReturn>,
  ): Format;
}

/**
 * Create a bindings object for a Zero instance.
 * This provides low-level access to query materialization and metadata extraction.
 *
 * @internal This API is for bindings only, not end users.
 */
export function bindingsForZero<
  TSchema extends Schema,
  MD extends CustomMutatorDefs | undefined,
  TContext,
>(zero: Zero<TSchema, MD, TContext>): BindingsForZero<TSchema> {
  const delegate = mustGetDelegate(zero);

  return {
    materialize<TTable extends keyof TSchema['tables'] & string, TReturn, T>(
      query: Query<TTable, TSchema, TReturn>,
      factory?: ViewFactory<TTable, TSchema, TReturn, T>,
      options?: MaterializeOptions,
    ) {
      return delegate.materialize(query, factory, options);
    },

    hash<TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TTable, TSchema, TReturn>,
    ): string {
      const queryInternals = asQueryInternals(query);
      return queryInternals.hash();
    },

    format<TTable extends keyof TSchema['tables'] & string, TReturn>(
      query: Query<TTable, TSchema, TReturn>,
    ): Format {
      const queryInternals = asQueryInternals(query);
      return queryInternals.format;
    },
  };
}
