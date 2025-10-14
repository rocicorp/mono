/**
 * Internal APIs for framework bindings (React, Vue, Svelte, etc.).
 * These APIs are not intended for end users.
 *
 * @module
 * @internal
 */

import type {CustomMutatorDefs} from '../../../zero-client/src/client/custom.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {ViewFactory} from '../../../zql/src/ivm/view.ts';
import type {QueryDelegate} from '../../../zql/src/query/query-delegate.ts';
import {materializeImpl} from '../../../zql/src/query/query-impl.ts';
import type {QueryInternals} from '../../../zql/src/query/query-internals.ts';
import type {
  HumanReadable,
  MaterializeOptions,
} from '../../../zql/src/query/query.ts';
import type {TypedView} from '../../../zql/src/query/typed-view.ts';
import type {Zero} from './zero.ts';

/**
 * Internal WeakMap to store QueryDelegate for each Zero instance.
 * This is populated by Zero's constructor and allows bindings to access
 * the delegate without exposing it as a public API.
 */
export const zeroDelegates = new WeakMap<
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  Zero<any, any, any>,
  // oxlint-disable-next-line @typescript-eslint/no-explicit-any
  QueryDelegate<any>
>();

/**
 * Materialize an already-resolved QueryInternals without redundant withContext() calls.
 *
 * Bindings should:
 * 1. Call `getQueryInternals(query, zero.context)` to resolve named queries
 * 2. Extract `format` and call `hash()` from the resolved QueryInternals
 * 3. Call this function with the resolved QueryInternals
 *
 * This avoids double-resolution: once for getting format/hash in the binding,
 * and again inside materialize(). The materializeImpl function detects that
 * the query is already resolved (via queryInternalsTag) and skips withContext().
 *
 * @internal This API is for bindings only, not end users.
 */
export function materializeQueryInternals<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  MD extends CustomMutatorDefs | undefined,
  TContext,
>(
  zero: Zero<TSchema, MD, TContext>,
  queryInternals: QueryInternals<TSchema, TTable, TReturn, TContext>,
  options?: MaterializeOptions,
): TypedView<HumanReadable<TReturn>>;

export function materializeQueryInternals<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  MD extends CustomMutatorDefs | undefined,
  TContext,
  T,
>(
  zero: Zero<TSchema, MD, TContext>,
  queryInternals: QueryInternals<TSchema, TTable, TReturn, TContext>,
  factory: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
  options?: MaterializeOptions,
): T;

export function materializeQueryInternals<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
  MD extends CustomMutatorDefs | undefined,
  TContext,
  T,
>(
  zero: Zero<TSchema, MD, TContext>,
  queryInternals: QueryInternals<TSchema, TTable, TReturn, TContext>,
  factoryOrOptions?:
    | ViewFactory<TSchema, TTable, TReturn, TContext, T>
    | MaterializeOptions,
  maybeOptions?: MaterializeOptions,
) {
  let factory;
  let options;

  if (typeof factoryOrOptions === 'function') {
    factory = factoryOrOptions;
    options = maybeOptions;
  } else {
    factory = undefined;
    options = factoryOrOptions;
  }

  // Get the QueryDelegate from the WeakMap that was populated by Zero's constructor.
  const delegate = zeroDelegates.get(zero);
  if (!delegate) {
    throw new Error('Zero instance not registered with bindings');
  }
  return materializeImpl(queryInternals, delegate, factory, options);
}
