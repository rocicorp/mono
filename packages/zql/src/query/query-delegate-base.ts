import {resolver} from '@rocicorp/resolver';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {hashOfNameAndArgs} from '../../../zero-protocol/src/query-hash.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../builder/builder.ts';
import {ArrayView} from '../ivm/array-view.ts';
import type {FilterInput} from '../ivm/filter-operators.ts';
import {MemoryStorage} from '../ivm/memory-storage.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {MetricMap} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from './query-builder.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from './query-delegate.ts';
import {asQueryInternals, type QueryInternals} from './query-internals.ts';
import {DEFAULT_PRELOAD_TTL_MS, DEFAULT_TTL_MS, type TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

/**
 * Base class that provides default implementations for common QueryDelegate methods.
 * Subclasses can override specific methods as needed.
 */
export abstract class QueryDelegateBase implements QueryDelegate {
  /**
   * Default implementation that just calls applyViewUpdates synchronously.
   * Override if you need custom batching behavior (e.g., SolidJS).
   */
  batchViewUpdates<T>(applyViewUpdates: () => T): T {
    return applyViewUpdates();
  }

  /**
   * Default implementation returns MemoryStorage.
   * Override if you need custom storage.
   */
  createStorage(): Storage {
    return new MemoryStorage();
  }

  /**
   * Default implementation calls materializeImpl.
   * Override if you need custom materialization behavior.
   */
  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: Query<TTable, TSchema, TReturn>,
    factory?: undefined,
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<TReturn>>;

  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
    T,
  >(
    query: Query<TTable, TSchema, TReturn>,
    factory?: ViewFactory<TTable, TSchema, TReturn, T>,
    options?: MaterializeOptions,
  ): T;

  /**
   * Materialize a query into a custom view using a provided factory function.
   */
  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
    T,
  >(
    query: Query<TTable, TSchema, TReturn>,
    factory?: ViewFactory<TTable, TSchema, TReturn, T>,
    options?: MaterializeOptions,
  ): T;

  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
    T,
  >(
    query: Query<TTable, TSchema, TReturn>,
    factory?: ViewFactory<TTable, TSchema, TReturn, T>,
    options?: MaterializeOptions,
  ): T {
    return materializeImpl(query, this, factory, options);
  }

  /**
   * Default implementation calls runImpl.
   * Override if you need custom query execution (e.g., TestPGQueryDelegate).
   */
  run<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: Query<TTable, TSchema, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>> {
    return runImpl(query, this, options);
  }

  /**
   * Default implementation calls preloadImpl.
   * Override if you need custom preload behavior.
   */
  preload<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: Query<TTable, TSchema, TReturn>,
    options?: PreloadOptions,
  ): {
    cleanup: () => void;
    complete: Promise<void>;
  } {
    return preloadImpl(query, this, options);
  }

  /**
   * Default no-op implementation for decorateSourceInput.
   * Override if you need to wrap or instrument source inputs.
   */
  decorateSourceInput(input: SourceInput, _queryID: string): Input {
    return input;
  }

  /**
   * Default no-op implementation for decorateInput.
   * Override if you need to wrap or instrument inputs.
   */
  decorateInput(input: Input, _name: string): Input {
    return input;
  }

  /**
   * Default no-op implementation for decorateFilterInput.
   * Override if you need to wrap or instrument filter inputs.
   */
  decorateFilterInput(input: FilterInput, _name: string): FilterInput {
    return input;
  }

  /**
   * Default no-op implementation for addEdge.
   * Override if you need to track graph edges (e.g., visualization).
   */
  addEdge(_source: InputBase, _dest: InputBase): void {
    // No-op
  }

  /**
   * Default no-op implementation for addMetric.
   * Override if you need to collect metrics.
   */
  addMetric<K extends keyof MetricMap>(
    _metric: K,
    _value: number,
    ..._args: MetricMap[K]
  ): void {
    // No-op
  }

  /**
   * Default no-op implementation.
   * Override if you need to track server queries (e.g., ZeroContext, test delegates).
   */
  addServerQuery(_ast: AST, _ttl: TTL, _gotCallback?: GotCallback): () => void {
    return () => {};
  }

  /**
   * Default no-op implementation.
   * Override if you need to track custom queries (e.g., ZeroContext, test delegates).
   */
  addCustomQuery(
    _ast: AST,
    _customQueryID: CustomQueryID,
    _ttl: TTL,
    _gotCallback?: GotCallback,
  ): () => void {
    return () => {};
  }

  /**
   * Default no-op implementation.
   * Override if you need to handle query updates.
   */
  updateServerQuery(_ast: AST, _ttl: TTL): void {
    // No-op
  }

  /**
   * Default no-op implementation.
   * Override if you need to handle custom query updates.
   */
  updateCustomQuery(_customQueryID: CustomQueryID, _ttl: TTL): void {
    // No-op
  }

  /**
   * Default no-op implementation.
   * Override if you need to flush query changes.
   */
  flushQueryChanges(): void {
    // No-op
  }

  /**
   * Called when a transaction commits. Override to add custom behavior.
   * Default implementation returns a no-op cleanup function.
   */
  onTransactionCommit(_cb: CommitListener): () => void {
    return () => {};
  }

  /**
   * Validates run options. Override to add custom validation.
   * Default implementation is a no-op.
   */
  assertValidRunOptions(_options?: RunOptions): void {
    // No-op
  }

  abstract readonly defaultQueryComplete: boolean;

  // BuilderDelegate methods - must be implemented
  abstract getSource(name: string): Source | undefined;
}

// oxlint-disable-next-line require-await
export async function runImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(
  query: Query<TTable, TSchema, TReturn>,
  delegate: QueryDelegate,
  options?: RunOptions,
): Promise<HumanReadable<TReturn>> {
  delegate.assertValidRunOptions(options);
  const v: TypedView<HumanReadable<TReturn>> = materializeImpl(
    query,
    delegate,
    undefined,
    {
      ttl: options?.ttl,
    },
  );
  if (options?.type === 'complete') {
    return new Promise(resolve => {
      v.addListener((data, type) => {
        if (type === 'complete') {
          v.destroy();
          resolve(data as HumanReadable<TReturn>);
        } else if (type === 'error') {
          v.destroy();
          resolve(Promise.reject(data));
        }
      });
    });
  }

  options?.type satisfies 'unknown' | undefined;

  const ret = v.data;
  v.destroy();
  return ret;
}

export function preloadImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
>(
  query: Query<TTable, TSchema, TReturn>,
  delegate: QueryDelegate,
  options?: PreloadOptions,
): {
  cleanup: () => void;
  complete: Promise<void>;
} {
  const qi = asQueryInternals(query);
  const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
  const {resolve, promise: complete} = resolver<void>();
  const {customQueryID, ast} = qi;
  if (customQueryID) {
    const cleanup = delegate.addCustomQuery(ast, customQueryID, ttl, got => {
      if (got) {
        resolve();
      }
    });
    return {
      cleanup,
      complete,
    };
  }

  const cleanup = delegate.addServerQuery(ast, ttl, got => {
    if (got) {
      resolve();
    }
  });
  return {
    cleanup,
    complete,
  };
}

export function materializeImpl<
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
  T,
>(
  query: Query<TTable, TSchema, TReturn>,
  delegate: QueryDelegate,
  factory: ViewFactory<
    TTable,
    TSchema,
    TReturn,
    T
    // oxlint-disable-next-line no-explicit-any
  > = arrayViewFactory as any,
  options?: MaterializeOptions,
): T {
  let ttl: TTL = options?.ttl ?? DEFAULT_TTL_MS;

  const qi = asQueryInternals(query);
  const {ast, format, customQueryID} = qi;
  const queryHash = qi.hash();

  const queryID = customQueryID
    ? hashOfNameAndArgs(customQueryID.name, customQueryID.args)
    : queryHash;
  const queryCompleteResolver = resolver<true>();
  let queryComplete: boolean | ErroredQuery = delegate.defaultQueryComplete;
  const updateTTL = customQueryID
    ? (newTTL: TTL) => delegate.updateCustomQuery(customQueryID, newTTL)
    : (newTTL: TTL) => delegate.updateServerQuery(ast, newTTL);

  const gotCallback: GotCallback = (got, error) => {
    if (error) {
      queryCompleteResolver.reject(error);
      queryComplete = error;
      return;
    }

    if (got) {
      delegate.addMetric(
        'query-materialization-end-to-end',
        performance.now() - t0,
        queryID,
        ast,
      );
      queryComplete = true;
      queryCompleteResolver.resolve(true);
    }
  };

  let removeCommitObserver: (() => void) | undefined;
  const onDestroy = () => {
    input.destroy();
    removeCommitObserver?.();
    removeAddedQuery();
  };

  const t0 = performance.now();

  const removeAddedQuery = customQueryID
    ? delegate.addCustomQuery(ast, customQueryID, ttl, gotCallback)
    : delegate.addServerQuery(ast, ttl, gotCallback);

  const input = buildPipeline(ast, delegate, queryID);

  const view = delegate.batchViewUpdates(() =>
    (factory ?? arrayViewFactory)(
      query,
      input,
      format,
      onDestroy,
      cb => {
        removeCommitObserver = delegate.onTransactionCommit(cb);
      },
      queryComplete || queryCompleteResolver.promise,
      updateTTL,
    ),
  );

  delegate.addMetric(
    'query-materialization-client',
    performance.now() - t0,
    queryID,
  );

  return view as T;
}

function arrayViewFactory<
  TTable extends string,
  TSchema extends Schema,
  TReturn,
>(
  _query: QueryInternals<TTable, TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | ErroredQuery | Promise<true>,
  updateTTL: (ttl: TTL) => void,
): TypedView<HumanReadable<TReturn>> {
  const v = new ArrayView<HumanReadable<TReturn>>(
    input,
    format,
    queryComplete,
    updateTTL,
  );
  v.onDestroy = onDestroy;
  onTransactionCommit(() => {
    v.flush();
  });
  return v;
}
