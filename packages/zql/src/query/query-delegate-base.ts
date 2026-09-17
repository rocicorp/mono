import {resolver} from '@rocicorp/resolver';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import {
  hashOfAST,
  hashOfNameAndArgs,
} from '../../../zero-protocol/src/query-hash.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import {buildPipeline} from '../builder/builder.ts';
import {ArrayView} from '../ivm/array-view.ts';
import {DeferredInput} from '../ivm/deferred-input.ts';
import type {FilterInput} from '../ivm/filter-operators.ts';
import {MemoryStorage} from '../ivm/memory-storage.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {MetricMap} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {
  AttachPipeline,
  CommitListener,
  GotCallback,
  QueryDelegate,
} from './query-delegate.ts';
import {asQueryInternals, type QueryInternals} from './query-internals.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from './query.ts';
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
    cached: Promise<void>;
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

  /**
   * Default: pipelines can always be built immediately. Override (together
   * with `onPipelinesReady`) to defer pipeline construction.
   */
  get pipelinesReady(): boolean {
    return true;
  }

  onPipelinesReady(_attach: AttachPipeline): () => void {
    throw new Error(
      'onPipelinesReady called on a delegate whose pipelines are always ready',
    );
  }

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
  if (options?.type === 'complete' || options?.type === 'cached') {
    // 'cached' is satisfied by a result the server confirmed on a previous
    // connection, or by this connection confirming it, whichever comes first.
    const acceptCached = options.type === 'cached';
    return new Promise(resolve => {
      v.addListener((data, type) => {
        if (type === 'complete' || (acceptCached && type === 'cached')) {
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

  // A snapshot of what the view holds right now. While the delegate's
  // pipelines are not ready the view is deferred and this is empty; 'unknown'
  // has never waited for the local data, so it does not wait for that either.

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
  cached: Promise<void>;
} {
  const qi = asQueryInternals(query);
  const ttl = options?.ttl ?? DEFAULT_PRELOAD_TTL_MS;
  const completeResolver = resolver<void>();
  const cachedResolver = resolver<void>();
  const {promise: complete} = completeResolver;
  const {promise: cached} = cachedResolver;
  // A caller may ignore either promise; a query error must not surface as an
  // unhandled rejection through the one nobody awaits.
  void complete.catch(() => {});
  void cached.catch(() => {});
  const {customQueryID, ast} = qi;
  const gotCallback: GotCallback = (got, error) => {
    if (error) {
      // The query cannot be satisfied; neither waiter should hang.
      cachedResolver.reject(error);
      completeResolver.reject(error);
      return;
    }
    // Only a server confirmation on this connection resolves `complete`;
    // `cached` is also satisfied by one from a previous connection.
    if (got === true) {
      cachedResolver.resolve();
      completeResolver.resolve();
    } else if (got === 'cached') {
      cachedResolver.resolve();
    }
  };
  const cleanup = customQueryID
    ? delegate.addCustomQuery(ast, customQueryID, ttl, gotCallback)
    : delegate.addServerQuery(ast, ttl, gotCallback);
  if (delegate.defaultQueryComplete) {
    // A delegate whose results are complete from the start (a server-side
    // one) has no got callback to drive the waiters.
    cachedResolver.resolve();
    completeResolver.resolve();
  }
  return {
    cleanup,
    complete,
    cached,
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

  const queryID = customQueryID
    ? hashOfNameAndArgs(customQueryID.name, customQueryID.args)
    : hashOfAST(qi.ast);
  const queryCompleteResolver = resolver<true>();
  // When the delegate cannot build pipelines yet, the view starts over an
  // empty DeferredInput and is hydrated later. A view must not report
  // `complete` until it has actually been hydrated, so completion is gated on
  // both the server's "got" and the pipeline being attached.
  const deferPipeline = !delegate.pipelinesReady;
  let attached = !deferPipeline;
  // The last got report: `true` once the server confirmed the query on this
  // connection, `'cached'` while the store holds a previous connection's
  // confirmed result, `false` otherwise.
  let got: boolean | 'cached' = delegate.defaultQueryComplete;
  let queryComplete: boolean | ErroredQuery = attached && got === true;
  const updateTTL = customQueryID
    ? (newTTL: TTL) => delegate.updateCustomQuery(customQueryID, newTTL)
    : (newTTL: TTL) => delegate.updateServerQuery(ast, newTTL);

  // Completion, and the end-to-end metric, require both the server's "got"
  // and the pipeline being attached: until then the view is still empty.
  const maybeResolveComplete = () => {
    if (attached && got === true && queryComplete !== true) {
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

  // The view, seen as its optional hooks. Only views that implement
  // `markCached`/`unmarkCached` (e.g. ArrayView) surface 'cached'; for any
  // other factory the optional calls are no-ops. The registration path can
  // report 'cached' synchronously, before the view below exists, so this
  // stays undefined until then and the mark is applied afterwards.
  let viewHooks: OptionalViewHooks | undefined;

  // Like 'complete', 'cached' is a claim about the rows the view holds, so it
  // waits for the view to exist and its pipeline to be attached. Once the
  // server has confirmed the query on this connection, 'complete' supersedes
  // it and the mark is skipped.
  const maybeMarkCached = () => {
    // Only while the query is still incomplete: 'complete' supersedes the
    // mark, and an error that arrived before attach must not be preceded by
    // a 'cached' notification.
    if (attached && got === 'cached' && queryComplete === false) {
      viewHooks?.markCached?.();
    }
  };

  let destroyed = false;
  const gotCallback: GotCallback = (value, error) => {
    if (destroyed) {
      // The delegate may keep this callback registered for a while after
      // destroy (removals are deferred while mutations are pending); a dead
      // view must not be driven, nor its listeners fired.
      return;
    }
    if (error) {
      queryCompleteResolver.reject(error);
      queryComplete = error;
      return;
    }

    got = value;
    if (got === 'cached') {
      maybeMarkCached();
    } else if (got === false) {
      // The got key was deleted (eviction) before the server confirmed the
      // query on this connection.
      viewHooks?.unmarkCached?.();
    } else {
      maybeResolveComplete();
    }
  };

  let removeCommitObserver: (() => void) | undefined;
  let removePendingAttach: (() => void) | undefined;
  const onDestroy = () => {
    destroyed = true;
    removePendingAttach?.();
    removePendingAttach = undefined;
    input.destroy();
    removeCommitObserver?.();
    removeAddedQuery();
  };

  const t0 = performance.now();

  const removeAddedQuery = customQueryID
    ? delegate.addCustomQuery(ast, customQueryID, ttl, gotCallback)
    : delegate.addServerQuery(ast, ttl, gotCallback);

  const deferred = deferPipeline
    ? newDeferredInput(ast, delegate, queryID)
    : undefined;
  // No type annotation here: annotating this as `Input` widens it enough that
  // the view type loses the pipeline's concrete schema, and `Zero.run` then
  // stops rejecting a query built from a schema with legacy queries enabled
  // (custom.test.ts covers that rejection).
  const input = deferred ?? buildPipeline(ast, delegate, queryID);

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

  if (!deferred) {
    delegate.addMetric(
      'query-materialization-client',
      performance.now() - t0,
      queryID,
    );
  } else {
    // Registered after the factory ran so a throwing factory leaves nothing
    // behind.
    removePendingAttach = delegate.onPipelinesReady(() => {
      removePendingAttach = undefined;
      if (deferred.destroyed) {
        return () => {};
      }
      const t1 = performance.now();
      try {
        viewHooks?.holdData?.();
        deferred.attach();
      } catch (e) {
        // Surface the failure on this view and let the delegate decide how
        // to log it. Other pending pipelines are unaffected.
        const error: ErroredQuery = {
          error: 'app',
          id: queryID,
          name: customQueryID?.name ?? '',
          message: e instanceof Error ? e.message : String(e),
        };
        queryComplete = error;
        queryCompleteResolver.reject(error);
        throw e;
      }
      const hydrateMs = performance.now() - t1;
      // The view holds its rows now but must not say so yet: the delegate
      // releases every view hydrated in the same batch together.
      return () => {
        if (destroyed) {
          return;
        }
        attached = true;
        delegate.addMetric('query-materialization-client', hydrateMs, queryID);
        maybeResolveComplete();
        maybeMarkCached();
      };
    });
  }

  viewHooks = view as OptionalViewHooks;
  maybeMarkCached();

  return view as T;
}

/**
 * Creates the placeholder input for a query whose pipeline cannot be built yet.
 *
 * `Input.getSchema()` is unconditional, so the placeholder needs the schema up
 * front. It is captured by building a pipeline and immediately destroying it:
 * building only connects to the sources, while the indexes and rows -- the
 * expensive part this deferral exists to postpone -- are read on the first
 * fetch. Measured at ~5us against ~500us for a single 100 row fetch.
 *
 * Building here also means a query that cannot be built at all (an unknown
 * table, `not(exists())` on the client) still throws from `materialize()`,
 * as it did before hydration was deferred.
 */
function newDeferredInput(
  ast: AST,
  delegate: QueryDelegate,
  queryID: string,
): DeferredInput {
  const build = () => buildPipeline(ast, delegate, queryID);
  const probe = build();
  try {
    return new DeferredInput(probe.getSchema(), build);
  } finally {
    probe.destroy();
  }
}

/**
 * Optional methods `materialize` calls on a view when it has them (ArrayView
 * does). See {@link ViewFactory} for what a custom view needs to know.
 */
type OptionalViewHooks = {
  // Surface the 'cached' result type. Views without these never show it.
  markCached?: (() => void) | undefined;
  unmarkCached?: (() => void) | undefined;
  // Keep showing the current snapshot until the next flush, so a deferred
  // view does not show rows ahead of its release.
  holdData?: (() => void) | undefined;
};

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
  onTransactionCommit(() => v.flush());
  return v;
}
