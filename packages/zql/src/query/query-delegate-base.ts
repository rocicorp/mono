import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {FilterInput} from '../ivm/filter-operators.ts';
import {MemoryStorage} from '../ivm/memory-storage.ts';
import type {Input, InputBase, Storage} from '../ivm/operator.ts';
import type {Source, SourceInput} from '../ivm/source.ts';
import type {ViewFactory} from '../ivm/view.ts';
import type {MetricMap} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from './query-delegate.ts';
import {materializeImpl, preloadImpl, runImpl} from './query-impl.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from './query.ts';
import type {TTL} from './ttl.ts';
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
