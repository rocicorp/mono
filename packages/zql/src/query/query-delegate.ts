import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Schema} from '../../../zero-types/src/schema.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  QueryBuilder,
  RunOptions,
} from './query-builder.ts';
import type {TTL} from './ttl.ts';
import type {TypedView} from './typed-view.ts';

export type CommitListener = () => void;
export type GotCallback = (got: boolean, error?: ErroredQuery) => void;

export interface NewQueryDelegate {
  newQuery<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
  ): QueryBuilder<TTable, TSchema, TReturn>;
}

/**
 * Interface for delegates that support materializing, running, and preloading queries.
 * This interface contains the methods needed to execute queries and manage their lifecycle.
 */
export interface QueryDelegate extends BuilderDelegate, MetricsDelegate {
  addServerQuery(ast: AST, ttl: TTL, gotCallback?: GotCallback): () => void;

  addCustomQuery(
    ast: AST,
    customQueryID: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback,
  ): () => void;

  updateServerQuery(ast: AST, ttl: TTL): void;

  updateCustomQuery(customQueryID: CustomQueryID, ttl: TTL): void;

  flushQueryChanges(): void;

  onTransactionCommit(cb: CommitListener): () => void;

  /**
   * batchViewUpdates is used to allow the view to batch multiple view updates together.
   * Normally, `applyViewUpdates` is called directly but for some cases, SolidJS for example,
   * the updates are wrapped in a batch to avoid multiple re-renders.
   */
  batchViewUpdates<T>(applyViewUpdates: () => T): T;

  /**
   * Asserts that the `RunOptions` provided to the `run` method are supported in
   * this context. For example, in a custom mutator, the `{type: 'complete'}`
   * option is not supported and this will throw.
   */
  assertValidRunOptions(options?: RunOptions): void;

  /**
   * Client queries start off as false (`unknown`) and are set to true when the
   * server sends the gotQueries message.
   *
   * For things like ZQLite the default is true (aka `complete`) because the
   * data is always available.
   */
  readonly defaultQueryComplete: boolean;

  /** Using the default view factory creates a TypedView */
  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: QueryBuilder<TTable, TSchema, TReturn>,
    factory?: undefined,
    options?: MaterializeOptions,
  ): TypedView<HumanReadable<TReturn>>;

  materialize<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
    T,
  >(
    query: QueryBuilder<TTable, TSchema, TReturn>,
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
    query: QueryBuilder<TTable, TSchema, TReturn>,
    factory?: ViewFactory<TTable, TSchema, TReturn, T>,
    options?: MaterializeOptions,
  ): T;

  /**
   * Run a query and return the results as a Promise.
   */
  run<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: QueryBuilder<TTable, TSchema, TReturn>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>>;

  /**
   * Preload a query's data without materializing a view.
   */
  preload<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    query: QueryBuilder<TTable, TSchema, TReturn>,
    options?: PreloadOptions,
  ): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}
