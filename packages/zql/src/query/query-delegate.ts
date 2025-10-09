import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {ErroredQuery} from '../../../zero-protocol/src/custom-queries.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import type {Format, ViewFactory} from '../ivm/view.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {QueryInternals} from './query-internals.ts';
import type {
  HumanReadable,
  MaterializeOptions,
  PreloadOptions,
  Query,
  RunOptions,
} from './query.ts';
import type {TTL} from './ttl.ts';

export type CommitListener = () => void;
export type GotCallback = (
  got: boolean,
  error?: ErroredQuery | undefined,
) => void;

export interface NewQueryDelegate {
  newQuery<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    schema: TSchema,
    table: TTable,
    ast: AST,
    format: Format,
  ): Query<TSchema, TTable, TReturn>;
}

/**
 * Interface for delegates that support materializing, running, and preloading queries.
 * This interface contains the methods needed to execute queries and manage their lifecycle.
 */
export interface MaterializableQueryDelegate
  extends BuilderDelegate,
    MetricsDelegate {
  addServerQuery(
    ast: AST,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void;
  addCustomQuery(
    ast: AST,
    customQueryID: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void;
  updateServerQuery(ast: AST, ttl: TTL): void;
  updateCustomQuery(customQueryID: CustomQueryID, ttl: TTL): void;
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

  /**
   * Materialize a query into a custom view using a provided factory function.
   */
  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
    T,
  >(
    query: QueryInternals<TSchema, TTable, TReturn, TContext>,
    factory: ViewFactory<TSchema, TTable, TReturn, TContext, T>,
    options?: MaterializeOptions,
  ): T;

  /**
   * Materialize a query into a view that automatically updates when data changes.
   * When no factory is provided, returns a TypedView with the query results.
   */
  materialize<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: QueryInternals<TSchema, TTable, TReturn, TContext>,
    options?: MaterializeOptions,
  ): import('./typed-view.ts').TypedView<HumanReadable<TReturn>>;

  /**
   * Run a query and return the results as a Promise.
   */
  run<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: QueryInternals<TSchema, TTable, TReturn, TContext>,
    options?: RunOptions,
  ): Promise<HumanReadable<TReturn>>;

  /**
   * Preload a query's data without materializing a view.
   */
  preload<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
    TContext,
  >(
    query: QueryInternals<TSchema, TTable, TReturn, TContext>,
    options?: PreloadOptions,
  ): {
    cleanup: () => void;
    complete: Promise<void>;
  };
}

export interface QueryDelegate extends MaterializableQueryDelegate {
  flushQueryChanges(): void;
}
