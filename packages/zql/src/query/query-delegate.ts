import type {AST} from '../../../zero-protocol/src/ast.ts';
import type {Schema} from '../../../zero-schema/src/builder/schema-builder.ts';
import type {BuilderDelegate} from '../builder/builder.ts';
import type {Format} from '../ivm/view.ts';
import type {MetricsDelegate} from './metrics-delegate.ts';
import type {CustomQueryID} from './named.ts';
import type {Query, RunOptions} from './query.ts';
import type {TTL} from './ttl.ts';

export type CommitListener = () => void;
export type GotCallback = (got: boolean) => void;

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

export interface QueryDelegate extends BuilderDelegate, MetricsDelegate {
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
}
