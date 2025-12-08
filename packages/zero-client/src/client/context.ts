import type {LogContext} from '@rocicorp/logger';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {DebugDelegate} from '../../../zql/src/builder/debug-delegate.ts';
import type {Input} from '../../../zql/src/ivm/operator.ts';
import type {Source, SourceInput} from '../../../zql/src/ivm/source.ts';
import {MeasurePushOperator} from '../../../zql/src/query/measure-push-operator.ts';
import type {MetricsDelegate} from '../../../zql/src/query/metrics-delegate.ts';
import type {RunOptions} from '../../../zql/src/query/query-builder.ts';
import {QueryDelegateBase} from '../../../zql/src/query/query-delegate-base.ts';
import type {CommitListener} from '../../../zql/src/query/query-delegate.ts';
import {type IVMSourceBranch} from './ivm-branch.ts';
import type {QueryManager} from './query-manager.ts';

export type AddQuery = QueryManager['addLegacy'];
export type AddCustomQuery = QueryManager['addCustom'];

export type UpdateQuery = QueryManager['updateLegacy'];
export type UpdateCustomQuery = QueryManager['updateCustom'];
export type FlushQueryChanges = QueryManager['flushBatch'];

/**
 * ZeroContext glues together zql and Replicache. It listens to changes in
 * Replicache data and pushes them into IVM and on tells the server about new
 * queries.
 */
export class ZeroContext extends QueryDelegateBase {
  readonly #lc: LogContext;

  // It is a bummer to have to maintain separate MemorySources here and copy the
  // data in from the Replicache db. But we want the data to be accessible via
  // pipelines *synchronously* and the core Replicache infra is all async. So
  // that needs to be fixed.
  readonly #mainSources: IVMSourceBranch;

  readonly addServerQuery: AddQuery;
  readonly addCustomQuery: AddCustomQuery;
  readonly updateServerQuery: UpdateQuery;
  readonly updateCustomQuery: UpdateCustomQuery;
  readonly flushQueryChanges: () => void;
  readonly #batchViewUpdates: (applyViewUpdates: () => void) => void;
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly assertValidRunOptions: (options?: RunOptions) => void;

  /**
   * Client-side queries start out as "unknown" and are then updated to
   * "complete" once the server has sent back the query result.
   */
  readonly defaultQueryComplete = false;

  readonly addMetric: MetricsDelegate['addMetric'];

  constructor(
    lc: LogContext,
    mainSources: IVMSourceBranch,
    addQuery: AddQuery,
    addCustomQuery: AddCustomQuery,
    updateQuery: UpdateQuery,
    updateCustomQuery: UpdateCustomQuery,
    flushQueryChanges: () => void,
    batchViewUpdates: (applyViewUpdates: () => void) => void,
    addMetric: MetricsDelegate['addMetric'],
    assertValidRunOptions: (options?: RunOptions) => void,
  ) {
    super();
    this.#lc = lc;
    this.#mainSources = mainSources;
    this.addServerQuery = addQuery;
    this.updateServerQuery = updateQuery;
    this.updateCustomQuery = updateCustomQuery;
    this.#batchViewUpdates = batchViewUpdates;
    this.assertValidRunOptions = assertValidRunOptions;
    this.addCustomQuery = addCustomQuery;
    this.flushQueryChanges = flushQueryChanges;
    this.addMetric = addMetric;
  }

  applyFiltersAnyway?: boolean | undefined;

  debug?: DebugDelegate | undefined;

  getSource(name: string): Source | undefined {
    return this.#mainSources.getSource(name);
  }

  mapAst(ast: AST): AST {
    return ast;
  }

  override decorateSourceInput(input: SourceInput, queryID: string): Input {
    return new MeasurePushOperator(input, queryID, this, 'query-update-client');
  }

  onTransactionCommit(cb: CommitListener): () => void {
    this.#commitListeners.add(cb);
    return () => {
      this.#commitListeners.delete(cb);
    };
  }

  override batchViewUpdates<T>(applyViewUpdates: () => T) {
    let result: T | undefined;
    let viewChangesPerformed = false;
    this.#batchViewUpdates(() => {
      result = applyViewUpdates();
      viewChangesPerformed = true;
    });
    assert(
      viewChangesPerformed,
      'batchViewUpdates must call applyViewUpdates synchronously.',
    );
    return result as T;
  }

  processChanges(
    expectedHead: Hash | undefined,
    newHead: Hash,
    changes: NoIndexDiff,
  ) {
    this.batchViewUpdates(() => {
      try {
        this.#mainSources.advance(expectedHead, newHead, changes);
      } finally {
        this.#endTransaction();
      }
    });
  }

  #endTransaction() {
    for (const listener of this.#commitListeners) {
      try {
        listener();
      } catch (e) {
        // We should not fatal the inner-workings of Zero due to the user's application
        // code throwing an error.
        // Hence we wrap notifications in a try-catch block.
        this.#lc.error?.(
          ErrorKind.Internal,
          'Failed notifying a commit listener of IVM updates',
          e,
        );
      }
    }
  }
}
