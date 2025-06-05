import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {ErrorKind} from '../../../zero-protocol/src/error-kind.ts';
import type {FilterInput} from '../../../zql/src/ivm/filter-operators.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Input, Storage} from '../../../zql/src/ivm/operator.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {CustomQueryID} from '../../../zql/src/query/named.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import type {RunOptions} from '../../../zql/src/query/query.ts';
import type {TTL} from '../../../zql/src/query/ttl.ts';
import {type IVMSourceBranch} from './ivm-branch.ts';
import type {QueryManager} from './query-manager.ts';
import type {ZeroLogContext} from './zero-log-context.ts';

export type AddQuery = QueryManager['addLegacy'];
export type AddCustomQuery = QueryManager['addCustom'];

export type UpdateQuery = QueryManager['update'];

/**
 * ZeroContext glues together zql and Replicache. It listens to changes in
 * Replicache data and pushes them into IVM and on tells the server about new
 * queries.
 */
export class ZeroContext implements QueryDelegate {
  // It is a bummer to have to maintain separate MemorySources here and copy the
  // data in from the Replicache db. But we want the data to be accessible via
  // pipelines *synchronously* and the core Replicache infra is all async. So
  // that needs to be fixed.
  readonly #mainSources: IVMSourceBranch;
  readonly #addQuery: AddQuery;
  readonly #addCustomQuery: AddCustomQuery;
  readonly #updateQuery: UpdateQuery;
  readonly #batchViewUpdates: (applyViewUpdates: () => void) => void;
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly #slowMaterializeThreshold: number;
  readonly #lc: ZeroLogContext;
  readonly assertValidRunOptions: (options?: RunOptions) => void;

  /**
   * Client-side queries start out as "unknown" and are then updated to
   * "complete" once the server has sent back the query result.
   */
  readonly defaultQueryComplete = false;

  constructor(
    lc: ZeroLogContext,
    mainSources: IVMSourceBranch,
    addQuery: AddQuery,
    addCustomQuery: AddCustomQuery,
    updateQuery: UpdateQuery,
    batchViewUpdates: (applyViewUpdates: () => void) => void,
    slowMaterializeThreshold: number,
    assertValidRunOptions: (options?: RunOptions) => void,
  ) {
    this.#mainSources = mainSources;
    this.#addQuery = addQuery;
    this.#updateQuery = updateQuery;
    this.#batchViewUpdates = batchViewUpdates;
    this.#lc = lc;
    this.#slowMaterializeThreshold = slowMaterializeThreshold;
    this.assertValidRunOptions = assertValidRunOptions;
    this.#addCustomQuery = addCustomQuery;
  }

  getSource(name: string): Source | undefined {
    return this.#mainSources.getSource(name);
  }

  addCustomQuery(
    customQueryID: CustomQueryID,
    ttl: TTL,
    gotCallback?: GotCallback | undefined,
  ): () => void {
    return this.#addCustomQuery(
      customQueryID.name,
      customQueryID.args,
      ttl,
      gotCallback,
    );
  }

  addServerQuery(ast: AST, ttl: TTL, gotCallback?: GotCallback | undefined) {
    return this.#addQuery(ast, ttl, gotCallback);
  }

  updateServerQuery(ast: AST, ttl: TTL): void {
    this.#updateQuery(ast, ttl);
  }

  onQueryMaterialized(hash: string, ast: AST, duration: number): void {
    if (
      this.#slowMaterializeThreshold !== undefined &&
      duration > this.#slowMaterializeThreshold
    ) {
      this.#lc.warn?.(
        'Slow query materialization (including server/network)',
        hash,
        ast,
        duration,
      );
    } else {
      this.#lc.debug?.(
        'Materialized query (including server/network)',
        hash,
        ast,
        duration,
      );
    }
  }

  mapAst(ast: AST): AST {
    return ast;
  }

  createStorage(): Storage {
    return new MemoryStorage();
  }

  decorateInput(input: Input): Input {
    return input;
  }

  decorateFilterInput(input: FilterInput): FilterInput {
    return input;
  }

  onTransactionCommit(cb: CommitListener): () => void {
    this.#commitListeners.add(cb);
    return () => {
      this.#commitListeners.delete(cb);
    };
  }

  batchViewUpdates<T>(applyViewUpdates: () => T) {
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
