import type {LogContext} from '@rocicorp/logger';
import type {NoIndexDiff} from '../../../replicache/src/btree/node.ts';
import type {Hash} from '../../../replicache/src/hash.ts';
import {assert} from '../../../shared/src/asserts.ts';
import type {AST} from '../../../zero-protocol/src/ast.ts';
import {MemoryStorage} from '../../../zql/src/ivm/memory-storage.ts';
import type {Input, Storage} from '../../../zql/src/ivm/operator.ts';
import type {Source} from '../../../zql/src/ivm/source.ts';
import type {
  CommitListener,
  GotCallback,
  QueryDelegate,
} from '../../../zql/src/query/query-impl.ts';
import type {TTL} from '../../../zql/src/query/ttl.ts';
import {type IVMSourceBranch} from './ivm-branch.ts';

export type AddQuery = (
  ast: AST,
  ttl: TTL,
  gotCallback: GotCallback | undefined,
) => () => void;

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
  readonly #batchViewUpdates: (applyViewUpdates: () => void) => void;
  readonly #commitListeners: Set<CommitListener> = new Set();

  readonly slowMaterializeThreshold: number;
  readonly lc: LogContext;
  readonly staticQueryParameters = undefined;

  constructor(
    lc: LogContext,
    mainSources: IVMSourceBranch,
    addQuery: AddQuery,
    batchViewUpdates: (applyViewUpdates: () => void) => void,
    slowMaterializeThreshold: number,
  ) {
    this.#mainSources = mainSources;
    this.#addQuery = addQuery;
    this.#batchViewUpdates = batchViewUpdates;
    this.lc = lc;
    this.slowMaterializeThreshold = slowMaterializeThreshold;
  }

  getSource(name: string): Source | undefined {
    return this.#mainSources.getSource(name);
  }

  addServerQuery(ast: AST, ttl: TTL, gotCallback?: GotCallback | undefined) {
    return this.#addQuery(ast, ttl, gotCallback);
  }

  onQueryMaterialized(hash: string, ast: AST, duration: number): void {
    if (
      this.slowMaterializeThreshold !== undefined &&
      duration > this.slowMaterializeThreshold
    ) {
      this.lc.warn?.(
        'Slow query materialization (including server/network)',
        hash,
        ast,
        duration,
      );
    } else {
      this.lc.debug?.(
        'Materialized query (including server/network)',
        hash,
        ast,
        duration,
      );
    }
  }

  createStorage(): Storage {
    return new MemoryStorage();
  }

  decorateInput(input: Input): Input {
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
        this.lc?.error?.(
          'Failed notifying a commit listener of IVM updates',
          e,
        );
      }
    }
  }
}
