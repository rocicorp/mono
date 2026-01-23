import {reconcile, type SetStoreFunction} from 'solid-js/store';
import {emptyArray} from '../../shared/src/sentinels.ts';
import {
  applyChange,
  idSymbol,
  skipYields,
  type ViewChange,
} from './bindings.ts';
import {
  type AnyViewFactory,
  type Change,
  type Entry,
  type ErroredQuery,
  type Format,
  type Input,
  type Node,
  type Output,
  type Query,
  type QueryErrorDetails,
  type QueryResultDetails,
  type Schema,
  type Stream,
  type TTL,
} from './zero.ts';

export type State = [Entry, QueryResultDetails];

export const COMPLETE: QueryResultDetails = Object.freeze({type: 'complete'});
export const UNKNOWN: QueryResultDetails = Object.freeze({type: 'unknown'});

export class SolidView implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;
  readonly #retry: () => void;

  #setState: SetStoreFunction<State>;

  // Optimization: if the store is currently empty we build up
  // the view on a plain old JS object stored at #builderRoot, and return
  // that for the new state on transaction commit.  This avoids building up
  // large views from scratch via solid produce.  The proxy object used by
  // solid produce is slow and in this case we don't care about solid tracking
  // the fine grained changes (everything has changed, it's all new).  For a
  // test case with a view with 3000 rows, each row having 2 children, this
  // optimization reduced #applyChanges time from 743ms to 133ms.
  #builderRoot: Entry | undefined;
  #pendingChanges: ViewChange[] = [];
  readonly #updateTTL: (ttl: TTL) => void;

  constructor(
    input: Input,
    onTransactionCommit: (cb: () => void) => void,
    format: Format,
    onDestroy: () => void,
    queryComplete: true | ErroredQuery | Promise<true>,
    updateTTL: (ttl: TTL) => void,
    setState: SetStoreFunction<State>,
    retry: () => void,
  ) {
    this.#input = input;
    onTransactionCommit(this.#onTransactionCommit);
    this.#format = format;
    this.#onDestroy = onDestroy;
    this.#updateTTL = updateTTL;
    this.#retry = retry;

    input.setOutput(this);

    const emptyRoot = this.#createEmptyRoot();
    const initialRoot = this.#applyChangesToRoot(
      skipYields(input.fetch({})),
      node => ({type: 'add', node}),
      emptyRoot,
    );

    this.#setState = setState;
    this.#setState(
      reconcile(
        [
          initialRoot,
          queryComplete === true
            ? COMPLETE
            : 'error' in queryComplete
              ? this.#makeError(queryComplete)
              : UNKNOWN,
        ],
        {
          // solidjs's types want a string, but a symbol works
          key: idSymbol as unknown as string,
        },
      ),
    );

    if (isEmptyRoot(initialRoot)) {
      this.#builderRoot = this.#createEmptyRoot();
    }

    if (queryComplete !== true && !('error' in queryComplete)) {
      void queryComplete
        .then(() => {
          this.#setState(prev => [prev[0], COMPLETE]);
        })
        .catch((error: ErroredQuery) => {
          this.#setState(prev => [prev[0], this.#makeError(error)]);
        });
    }
  }

  #makeError(error: ErroredQuery): QueryErrorDetails {
    const message = error.message ?? 'An unknown error occurred';
    return {
      type: 'error',
      retry: this.#retry,
      refetch: this.#retry,
      error: {
        type: error.error,
        message,
        ...(error.details ? {details: error.details} : {}),
      },
    };
  }

  destroy(): void {
    this.#onDestroy();
  }

  #onTransactionCommit = () => {
    const builderRoot = this.#builderRoot;
    if (builderRoot) {
      if (!isEmptyRoot(builderRoot)) {
        this.#setState(
          0,
          reconcile(builderRoot, {
            // solidjs's types want a string, but a symbol works
            key: idSymbol as unknown as string,
          }),
        );
        this.#setState(prev => [builderRoot, prev[1]]);
        this.#builderRoot = undefined;
      }
    } else {
      try {
        this.#applyChanges(this.#pendingChanges, c => c);
      } finally {
        this.#pendingChanges = [];
      }
    }
  };

  push(change: Change) {
    // Delay updating the solid store state until the transaction commit
    // (because each update of the solid store is quite expensive).  If
    // this.#builderRoot is defined apply the changes to it (we are building
    // from an empty root), otherwise queue the changes to be applied
    // using produce at the end of the transaction but read the relationships
    // now as they are only valid to read when the push is received.
    if (this.#builderRoot) {
      this.#builderRoot = this.#applyChangeToRoot(change, this.#builderRoot);
    } else {
      this.#pendingChanges.push(materializeRelationships(change));
    }
    return emptyArray;
  }

  #currentRoot: Entry | undefined;

  #applyChanges<T>(changes: Iterable<T>, mapper: (v: T) => ViewChange): void {
    // BEHAVIOR CHANGE: Previously used `produce` (immer-style mutations) to apply
    // changes directly to the Solid store. Now we apply changes immutably first,
    // then use `reconcile` to diff the new state into the store.
    //
    // Why the change: `applyChange` now returns new immutable Entry objects
    // (using spread, toSpliced, with) instead of mutating in place. This enables
    // React.memo optimization since unchanged rows keep their object identity.
    //
    // How it works now:
    // 1. Read current state from the store
    // 2. Apply all changes immutably to produce a new root Entry
    // 3. Use `reconcile` to diff the new state into the store
    //
    // `reconcile` compares new vs old by key (idSymbol) and only updates changed
    // properties, preserving fine-grained reactivity at the property level.
    this.#setState((prev: State): State => {
      this.#currentRoot = prev[0];
      return prev;
    });

    if (this.#currentRoot === undefined) {
      return;
    }

    const newRoot = this.#applyChangesToRoot<T>(
      changes,
      mapper,
      this.#currentRoot,
    );
    this.#currentRoot = undefined;

    if (isEmptyRoot(newRoot)) {
      this.#builderRoot = this.#createEmptyRoot();
    }

    // Use `reconcile` to diff the new immutable state into the Solid store.
    //
    // Options:
    // - key: idSymbol - Match array items by their stable ID (derived from primary key)
    // - merge: true - Update individual properties rather than replacing objects,
    //   enabling fine-grained reactivity (only re-render components that depend
    //   on changed fields)
    //
    // PREVIOUS BEHAVIOR: `produce` applied mutations directly, so row object
    // identity was always preserved (same object, properties mutated in place).
    //
    // CURRENT BEHAVIOR: `reconcile` with merge:true preserves row identity when
    // the idSymbol matches. The row object reference stays the same, and only
    // changed properties trigger reactivity. EXCEPTION: When a primary key changes,
    // the idSymbol changes, so reconcile treats it as a remove+add (new row object).
    // This is semantically correct: a row with a different primary key IS a
    // different entity.
    this.#setState(
      0,
      reconcile(newRoot, {
        // solidjs's types want a string, but a symbol works
        key: idSymbol as unknown as string,
        merge: true,
      }),
    );
  }

  #applyChangesToRoot<T>(
    changes: Iterable<T>,
    mapper: (v: T) => ViewChange,
    root: Entry,
  ): Entry {
    let currentRoot = root;
    for (const change of changes) {
      currentRoot = this.#applyChangeToRoot(mapper(change), currentRoot);
    }
    return currentRoot;
  }

  #applyChangeToRoot(change: ViewChange, root: Entry): Entry {
    return applyChange(
      root,
      change,
      this.#input.getSchema(),
      '',
      this.#format,
      true /* withIDs */,
    );
  }

  #createEmptyRoot(): Entry {
    return {
      '': this.#format.singular ? undefined : [],
    };
  }

  updateTTL(ttl: TTL): void {
    this.#updateTTL(ttl);
  }
}

function materializeRelationships(change: Change): ViewChange {
  switch (change.type) {
    case 'add':
      return {type: 'add', node: materializeNodeRelationships(change.node)};
    case 'remove':
      return {type: 'remove', node: materializeNodeRelationships(change.node)};
    case 'child':
      return {
        type: 'child',
        node: {row: change.node.row},
        child: {
          relationshipName: change.child.relationshipName,
          change: materializeRelationships(change.child.change),
        },
      };
    case 'edit':
      return {
        type: 'edit',
        node: {row: change.node.row},
        oldNode: {row: change.oldNode.row},
      };
  }
}

function materializeNodeRelationships(node: Node): Node {
  const relationships: Record<string, () => Stream<Node>> = {};
  for (const relationship in node.relationships) {
    const materialized: Node[] = [];
    for (const n of skipYields(node.relationships[relationship]())) {
      materialized.push(materializeNodeRelationships(n));
    }
    relationships[relationship] = () => materialized;
  }
  return {
    row: node.row,
    relationships,
  };
}

function isEmptyRoot(entry: Entry) {
  const data = entry[''];
  return data === undefined || (Array.isArray(data) && data.length === 0);
}

export function createSolidViewFactory(
  setState: SetStoreFunction<State>,
  retry?: () => void,
) {
  function solidViewFactory<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
  >(
    _query: Query<TTable, TSchema, TReturn>,
    input: Input,
    format: Format,
    onDestroy: () => void,
    onTransactionCommit: (cb: () => void) => void,
    queryComplete: true | ErroredQuery | Promise<true>,
    updateTTL: (ttl: TTL) => void,
  ) {
    return new SolidView(
      input,
      onTransactionCommit,
      format,
      onDestroy,
      queryComplete,
      updateTTL,
      setState,
      retry || (() => {}),
    );
  }

  solidViewFactory satisfies AnyViewFactory;

  return solidViewFactory;
}
