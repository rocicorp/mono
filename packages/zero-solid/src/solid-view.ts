import {produce, reconcile, type SetStoreFunction} from 'solid-js/store';
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

/**
 * SolidView bridges Zero's incremental view updates with Solid's reactive
 * store.
 *
 * The challenge: Zero pushes individual row changes (add/remove/edit) as they
 * arrive, but updating Solid's store on each push is expensive. We batch
 * changes until transaction commit, then apply them all at once.
 *
 * Two code paths optimize for different scenarios:
 *
 *   1. BULK INSERT (builderRoot path): When building from empty, we skip
 *      Solid's reactive tracking entirely and build a plain JS object. This is
 *      5x faster for large initial loads. Uses reconcile() to diff the final
 *      result into Solid.
 *
 *   2. INCREMENTAL UPDATE (#pendingChanges path): For existing views, we queue
 *      changes and apply them in-place within a produce() draft. produce()
 *      creates a mutable draft, applyChange mutates it directly, then produce()
 *      handles diffing and reactivity.
 *
 *
 *   push(change)
 *       │
 *       ▼
 *   ┌─────────────────────────────────┐
 *   │ builderRoot defined?            │
 *   │ (building from empty)           │
 *   └─────────────────────────────────┘
 *       │yes                │no
 *       ▼                   ▼
 *   Build plain JS      Queue in
 *   object (fast)       #pendingChanges
 *       │                   │
 *       └───────┬───────────┘
 *               ▼
 *         onTransactionCommit()
 *               │
 *       ┌───────┴──────────┐
 *       │                  │
 *   builderRoot      produce(draft =>
 *   → reconcile()      applyChange(draft, ..., mutate: true))
 */
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

    const initialRoot = this.#createEmptyRoot();
    this.#applyChangesToRoot(
      skipYields(input.fetch({})),
      node => ({type: 'add', node}),
      initialRoot,
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
      this.#applyChangeToRoot(change, this.#builderRoot);
    } else {
      this.#pendingChanges.push(materializeRelationships(change));
    }
    return emptyArray;
  }

  #applyChanges<T>(changes: Iterable<T>, mapper: (v: T) => ViewChange): void {
    this.#setState(
      produce((draftState: State) => {
        this.#applyChangesToRoot<T>(changes, mapper, draftState[0]);
        if (isEmptyRoot(draftState[0])) {
          this.#builderRoot = this.#createEmptyRoot();
        }
      }),
    );
  }

  #applyChangesToRoot<T>(
    changes: Iterable<T>,
    mapper: (v: T) => ViewChange,
    root: Entry,
  ) {
    for (const change of changes) {
      this.#applyChangeToRoot(mapper(change), root);
    }
  }

  #applyChangeToRoot(change: ViewChange, root: Entry) {
    applyChange(
      root,
      change,
      this.#input.getSchema(),
      '',
      this.#format,
      true /* withIDs */,
      true /* mutate */,
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
