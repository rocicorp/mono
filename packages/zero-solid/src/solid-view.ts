import {
  createStore,
  produce,
  unwrap,
  type SetStoreFunction,
  type Store,
} from 'solid-js/store';
import {
  applyChange,
  type Change,
  type Entry,
  type Format,
  type HumanReadable,
  type Input,
  type Node,
  type Output,
  type Query,
  type ResultType,
  type Stream,
  type ViewChange,
  type ViewFactory,
} from '../../zero-advanced/src/mod.js';
import type {Schema} from '../../zero-schema/src/mod.js';
import type {RefCountMap} from '../../zql/src/ivm/view-apply-change.ts';

export type QueryResultDetails = {
  readonly type: ResultType;
};

type State = [Entry, QueryResultDetails];

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

/**
 * We need this class since Solid wraps the underlying object in a proxy
 * and we need to store the ref count on the underlying object.
 */
class SolidRefCountMap implements RefCountMap {
  readonly #map = new WeakMap<Entry, number>();

  get(entry: Entry): number | undefined {
    return this.#map.get(unwrap(entry));
  }
  set(entry: Entry, refCount: number): void {
    this.#map.set(unwrap(entry), refCount);
  }
  delete(entry: Entry): boolean {
    return this.#map.delete(unwrap(entry));
  }
}

export class SolidView<V> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  #state: Store<State>;
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
  readonly #refCountMap = new SolidRefCountMap();

  constructor(
    input: Input,
    onTransactionCommit: (cb: () => void) => void,
    format: Format,
    onDestroy: () => void,
    queryComplete: true | Promise<true>,
  ) {
    this.#input = input;
    onTransactionCommit(this.#onTransactionCommit);
    this.#format = format;
    this.#onDestroy = onDestroy;
    input.setOutput(this);

    const initialRoot = this.#createEmptyRoot();
    this.#applyChangesToRoot(
      input.fetch({}),
      node => ({type: 'add', node}),
      initialRoot,
    );
    [this.#state, this.#setState] = createStore<State>([
      initialRoot,
      queryComplete === true ? complete : unknown,
    ]);
    if (isEmptyRoot(initialRoot)) {
      this.#builderRoot = this.#createEmptyRoot();
    }

    if (queryComplete !== true) {
      void queryComplete.then(() => {
        this.#setState(oldState => [oldState[0], complete]);
      });
    }
  }

  get data(): V {
    return this.#state[0][''] as V;
  }

  get resultDetails(): QueryResultDetails {
    return this.#state[1];
  }

  destroy(): void {
    this.#onDestroy();
  }

  #onTransactionCommit = () => {
    const builderRoot = this.#builderRoot;
    if (builderRoot) {
      if (!isEmptyRoot(builderRoot)) {
        this.#setState(oldState => [builderRoot, oldState[1]]);
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

  push(change: Change): void {
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
      this.#refCountMap,
    );
  }

  #createEmptyRoot(): Entry {
    return {
      '': this.#format.singular ? undefined : [],
    };
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
    for (const n of node.relationships[relationship]()) {
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

export function solidViewFactory<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  _query: Query<TSchema, TTable, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
) {
  return new SolidView<HumanReadable<TReturn>>(
    input,
    onTransactionCommit,
    format,
    onDestroy,
    queryComplete,
  );
}

solidViewFactory satisfies ViewFactory<Schema, string, unknown, unknown>;
