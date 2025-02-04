import {
  createStore,
  produce,
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
  type Output,
  type Query,
  type ViewFactory,
} from '../../zero-advanced/src/mod.js';
import type {Schema} from '../../zero-schema/src/mod.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';
import type {Node} from '../../zql/src/ivm/data.ts';
import type {ViewChange} from '../../zql/src/ivm/view-apply-change.ts';

export type QueryResultDetails = {
  readonly type: ResultType;
};

type State = [Entry, QueryResultDetails];

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

export class SolidView<V> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  #state: Store<State>;
  #setState: SetStoreFunction<State>;

  #pendingChanges: ViewChange[] = [];

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
    [this.#state, this.#setState] = createStore<State>([
      this.#initialEmptyEntry(),
      queryComplete === true ? complete : unknown,
    ]);
    input.setOutput(this);

    this.#applyChanges(input.fetch({}), node => ({type: 'add', node}));

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
    try {
      this.#applyChanges(this.#pendingChanges, c => c);
    } finally {
      this.#pendingChanges = [];
    }
  };

  push(change: Change): void {
    // Delay updating the solid store state until the transaction commit
    // (because each update of the solid store is quite expensive), but
    // read the relationships now as they are only valid to read
    // when the push is received.
    this.#pendingChanges.push(materializeRelationships(change));
  }

  #applyChanges<T>(changes: Iterable<T>, mapper: (v: T) => ViewChange): void {
    this.#setState(oldState => {
      // Optimization: if the store is currently empty build up
      // the view on a new plain old JS object root, and return that
      // for the new state.  This avoids building up large views from
      // scratch via solid produce.  The proxy object used by solid produce
      // is slow and in this case we don't care about solid tracking the fine
      // grained changes (everything has changed, its all new).  For a test case
      // with a view with 3000 rows, each row having 2 children, this
      // optimization reduced #applyChanges time from 743ms to 133ms.
      if (
        oldState[0][''] === undefined ||
        (Array.isArray(oldState[0]['']) && oldState[0][''].length === 0)
      ) {
        const root: Entry = this.#initialEmptyEntry();
        this.#applyChangesToRoot<T>(changes, mapper, root);
        return [root, oldState[1]];
      }
      return produce((draftState: State) => {
        this.#applyChangesToRoot<T>(changes, mapper, draftState[0]);
      })(oldState);
    });
  }

  #applyChangesToRoot<T>(
    changes: Iterable<T>,
    mapper: (v: T) => ViewChange,
    root: Entry,
  ) {
    for (const change of changes) {
      applyChange(
        root,
        mapper(change),
        this.#input.getSchema(),
        '',
        this.#format,
      );
    }
  }

  #initialEmptyEntry(): Entry {
    return {
      '': this.#format.singular ? undefined : [],
    };
  }
}

function materializeRelationships(change: Change): ViewChange {
  switch (change.type) {
    case 'add':
      return {type: 'add', node: materializeNodesRelationships(change.node)};
    case 'remove':
      return {type: 'remove', node: materializeNodesRelationships(change.node)};
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

function materializeNodesRelationships(node: Node): Node {
  return {
    row: node.row,
    relationships: Object.fromEntries(
      Object.entries(node.relationships).map(([relationship, stream]) => {
        const drained = [...stream()].map(materializeNodesRelationships);
        return [relationship, () => drained];
      }),
    ),
  };
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
