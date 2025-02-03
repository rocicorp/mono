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
import type {Row} from '../../zero-protocol/src/data.ts';
import type {Node} from '../../zql/src/ivm/data.ts';

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

  #pendingChanges: DrainedChange[] = [];

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
      {'': format.singular ? undefined : []},
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
    this.#applyChanges(this.#pendingChanges, c => c);
  };

  #applyChanges<T>(changes: Iterable<T>, mapper: (v: T) => Change): void {
    try {
      this.#setState(
        produce((draftState: State) => {
          for (const change of changes) {
            applyChange(
              draftState[0],
              mapper(change),
              this.#input.getSchema(),
              '',
              this.#format,
            );
          }
        }),
      );
    } finally {
      this.#pendingChanges = [];
    }
  }

  push(change: Change): void {
    // Delay setting the state until the transaction commit.
    this.#pendingChanges.push(drain(change));
  }
}

function drain(change: Change): DrainedChange {
  switch (change.type) {
    case 'add':
      return {type: 'add', node: drainNode(change.node)};
    case 'remove':
      return {type: 'remove', node: drainNode(change.node)};
    case 'child':
      return {
        type: 'child',
        node: drainNode(change.node),
        child: {
          relationshipName: change.child.relationshipName,
          change: drain(change.child.change),
        },
      };
    case 'edit':
      return {
        type: 'edit',
        node: drainNode(change.node),
        oldNode: drainNode(change.oldNode),
      };
  }
}

function drainNode(node: Node): DrainedNode {
  return {
    row: node.row,
    relationships: Object.fromEntries(
      Object.entries(node.relationships).map(([relationship, stream]) => {
        const drained = [...stream()].map(drainNode);
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

type DrainedChange = AddChange | RemoveChange | ChildChange | EditChange;

export type AddChange = {
  type: 'add';
  node: DrainedNode;
};

export type RemoveChange = {
  type: 'remove';
  node: DrainedNode;
};

type ChildChange = {
  type: 'child';
  node: DrainedNode;
  child: {
    relationshipName: string;
    change: DrainedChange;
  };
};

type EditChange = {
  type: 'edit';
  node: DrainedNode;
  oldNode: DrainedNode;
};

type DrainedNode = {
  row: Row;
  relationships: Record<string, () => DrainedNode[]>;
};
