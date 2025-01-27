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

  #pendingChanges: Change[] = [];

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
    this.#pendingChanges.push(change);
  }
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
