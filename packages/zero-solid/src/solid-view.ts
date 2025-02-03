import {
  createStore,
  reconcile,
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
} from '../../zero/src/advanced.ts';
import type {ResultType, Schema} from '../../zero/src/zero.ts';
import {deepClone} from '../../shared/src/deep-clone.ts';
import {createSignal} from 'solid-js';

export type QueryResultDetails = {
  readonly type: ResultType;
};

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

export class SolidView<V> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  #root: Entry;
  #rootStore: Store<Entry>;
  #setRootStore: SetStoreFunction<Entry>;
  #resultDetails: () => QueryResultDetails;

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
    this.#root = {'': format.singular ? undefined : []};
    input.setOutput(this);

    for (const node of input.fetch({})) {
      applyChange(
        this.#root,
        {
          type: 'add',
          node,
        },
        this.#input.getSchema(),
        '',
        this.#format,
      );
    }

    [this.#rootStore, this.#setRootStore] = createStore<Entry>(
      deepClone(this.#root) as Entry,
    );
    const [resultDetails, setResultDetails] = createSignal<QueryResultDetails>(
      queryComplete === true ? complete : unknown,
    );
    this.#resultDetails = resultDetails;

    if (queryComplete !== true) {
      void queryComplete.then(() => {
        setResultDetails(complete);
      });
    }
  }

  get data(): V {
    return this.#rootStore[''] as V;
  }

  get resultDetails(): QueryResultDetails {
    return this.#resultDetails();
  }

  destroy(): void {
    this.#onDestroy();
  }

  #onTransactionCommit = () => {
    this.#setRootStore(reconcile(this.#root));
  };

  push(change: Change): void {
    applyChange(this.#root, change, this.#input.getSchema(), '', this.#format);
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
