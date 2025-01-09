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
  type Input,
  type Output,
  type Query,
  type QueryType,
  type Smash,
  type TableSchema,
  type View,
  type ViewFactory,
} from '../../zero-advanced/src/mod.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';

export type QueryResultDetails = {
  readonly type: ResultType;
};

type State = [Entry, QueryResultDetails];

const complete = {type: 'complete'} as const;
const unknown = {type: 'unknown'} as const;

export class SolidView<V extends View> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  #state: Store<State>;
  #setState: SetStoreFunction<State>;

  constructor(
    input: Input,
    format: Format = {singular: false, relationships: {}},
    onDestroy: () => void = () => {},
    queryComplete: true | Promise<true> = true,
  ) {
    this.#input = input;
    this.#format = format;
    this.#onDestroy = onDestroy;
    [this.#state, this.#setState] = createStore<State>([
      {'': format.singular ? undefined : []},
      queryComplete === true ? complete : unknown,
    ]);
    input.setOutput(this);

    this.#setState(
      produce(draftState => {
        for (const node of input.fetch({})) {
          applyChange(
            draftState[0],
            {type: 'add', node},
            input.getSchema(),
            '',
            this.#format,
          );
        }
      }),
    );
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

  push(change: Change): void {
    this.#setState(
      produce((draftState: State) => {
        applyChange(
          draftState[0],
          change,
          this.#input.getSchema(),
          '',
          this.#format,
        );
      }),
    );
  }
}

export function solidViewFactory<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  _query: Query<TSchema, TReturn>,
  input: Input,
  format: Format,
  onDestroy: () => void,
  _onTransactionCommit: (cb: () => void) => void,
  queryComplete: true | Promise<true>,
): SolidView<Smash<TReturn>> {
  return new SolidView<Smash<TReturn>>(input, format, onDestroy, queryComplete);
}

solidViewFactory satisfies ViewFactory<TableSchema, QueryType, unknown>;
