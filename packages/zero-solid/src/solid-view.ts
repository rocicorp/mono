import {createStore, produce, type SetStoreFunction} from 'solid-js/store';
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

export class SolidView<V extends View> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  readonly #rootStore: Entry;
  readonly #setRoot: SetStoreFunction<Entry>;

  readonly #resultTypeStore: {resultType: ResultType};
  readonly #setResultType: SetStoreFunction<{resultType: ResultType}>;

  constructor(
    input: Input,
    format: Format = {singular: false, relationships: {}},
    onDestroy: () => void = () => {},
    queryComplete: true | Promise<true> = true,
  ) {
    this.#input = input;
    this.#format = format;
    this.#onDestroy = onDestroy;
    [this.#rootStore, this.#setRoot] = createStore({
      '': format.singular ? undefined : [],
    });
    [this.#resultTypeStore, this.#setResultType] = createStore({
      resultType: queryComplete === true ? 'complete' : 'unknown',
    });
    input.setOutput(this);

    this.#setRoot(
      produce(draftRoot => {
        for (const node of input.fetch({})) {
          applyChange(
            draftRoot,
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
        this.#setResultType({resultType: 'complete'});
      });
    }
  }

  get data() {
    return this.#rootStore[''] as V;
  }

  get resultType() {
    return this.#resultTypeStore.resultType;
  }

  destroy() {
    this.#onDestroy();
  }

  push(change: Change): void {
    this.#setRoot(
      produce(draftRoot => {
        applyChange(
          draftRoot,
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
  const v = new SolidView<Smash<TReturn>>(
    input,
    format,
    onDestroy,
    queryComplete,
  );

  return v;
}

solidViewFactory satisfies ViewFactory<TableSchema, QueryType, unknown>;
