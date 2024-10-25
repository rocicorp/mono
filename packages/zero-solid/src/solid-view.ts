import type {Input, Output} from '../../zql/src/zql/ivm/operator.js';
import type {TableSchema} from '../../zql/src/zql/query/schema.js';
import {type Entry} from '../../zql/src/zql/ivm/view.js';
import {createStore, produce, type SetStoreFunction} from 'solid-js/store';
import type {Change} from '../../zql/src/zql/ivm/change.js';
import type {View, Format} from '../../zql/src/zql/ivm/view.js';
import {applyChange} from '../../zql/src/zql/ivm/array-view.js';
import type {Query, QueryType, Smash} from '../../zql/src/zql/query/query.js';

export class SolidView<V extends View> implements Output {
  readonly #input: Input;
  readonly #format: Format;
  readonly #onDestroy: () => void;

  // Synthetic "root" entry that has a single "" relationship, so that we can
  // treat all changes, including the root change, generically.
  readonly #root: Entry;
  readonly #setRoot: SetStoreFunction<Entry>;

  constructor(
    input: Input,
    format: Format = {singular: false, relationships: {}},
    onDestroy: () => void = () => {},
  ) {
    this.#input = input;
    this.#format = format;
    this.#onDestroy = onDestroy;
    this.#input.setOutput(this);
    [this.#root, this.#setRoot] = createStore({
      '': format.singular ? undefined : [],
    });

    this.#setRoot(
      produce(draftRoot => {
        for (const node of this.#input.fetch({})) {
          applyChange(
            draftRoot,
            {type: 'add', node},
            this.#input.getSchema(),
            '',
            this.#format,
          );
        }
      }),
    );
  }

  get data() {
    return this.#root[''] as V;
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
): SolidView<Smash<TReturn>> {
  const v = new SolidView<Smash<TReturn>>(input, format, onDestroy);

  return v;
}
