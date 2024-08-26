import {View} from '../ivm2/view.js';
import {EmptyQueryResultRow, QueryResultRow, Smash} from './query.js';
import {TypedView} from './typed-view.js';

export class MaterializedQuery<
  TReturn extends Iterable<QueryResultRow> = Iterable<EmptyQueryResultRow>,
> {
  readonly #view: TypedView<TReturn>;
  #lastResult: TReturn | undefined;

  constructor(view: View) {
    this.#view = view as unknown as TypedView<TReturn>;
    this.#view.addListener(data => {
      this.#lastResult = data;
    });
    this.#view.hydrate();
  }

  get(): Smash<TReturn> {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return this.#lastResult! as any;
  }

  subscribe(listener: (data: Smash<TReturn>) => void): () => void {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const ret = this.#view.addListener(listener as any);
    if (this.#lastResult !== undefined) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      listener(this.#lastResult as any);
    }
    return ret;
  }

  disconnect() {
    this.#view.destroy();
  }
}
