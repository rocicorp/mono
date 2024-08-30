import {AST} from '../ast2/ast.js';
import {SubscriptionDelegate} from '../context/context.js';
import {ArrayView, EntryList} from '../ivm/array-view.js';
import {Immutable} from 'shared/src/immutable.js';

export type ResultType = 'complete' | 'partial' | 'none';
export type Listener = (
  entries: Immutable<EntryList>,
  resultType: ResultType,
) => void;

/**
 * Wraps an ArrayView and wires it to the ZeroProtocol
 * so that the query is also registered with and executed
 * against the server.
 */
export class HybridQueryView {
  readonly #view: ArrayView;
  readonly #ast: AST;
  readonly #subscriptionDelegate: SubscriptionDelegate;
  #resultType: ResultType = 'none';
  #hydrated = false;

  constructor(
    subscriptionDelegate: SubscriptionDelegate,
    ast: AST,
    view: ArrayView,
  ) {
    this.#ast = ast;
    this.#subscriptionDelegate = subscriptionDelegate;
    this.#view = view;
  }

  get data() {
    return this.#view.data;
  }

  addListener(listener: Listener) {
    const subscriptionRemoved = this.#subscriptionDelegate.subscriptionAdded(
      this.#ast,
      got => {
        if (got) {
          this.#resultType = 'complete';
        }
        if (this.#hydrated) {
          // When we get the gotQueries signal, we need to call the callback since
          // the result might be empty and the view won't trigger a change.
          listener(this.#view.data, this.#resultType);
        }
      },
    );

    const cleanupViewListener = this.#view.addListener(() => {
      if (this.#resultType === 'none') {
        this.#resultType = 'partial';
      }
      listener(this.#view.data, this.#resultType);
    });
    return () => {
      subscriptionRemoved();
      cleanupViewListener();
    };
  }

  destroy() {
    this.#view.destroy();
  }

  hydrate() {
    this.#hydrated = true;
    this.#view.hydrate();
  }
}
