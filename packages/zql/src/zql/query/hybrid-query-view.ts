import {AST} from '../ast/ast.js';
import {SubscriptionDelegate} from '../context/context.js';
import {ArrayView, EntryList} from '../ivm/array-view.js';
import {Immutable} from 'shared/src/immutable.js';

export type Listener = (entries: Immutable<EntryList>) => void;

/**
 * Wraps an ArrayView and wires it to the ZeroProtocol
 * so that the query is also registered with and executed
 * against the server.
 */
export class HybridQueryView {
  readonly #view: ArrayView;
  readonly #ast: AST;
  readonly #subscriptionDelegate: SubscriptionDelegate;
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
    );
    const cleanupViewListener = this.#view.addListener(listener);
    return () => {
      subscriptionRemoved();
      cleanupViewListener();
    };
  }

  destroy() {
    this.#view.destroy();
  }

  hydrate() {
    if (this.#hydrated) {
      throw new Error('Already hydrated');
    }
    this.#hydrated = true;
    this.#view.hydrate();
  }
}
