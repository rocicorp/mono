import {useSyncExternalStore} from 'react';
import {deepClone} from '../../shared/src/deep-clone.js';
import type {Immutable} from '../../shared/src/immutable.js';
import type {
  Query,
  QueryType,
  ReadonlyJSONValue,
  Smash,
  TableSchema,
  TypedView,
} from '../../zero-client/src/mod.js';
import type {AdvancedQuery} from '../../zql/src/zql/query/query-internal.js';
import {useZero} from './use-zero.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(q: Query<TSchema, TReturn>, enable: boolean = true): Smash<TReturn> {
  const z = useZero();
  const view = viewStore.getView(
    z.clientID,
    q as AdvancedQuery<TSchema, TReturn>,
    enable,
  );
  // https://react.dev/reference/react/useSyncExternalStore
  return useSyncExternalStore(view.subscribeReactInternals, view.getSnapshot);
}

const emptyArray: unknown[] = [];
const disabledSubscriber = () => () => {};

/**
 * A global store of all active views.
 *
 * React subscribes and unsubscribes to these views
 * via `useSyncExternalStore`.
 *
 * Managing views through `useEffect` or `useLayoutEffect` causes
 * inconsistencies because effects run after render.
 *
 * For example, if useQuery used use*Effect in the component below:
 * ```ts
 * function Foo({issueID}) {
 *   const issue = useQuery(z.query.issue.where('id', issueID).one());
 *   if (issue?.id !== undefined && issue.id !== issueID) {
 *     console.log('MISMATCH!', issue.id, issueID);
 *   }
 * }
 * ```
 *
 * `MISMATCH` will be printed whenever the `issueID` prop changes.
 *
 * This is because the component will render once with
 * the old state returned from `useQuery`. Then the effect inside
 * `useQuery` will run. The component will render again with the new
 * state. This inconsistent transition can cause unexpected results.
 *
 * Emulating `useEffect` via `useState` and `if` causes resource leaks.
 * That is:
 *
 * ```ts
 * function useQuery(q) {
 *   const [oldHash, setOldHash] = useState();
 *   if (hash(q) !== oldHash) {
 *      // make new view
 *   }
 *
 *   useEffect(() => {
 *     return () => view.destroy();
 *   }, []);
 * }
 * ```
 *
 * I'm not sure why but in strict mode the cleanup function
 * fails to be called for the first instance of the view and only
 * cleans up later instances.
 *
 * Swapping `useState` to `useRef` has similar problems.
 */
class ViewStore {
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  #views = new Map<string, ViewWrapper<any, any>>();

  getView<TSchema extends TableSchema, TReturn extends QueryType>(
    clientID: string,
    query: AdvancedQuery<TSchema, TReturn>,
    enabled: boolean,
  ): {
    getSnapshot: () => Smash<TReturn>;
    subscribeReactInternals: (internals: () => void) => () => void;
  } {
    if (!enabled) {
      return {
        getSnapshot: () =>
          (query.format.singular ? undefined : emptyArray) as Smash<TReturn>,
        subscribeReactInternals: disabledSubscriber,
      };
    }

    const hash = query.hash() + clientID;
    let existing = this.#views.get(hash);
    if (!existing) {
      existing = new ViewWrapper(
        query,
        view => {
          const lastView = this.#views.get(hash);
          // I don't think this can happen
          // but lets guard against it so we don't
          // leak resources.
          if (lastView && lastView !== view) {
            throw new Error('View already exists');
          }
          this.#views.set(hash, view);
        },
        () => {
          this.#views.delete(hash);
        },
      ) as ViewWrapper<TSchema, TReturn>;
      this.#views.set(hash, existing);
    }
    return existing;
  }
}

const viewStore = new ViewStore();

/**
 * This wraps and ref counts a view.
 *
 * The only signal we have from React as to whether or not it is
 * done with a view is when it calls `unsubscribe`.
 *
 * In non-strict-mode we can clean up the view as soon
 * as the listener count goes to 0.
 *
 * In strict-mode, the listener cound will go to 0 then a
 * new listener for the same view is immeidatiely added back.
 *
 * This is why the `onMaterialized` and `onDematerialized` callbacks exist --
 * they allow a view which React is still referencing to be added
 * back into the store when React re-subscribes to it.
 *
 * This wrapper also exists to deal with the various
 * `useSyncExternalStore` caveats that cause excessive
 * re-renders and materializations.
 *
 * See: https://react.dev/reference/react/useSyncExternalStore#caveats
 * Especially:
 * 1. The store snapshot returned by getSnapshot must be immutable. If the underlying store has mutable data, return a new immutable snapshot if the data has changed. Otherwise, return a cached last snapshot.
 * 2. If a different subscribe function is passed during a re-render, React will re-subscribe to the store using the newly passed subscribe function. You can prevent this by declaring subscribe outside the component.
 */
class ViewWrapper<TSchema extends TableSchema, TReturn extends QueryType> {
  #view: TypedView<Smash<TReturn>> | undefined;
  readonly #defaultSnapshot: Smash<TReturn>;
  readonly #onDematerialized;
  readonly #onMaterialized;
  readonly #query: AdvancedQuery<TSchema, TReturn>;
  #snapshot: Smash<TReturn>;
  #reactInternals: Set<() => void>;

  constructor(
    query: AdvancedQuery<TSchema, TReturn>,
    onMaterialized: (view: ViewWrapper<TSchema, TReturn>) => void,
    onDematerialized: () => void,
  ) {
    this.#defaultSnapshot = (query.format.singular
      ? undefined
      : emptyArray) as unknown as Smash<TReturn>;
    this.#snapshot = this.#defaultSnapshot;
    this.#onMaterialized = onMaterialized;
    this.#onDematerialized = onDematerialized;
    this.#reactInternals = new Set();
    this.#query = query;
  }

  #onData = (snap: Immutable<Smash<TReturn>>) => {
    this.#snapshot = (
      snap === undefined ? snap : deepClone(snap as ReadonlyJSONValue)
    ) as Smash<TReturn>;
    for (const internals of this.#reactInternals) {
      internals();
    }
  };

  #materializeIfNeeded = () => {
    if (this.#view) {
      return;
    }

    this.#view = this.#query.materialize();
    this.#view.addListener(this.#onData);

    this.#onMaterialized(this);
  };

  getSnapshot = () => this.#snapshot;

  subscribeReactInternals = (internals: () => void): (() => void) => {
    this.#reactInternals.add(internals);
    this.#materializeIfNeeded();
    return () => {
      this.#reactInternals.delete(internals);
      if (this.#reactInternals.size === 0) {
        this.#view?.destroy();
        this.#view = undefined;
        this.#onDematerialized();
      }
    };
  };
}
