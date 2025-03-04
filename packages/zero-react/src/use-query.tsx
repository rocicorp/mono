import {useSyncExternalStore} from 'react';
import {deepClone} from '../../shared/src/deep-clone.ts';
import type {Immutable} from '../../shared/src/immutable.ts';
import type {ReadonlyJSONValue} from '../../shared/src/json.ts';
import type {Schema} from '../../zero-schema/src/builder/schema-builder.ts';
import type {AdvancedQuery} from '../../zql/src/query/query-internal.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import type {ResultType, TypedView} from '../../zql/src/query/typed-view.ts';
import {useZero} from './use-zero.tsx';

export type QueryResultDetails = Readonly<{
  type: ResultType;
}>;

export type QueryResult<TReturn> = readonly [
  HumanReadable<TReturn>,
  QueryResultDetails,
];

export type UseQueryOptions = {
  enabled?: boolean | undefined;
  /**
   * Time to live (TTL) in seconds. Controls how long query results are cached
   * after the query is removed. During this time, Zero continues to sync the query.
   * Default is 10 seconds.
   */
  ttl?: number | undefined;
};

const DEFAULT_TTL = 10_000;

export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: Query<TSchema, TTable, TReturn>,
  options?: UseQueryOptions | boolean,
): QueryResult<TReturn> {
  let enabled = true;
  let ttl: number | undefined;
  if (typeof options === 'boolean') {
    enabled = options;
  } else if (options) {
    ({enabled = true, ttl = DEFAULT_TTL} = options);
  }
  const advancedQuery = query as AdvancedQuery<TSchema, TTable, TReturn>;

  const z = useZero();
  const view = viewStore.getView(z.clientID, advancedQuery, enabled, ttl);
  // https://react.dev/reference/react/useSyncExternalStore
  return useSyncExternalStore(
    view.subscribeReactInternals,
    view.getSnapshot,
    view.getSnapshot,
  );
}

const emptyArray: unknown[] = [];
const disabledSubscriber = () => () => {};

const resultTypeUnknown = {type: 'unknown'} as const;
const resultTypeComplete = {type: 'complete'} as const;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;

function getDefaultSnapshot<TReturn>(singular: boolean): QueryResult<TReturn> {
  return (
    singular ? emptySnapshotSingularUnknown : emptySnapshotPluralUnknown
  ) as QueryResult<TReturn>;
}

/**
 * Returns a new snapshot or one of the empty predefined ones. Returning the
 * predefined ones is important to prevent unnecessary re-renders in React.
 */
function getSnapshot<TReturn>(
  singular: boolean,
  data: HumanReadable<TReturn>,
  resultType: string,
): QueryResult<TReturn> {
  if (singular && data === undefined) {
    return (resultType === 'complete'
      ? emptySnapshotSingularComplete
      : emptySnapshotSingularUnknown) as unknown as QueryResult<TReturn>;
  }

  if (!singular && (data as unknown[]).length === 0) {
    return (
      resultType === 'complete'
        ? emptySnapshotPluralComplete
        : emptySnapshotPluralUnknown
    ) as QueryResult<TReturn>;
  }

  return [
    data,
    resultType === 'complete' ? resultTypeComplete : resultTypeUnknown,
  ];
}

declare const TESTING: boolean;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type ViewWrapperAny = ViewWrapper<any, any, any>;

const allViews = new WeakMap<ViewStore, Map<string, ViewWrapperAny>>();

export function getAllViewsSizeForTesting(store: ViewStore): number {
  if (TESTING) {
    return allViews.get(store)?.size ?? 0;
  }
  return 0;
}

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
export class ViewStore {
  #views = new Map<string, ViewWrapperAny>();

  constructor() {
    if (TESTING) {
      allViews.set(this, this.#views);
    }
  }

  getView<
    TSchema extends Schema,
    TTable extends keyof TSchema['tables'] & string,
    TReturn,
  >(
    clientID: string,
    query: AdvancedQuery<TSchema, TTable, TReturn>,
    enabled: boolean,
    ttl: number | undefined,
  ): {
    getSnapshot: () => QueryResult<TReturn>;
    subscribeReactInternals: (internals: () => void) => () => void;
  } {
    if (!enabled) {
      return {
        getSnapshot: () => getDefaultSnapshot(query.format.singular),
        subscribeReactInternals: disabledSubscriber,
      };
    }

    const hash = query.hash() + clientID;
    let existing = this.#views.get(hash);
    if (!existing) {
      existing = new ViewWrapper(
        query,
        ttl,
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
      ) as ViewWrapper<TSchema, TTable, TReturn>;
      this.#views.set(hash, existing);
    }
    return existing as ViewWrapper<TSchema, TTable, TReturn>;
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
 * In strict-mode, the listener count will go to 0 then a
 * new listener for the same view is immediately added back.
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
class ViewWrapper<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
> {
  #view: TypedView<HumanReadable<TReturn>> | undefined;
  readonly #onDematerialized;
  readonly #onMaterialized;
  readonly #query: AdvancedQuery<TSchema, TTable, TReturn>;
  #snapshot: QueryResult<TReturn>;
  #reactInternals: Set<() => void>;
  readonly #ttl: number | undefined;

  constructor(
    query: AdvancedQuery<TSchema, TTable, TReturn>,
    ttl: number | undefined,
    onMaterialized: (view: ViewWrapper<TSchema, TTable, TReturn>) => void,
    onDematerialized: () => void,
  ) {
    this.#query = query;
    this.#ttl = ttl;
    this.#onMaterialized = onMaterialized;
    this.#onDematerialized = onDematerialized;
    this.#snapshot = getDefaultSnapshot(query.format.singular);
    this.#reactInternals = new Set();
    this.#materializeIfNeeded();
  }

  #onData = (
    snap: Immutable<HumanReadable<TReturn>>,
    resultType: ResultType,
  ) => {
    const data =
      snap === undefined
        ? snap
        : (deepClone(snap as ReadonlyJSONValue) as HumanReadable<TReturn>);
    this.#snapshot = getSnapshot(this.#query.format.singular, data, resultType);
    for (const internals of this.#reactInternals) {
      internals();
    }
  };

  #materializeIfNeeded = () => {
    if (this.#view) {
      return;
    }

    this.#view = this.#query.materialize(this.#ttl);
    this.#view.addListener(this.#onData);

    this.#onMaterialized(this);
  };

  getSnapshot = () => this.#snapshot;

  subscribeReactInternals = (internals: () => void): (() => void) => {
    this.#reactInternals.add(internals);
    this.#materializeIfNeeded();
    return () => {
      this.#reactInternals.delete(internals);

      // only schedule a cleanup task if we have no listeners left
      if (this.#reactInternals.size === 0) {
        setTimeout(() => {
          // Someone re-registered a listener on this view before the timeout elapsed.
          // This happens often in strict-mode which forces a component
          // to mount, unmount, remount.
          if (this.#reactInternals.size > 0) {
            return;
          }
          // We already destroyed the view
          if (this.#view === undefined) {
            return;
          }
          this.#view?.destroy();
          this.#view = undefined;
          this.#onDematerialized();
        }, 10);
      }
    };
  };
}
