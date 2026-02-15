import {resolver} from '@rocicorp/resolver';
import React, {useSyncExternalStore} from 'react';
import {deepEqual} from 'shared/src/json.ts';
import {
  type Immutable,
  addContextToQuery,
  asQueryInternals,
  DEFAULT_TTL_MS,
} from './bindings.ts';
import {useZero} from './zero-provider.tsx';
import type {
  AnyMutatorRegistry,
  CustomMutatorDefs,
  DefaultContext,
  DefaultSchema,
  ErroredQuery,
  Falsy,
  HumanReadable,
  PullRow,
  Query,
  QueryErrorDetails,
  QueryOrQueryRequest,
  QueryResultDetails,
  ReadonlyJSONValue,
  ResultType,
  Schema,
  TTL,
  TypedView,
  Zero,
} from './zero.ts';

export type QueryResult<TReturn> = readonly [
  HumanReadable<TReturn>,
  QueryResultDetails & {},
];

/**
 * Result type for "maybe queries" - queries that may be falsy.
 * The data value can be undefined when the query is falsy/disabled.
 */
export type MaybeQueryResult<TReturn> = readonly [
  HumanReadable<TReturn> | undefined,
  QueryResultDetails & {},
];

export type UseQueryOptions = {
  enabled?: boolean | undefined;
  /**
   * Time to live (TTL) in seconds. Controls how long query results are cached
   * after the query is removed. During this time, Zero continues to sync the query.
   * Default is 'never'.
   */
  ttl?: TTL | undefined;
};

/**
 * Options for useQuery with a select function that transforms query results.
 * When select is provided, the hook uses deep equality comparison for re-renders.
 */
export type UseQueryOptionsWithSelect<TReturn, TSelected> = UseQueryOptions & {
  /**
   * Transform function that derives a value from the query result.
   * The hook will only re-render when the selected value changes (deep equality).
   * This encourages a pattern of large top-down queries with selective subscriptions.
   *
   * @param data - The query result (may be undefined during loading or for .one() queries)
   * @returns The derived value to subscribe to
   *
   * @example
   * ```typescript
   * // Only re-renders when student IDs change
   * const studentIds = useQuery(
   *   z.query.school({id: schoolId}),
   *   {select: (school) => school?.students.map(s => s.id) ?? []}
   * );
   * ```
   */
  select: (data: HumanReadable<TReturn>) => TSelected;
};

/**
 * Result type for queries with a select function.
 */
export type SelectedQueryResult<TSelected> = readonly [
  TSelected,
  QueryResultDetails & {},
];

/**
 * Result type for "maybe queries" with a select function.
 * Used when the query may be falsy/disabled.
 */
export type MaybeSelectedQueryResult<TSelected> = readonly [
  TSelected | undefined,
  QueryResultDetails & {},
];

export type UseSuspenseQueryOptions = UseQueryOptions & {
  /**
   * Whether to suspend until:
   * - 'partial': the query has partial results (partial array or defined
   *   value for singular results) which may be of result type 'unknown',
   *   or the query result type is 'complete' (in which case results may be
   *   empty).  This is useful for suspending until there are partial
   *   optimistic local results, or the query has completed loading from the
   *   server.
   * - 'complete': the query result type is 'complete'.
   *
   * Default is 'partial'.
   */
  suspendUntil?: 'complete' | 'partial';
};

const reactUse = (React as {use?: (p: Promise<unknown>) => void}).use;
const suspend: (p: Promise<unknown>) => void = reactUse
  ? reactUse
  : p => {
      throw p;
    };

// Overload 1: Query with select
export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
  TSelected = unknown,
>(
  query: QueryOrQueryRequest<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  options: UseQueryOptionsWithSelect<TReturn, TSelected>,
): SelectedQueryResult<TSelected>;

// Overload 2: Maybe query with select
export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
  TSelected = unknown,
>(
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
  options: UseQueryOptionsWithSelect<TReturn, TSelected>,
): MaybeSelectedQueryResult<TSelected>;

// Overload 3: Query without select
export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query: QueryOrQueryRequest<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  options?: UseQueryOptions | boolean,
): QueryResult<TReturn>;

// Overload 4: Maybe query without select
export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
  options?: UseQueryOptions | boolean,
): MaybeQueryResult<TReturn>;

// Implementation
export function useQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
  TSelected = unknown,
>(
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
  options?: UseQueryOptions | UseQueryOptionsWithSelect<TReturn, TSelected> | boolean,
): QueryResult<TReturn> | MaybeQueryResult<TReturn> | SelectedQueryResult<TSelected> | MaybeSelectedQueryResult<TSelected> {
  let enabled = true;
  let ttl: TTL = DEFAULT_TTL_MS;
  let select: ((data: HumanReadable<TReturn>) => TSelected) | undefined;
  if (typeof options === 'boolean') {
    enabled = options;
  } else if (options) {
    ({enabled = true, ttl = DEFAULT_TTL_MS} = options);
    if ('select' in options) {
      select = options.select;
    }
  }

  const zero = useZero<TSchema, undefined, TContext>();

  // When query is falsy, use disabled subscriber/snapshot to maintain hook order
  const q = query ? addContextToQuery(query, zero.context) : undefined;
  const view = q ? viewStore.getView(zero, q, enabled, ttl) : undefined;

  // Track the previous selected value for deep equality comparison.
  // This ref persists across renders and allows us to return the same
  // snapshot reference when the selected value hasn't changed.
  const selectStateRef = React.useRef<{
    lastSelected: TSelected | undefined;
    lastDetails: QueryResultDetails | undefined;
    lastSnapshot: SelectedQueryResult<TSelected> | MaybeSelectedQueryResult<TSelected> | undefined;
  }>({lastSelected: undefined, lastDetails: undefined, lastSnapshot: undefined});

  // Create a wrapper getSnapshot that applies select and deep equality.
  // This callback is only used when select is defined.
  const getSnapshotWithSelect = React.useCallback((): SelectedQueryResult<TSelected> | MaybeSelectedQueryResult<TSelected> => {
    const baseSnapshot = view?.getSnapshot() ?? getDisabledSnapshot();
    // Data is HumanReadable<TReturn> | undefined. For singular queries,
    // HumanReadable<TReturn> already includes undefined. For plural queries
    // with disabled view, we pass undefined to select (callers should handle this).
    const data = baseSnapshot[0];
    const details = baseSnapshot[1];

    // TypeScript can't narrow select here since it's from a closure, so we
    // assert it exists (this callback is only used when select is truthy).
    const selected = select!(data as HumanReadable<TReturn>);

    // Use deep equality to determine if we should return a new snapshot.
    // Cast to ReadonlyJSONValue since selected values derived from Zero queries
    // are always JSON-serializable.
    // IMPORTANT: We must also re-render if the details/status changed, even if
    // the selected value is the same.
    const state = selectStateRef.current;
    if (
      state.lastSnapshot !== undefined &&
      state.lastDetails === details &&
      deepEqual(
        state.lastSelected as ReadonlyJSONValue | undefined,
        selected as ReadonlyJSONValue | undefined,
      )
    ) {
      return state.lastSnapshot;
    }

    // Selected value or details changed, create new snapshot
    const newSnapshot: SelectedQueryResult<TSelected> = [selected, details];
    state.lastSelected = selected;
    state.lastDetails = details;
    state.lastSnapshot = newSnapshot;
    return newSnapshot;
  }, [view, select]);

  // https://react.dev/reference/react/useSyncExternalStore
  // Always call useSyncExternalStore to maintain consistent hook order.
  // When select is provided, use the wrapper that applies transformation and deep equality.
  // Otherwise, use the view's raw getSnapshot (or disabled snapshot for falsy queries).
  if (select) {
    return useSyncExternalStore(
      view?.subscribeReactInternals ?? disabledSubscriber,
      getSnapshotWithSelect,
      getSnapshotWithSelect,
    );
  }

  // Cast needed: getDisabledSnapshot returns a narrower type than QueryResult<TReturn>
  const getSnapshot = (view?.getSnapshot ?? getDisabledSnapshot) as () => QueryResult<TReturn>;
  return useSyncExternalStore(
    view?.subscribeReactInternals ?? disabledSubscriber,
    getSnapshot,
    getSnapshot,
  );
}

// Overload 1: Query
export function useSuspenseQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query: QueryOrQueryRequest<
    TTable,
    TInput,
    TOutput,
    TSchema,
    TReturn,
    TContext
  >,
  options?: UseSuspenseQueryOptions | boolean,
): QueryResult<TReturn>;

// Overload 2: Maybe query
export function useSuspenseQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
  options?: UseSuspenseQueryOptions | boolean,
): MaybeQueryResult<TReturn>;

// Implementation
export function useSuspenseQuery<
  TTable extends keyof TSchema['tables'] & string,
  TInput extends ReadonlyJSONValue | undefined,
  TOutput extends ReadonlyJSONValue | undefined,
  TSchema extends Schema = DefaultSchema,
  TReturn = PullRow<TTable, TSchema>,
  TContext = DefaultContext,
>(
  query:
    | QueryOrQueryRequest<TTable, TInput, TOutput, TSchema, TReturn, TContext>
    | Falsy,
  options?: UseSuspenseQueryOptions | boolean,
): QueryResult<TReturn> | MaybeQueryResult<TReturn> {
  let enabled = true;
  let ttl: TTL = DEFAULT_TTL_MS;
  let suspendUntil: 'complete' | 'partial' = 'partial';
  if (typeof options === 'boolean') {
    enabled = options;
  } else if (options) {
    ({
      enabled = true,
      ttl = DEFAULT_TTL_MS,
      suspendUntil = 'complete',
    } = options);
  }

  const zero = useZero<TSchema, undefined, TContext>();

  // When query is falsy, use disabled subscriber/snapshot to maintain hook order
  const q = query ? addContextToQuery(query, zero.context) : undefined;
  const view = q ? viewStore.getView(zero, q, enabled, ttl) : undefined;

  // https://react.dev/reference/react/useSyncExternalStore
  // Always call useSyncExternalStore to maintain consistent hook order
  const snapshot = useSyncExternalStore(
    view?.subscribeReactInternals ?? disabledSubscriber,
    view?.getSnapshot ??
      (getDisabledSnapshot as () => MaybeQueryResult<TReturn>),
    view?.getSnapshot ??
      (getDisabledSnapshot as () => MaybeQueryResult<TReturn>),
  );

  if (view && enabled) {
    if (suspendUntil === 'complete' && !view.complete) {
      suspend(view.waitForComplete());
    }

    if (suspendUntil === 'partial' && !view.nonEmpty) {
      suspend(view.waitForNonEmpty());
    }
  }

  return snapshot;
}

const emptyArray: unknown[] = [];
const disabledSubscriber = () => () => {};

const resultTypeUnknown = {type: 'unknown'} as const;
const resultTypeComplete = {type: 'complete'} as const;
const resultTypeError = {type: 'error'} as const;

const disabledQuerySnapshot = [undefined, resultTypeUnknown] as const;
const getDisabledSnapshot = () => disabledQuerySnapshot;

const emptySnapshotSingularUnknown = [undefined, resultTypeUnknown] as const;
const emptySnapshotSingularComplete = [undefined, resultTypeComplete] as const;
const emptySnapshotSingularErrorUnknown = [undefined, resultTypeError] as const;
const emptySnapshotPluralUnknown = [emptyArray, resultTypeUnknown] as const;
const emptySnapshotPluralComplete = [emptyArray, resultTypeComplete] as const;
const emptySnapshotErrorUnknown = [emptyArray, resultTypeError] as const;

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
  resultType: ResultType,
  retryFn: () => void,
  error?: ErroredQuery,
): QueryResult<TReturn> {
  if (singular && data === undefined) {
    switch (resultType) {
      case 'error':
        if (error) {
          return [
            undefined,
            makeError(retryFn, error),
          ] as unknown as QueryResult<TReturn>;
        }
        return emptySnapshotSingularErrorUnknown as unknown as QueryResult<TReturn>;
      case 'complete':
        return emptySnapshotSingularComplete as unknown as QueryResult<TReturn>;
      case 'unknown':
        return emptySnapshotSingularUnknown as unknown as QueryResult<TReturn>;
    }
  }

  if (!singular && (data as unknown[]).length === 0) {
    switch (resultType) {
      case 'error':
        if (error) {
          return [
            emptyArray,
            makeError(retryFn, error),
          ] as unknown as QueryResult<TReturn>;
        }
        return emptySnapshotErrorUnknown as unknown as QueryResult<TReturn>;
      case 'complete':
        return emptySnapshotPluralComplete as unknown as QueryResult<TReturn>;
      case 'unknown':
        return emptySnapshotPluralUnknown as unknown as QueryResult<TReturn>;
    }
  }

  switch (resultType) {
    case 'error':
      if (error) {
        return [data, makeError(retryFn, error)];
      }
      return [
        data,
        makeError(retryFn, {
          error: 'app',
          id: 'unknown',
          name: 'unknown',
          message: 'An unknown error occurred',
        }),
      ];
    case 'complete':
      return [data, resultTypeComplete];
    case 'unknown':
      return [data, resultTypeUnknown];
  }
}

function makeError(retry: () => void, error: ErroredQuery): QueryErrorDetails {
  const message = error.message ?? 'An unknown error occurred';
  return {
    type: 'error',
    retry,
    refetch: retry,
    error: {
      type: error.error,
      message,
      ...(error.details ? {details: error.details} : {}),
    },
  };
}

declare const TESTING: boolean;

// oxlint-disable-next-line @typescript-eslint/no-explicit-any
type AnyViewWrapper = ViewWrapper<any, any, any, any, any>;

const allViews = new WeakMap<ViewStore, Map<string, AnyViewWrapper>>();

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
  #views = new Map<string, AnyViewWrapper>();

  constructor() {
    if (TESTING) {
      allViews.set(this, this.#views);
    }
  }

  getView<
    TTable extends keyof TSchema['tables'] & string,
    TSchema extends Schema,
    TReturn,
    MD extends CustomMutatorDefs | undefined,
    TContext,
  >(
    zero: Zero<TSchema, MD, TContext>,
    q: Query<TTable, TSchema, TReturn>,
    enabled: boolean,
    ttl: TTL,
  ): {
    getSnapshot: () => QueryResult<TReturn>;
    subscribeReactInternals: (internals: () => void) => () => void;
    updateTTL: (ttl: TTL) => void;
    waitForComplete: () => Promise<void>;
    waitForNonEmpty: () => Promise<void>;
    complete: boolean;
    nonEmpty: boolean;
  } {
    const qi = asQueryInternals(q);

    if (!enabled) {
      return {
        getSnapshot: () => getDefaultSnapshot(qi.format.singular),
        subscribeReactInternals: disabledSubscriber,
        updateTTL: () => {},
        waitForComplete: () => Promise.resolve(),
        waitForNonEmpty: () => Promise.resolve(),
        complete: false,
        nonEmpty: false,
      };
    }

    const hash = qi.hash() + zero.clientID;
    let existing = this.#views.get(hash);
    if (!existing) {
      existing = new ViewWrapper(q, zero, ttl, view => {
        const currentView = this.#views.get(hash);
        if (currentView && currentView !== view) {
          // we replaced the view with a new one already.
          return;
        }
        this.#views.delete(hash);
      });
      this.#views.set(hash, existing);
    } else {
      existing.updateTTL(ttl);
    }
    return existing as ViewWrapper<TTable, TSchema, TReturn, MD, TContext>;
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
  TTable extends keyof TSchema['tables'] & string,
  TSchema extends Schema,
  TReturn,
  MD extends AnyMutatorRegistry | CustomMutatorDefs | undefined,
  TContext,
> {
  #view: TypedView<HumanReadable<TReturn>> | undefined;
  readonly #onDematerialized;
  readonly #query: Query<TTable, TSchema, TReturn>;
  #snapshot: QueryResult<TReturn>;
  readonly #reactInternals: Set<() => void> = new Set();
  #ttl: TTL;
  #complete = false;
  #completeResolver = resolver<void>();
  #nonEmpty = false;
  #nonEmptyResolver = resolver<void>();
  readonly #zero: Pick<Zero<TSchema, undefined, TContext>, 'materialize'>;
  readonly #singular: boolean;

  constructor(
    query: Query<TTable, TSchema, TReturn>,
    zero: Pick<Zero<TSchema, undefined, TContext>, 'materialize'>,
    ttl: TTL,
    onDematerialized: (
      view: ViewWrapper<TTable, TSchema, TReturn, MD, TContext>,
    ) => void,
  ) {
    this.#query = query;
    this.#zero = zero;
    this.#ttl = ttl;
    this.#onDematerialized = onDematerialized;
    const {singular} = asQueryInternals(query).format;
    this.#singular = singular;
    this.#snapshot = getDefaultSnapshot(singular);
    this.#materializeIfNeeded();
  }

  #onData = (
    snap: Immutable<HumanReadable<TReturn>>,
    resultType: ResultType,
    error?: ErroredQuery,
  ) => {
    // applyChange now returns immutable data structures, so no deep clone needed.
    // Unchanged rows preserve their object identity for React.memo optimization.
    const data = snap as HumanReadable<TReturn>;
    this.#snapshot = getSnapshot(
      this.#singular,
      data,
      resultType,
      this.#retry,
      error,
    );
    if (resultType === 'complete' || resultType === 'error') {
      this.#complete = true;
      this.#completeResolver.resolve();
      this.#nonEmpty = true;
      this.#nonEmptyResolver.resolve();
    }

    if (
      this.#singular
        ? this.#snapshot[0] !== undefined
        : (this.#snapshot[0] as unknown[]).length !== 0
    ) {
      this.#nonEmpty = true;
      this.#nonEmptyResolver.resolve();
    }

    for (const internals of this.#reactInternals) {
      internals();
    }
  };

  /**
   * Called by the user to force a retry of the query
   * in the case the query errored.
   */
  #retry = () => {
    this.#view?.destroy();
    this.#view = undefined;
    this.#materializeIfNeeded();
  };

  #materializeIfNeeded = () => {
    if (this.#view) {
      return;
    }
    this.#view = this.#zero.materialize(this.#query, {
      ttl: this.#ttl,
    });
    this.#view.addListener(this.#onData);
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
          // We already destroyed the view
          if (this.#view === undefined) {
            return;
          }

          // Someone re-registered a listener on this view before the timeout elapsed.
          // This happens often in strict-mode which forces a component
          // to mount, unmount, remount.
          if (this.#reactInternals.size > 0) {
            return;
          }

          this.#view.destroy();
          this.#view = undefined;
          this.#complete = false;
          this.#completeResolver = resolver();
          this.#nonEmpty = false;
          this.#nonEmptyResolver = resolver();
          this.#onDematerialized(this);
        }, 10);
      }
    };
  };

  updateTTL(ttl: TTL): void {
    this.#ttl = ttl;
    this.#view?.updateTTL(ttl);
  }

  get complete() {
    return this.#complete;
  }

  waitForComplete(): Promise<void> {
    return this.#completeResolver.promise;
  }

  get nonEmpty() {
    return this.#nonEmpty;
  }

  waitForNonEmpty(): Promise<void> {
    return this.#nonEmptyResolver.promise;
  }
}
