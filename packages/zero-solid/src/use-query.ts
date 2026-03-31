import {
  createEffect,
  createMemo,
  createSignal,
  on,
  onCleanup,
  untrack,
  type Accessor,
} from 'solid-js';
import {createStore} from 'solid-js/store';
import {
  addContextToQuery,
  asQueryInternals,
  DEFAULT_TTL_MS,
} from './bindings.ts';
import {createSolidViewFactory, UNKNOWN, type State} from './solid-view.ts';
import {useZero} from './use-zero.ts';
import {
  type AnyQueryOrQueryRequest,
  type Falsy,
  type HumanReadable,
  type QueryContext,
  type QueryReturn,
  type QueryResultDetails,
  type QuerySchema,
  type TTL,
} from './zero.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails & {}>,
];

/**
 * Result type for "maybe queries" - queries that may be falsy.
 * The data value can be undefined when the query is falsy/disabled.
 */
export type MaybeQueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn> | undefined>,
  Accessor<QueryResultDetails & {}>,
];

type QueryResultFor<TQuery extends AnyQueryOrQueryRequest> = QueryResult<
  QueryReturn<TQuery>
>;

type MaybeQueryResultFor<TQuery extends AnyQueryOrQueryRequest> =
  MaybeQueryResult<QueryReturn<TQuery>>;

// Deprecated in 0.22
/**
 * @deprecated Use {@linkcode UseQueryOptions} instead.
 */
export type CreateQueryOptions = {
  ttl?: TTL | undefined;
};

export type UseQueryOptions = {
  ttl?: TTL | undefined;
};

// Deprecated in 0.22
/**
 * @deprecated Use {@linkcode useQuery} instead.
 */
export function createQuery<TQuery extends AnyQueryOrQueryRequest>(
  querySignal: Accessor<TQuery>,
  options?: CreateQueryOptions | Accessor<CreateQueryOptions>,
): QueryResultFor<TQuery> {
  return useQuery(querySignal, options);
}

// Overload 1: Query - returns QueryResult<TReturn>
export function useQuery<TQuery extends AnyQueryOrQueryRequest>(
  querySignal: Accessor<TQuery>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): QueryResultFor<TQuery>;

// Overload 2: Maybe query
export function useQuery<TQuery extends AnyQueryOrQueryRequest>(
  querySignal: Accessor<TQuery | Falsy>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): MaybeQueryResultFor<TQuery>;

// Implementation
export function useQuery<TQuery extends AnyQueryOrQueryRequest>(
  querySignal: Accessor<TQuery | Falsy>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): QueryResultFor<TQuery> | MaybeQueryResultFor<TQuery> {
  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    UNKNOWN,
  ]);
  const initialRefetchKey = 0;
  const [refetchKey, setRefetchKey] = createSignal(initialRefetchKey);

  const refetch = () => {
    setRefetchKey(k => k + 1);
  };

  const zero = useZero<QuerySchema<TQuery>, undefined, QueryContext<TQuery>>();

  // Handle possibly falsy queries
  const q = createMemo(() => {
    const query = querySignal();
    if (!query) return undefined;
    return addContextToQuery(query, zero().context);
  });

  const qi = createMemo(() => {
    const query = q();
    if (!query) return undefined;
    return asQueryInternals(query);
  });

  const hash = createMemo(() => qi()?.hash());
  const ttl = createMemo(() => normalize(options)?.ttl ?? DEFAULT_TTL_MS);

  const initialTTL = ttl();

  const view = createMemo(() => {
    // Depend on hash instead of query to avoid recreating the view when the
    // query object changes but the hash is the same.
    const currentHash = hash();
    refetchKey();

    // If query is falsy, don't create a view and reset state to undefined
    if (currentHash === undefined) {
      setState([{'': undefined}, UNKNOWN]);
      return undefined;
    }

    const untrackedQuery = untrack(q);
    if (!untrackedQuery) {
      setState([{'': undefined}, UNKNOWN]);
      return undefined;
    }

    const v = zero().materialize(
      untrackedQuery,
      createSolidViewFactory(setState, refetch),
      {
        ttl: initialTTL,
      },
    );

    onCleanup(() => v.destroy());

    return v;
  });

  // Update TTL on existing view when it changes.
  createEffect(
    on(
      ttl,
      currentTTL => {
        view()?.updateTTL(currentTTL);
      },
      {defer: true},
    ),
  );

  return [
    () => state[0][''] as HumanReadable<QueryReturn<TQuery>>,
    () => state[1],
  ];
}

function normalize<T>(options?: T | Accessor<T | undefined>): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
