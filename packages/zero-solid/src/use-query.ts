import {createComputed, createSignal, onCleanup, type Accessor} from 'solid-js';
import {createStore} from 'solid-js/store';
import type {ClientID} from '../../replicache/src/sync/ids.ts';
import {bindingsForZero} from '../../zero-client/src/client/bindings.ts';
import type {QueryResultDetails} from '../../zero-client/src/types/query-result.ts';
import type {Schema} from '../../zero-types/src/schema.ts';
import type {HumanReadable, Query} from '../../zql/src/query/query.ts';
import {DEFAULT_TTL_MS, type TTL} from '../../zql/src/query/ttl.ts';
import {
  createSolidViewFactory,
  UNKNOWN,
  type SolidView,
  type State,
} from './solid-view.ts';
import {useZero} from './use-zero.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails & {}>,
];

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
export function createQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  querySignal: Accessor<Query<TSchema, TTable, TReturn>>,
  options?: CreateQueryOptions | Accessor<CreateQueryOptions>,
): QueryResult<TReturn> {
  return useQuery(querySignal, options);
}

export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  querySignal: Accessor<Query<TSchema, TTable, TReturn>>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): QueryResult<TReturn> {
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

  let view: SolidView | undefined = undefined;

  // Wrap in in createComputed to ensure a new view is created if the querySignal changes.
  createComputed<
    [
      SolidView | undefined,
      ClientID | undefined,
      Query<TSchema, TTable, TReturn> | undefined,
      string | undefined,
      TTL | undefined,
      number,
    ]
  >(
    ([
      prevView,
      prevClientID,
      prevQuery,
      prevQueryHash,
      prevTtl,
      prevRefetchKey,
    ]) => {
      const zero = useZero()();
      const currentRefetchKey = refetchKey(); // depend on refetchKey to force re-evaluation
      const {clientID} = zero;
      const query = querySignal();
      const bindings = bindingsForZero(zero);
      const queryHash = bindings.hash(query);
      const ttl = normalize(options)?.ttl ?? DEFAULT_TTL_MS;
      if (
        !prevView ||
        clientID !== prevClientID ||
        prevRefetchKey !== currentRefetchKey ||
        (query !== prevQuery &&
          (clientID === undefined || queryHash !== prevQueryHash))
      ) {
        if (prevView) {
          prevView.destroy();
        }
        view = bindings.materialize(
          query,
          createSolidViewFactory(setState, refetch),
          {ttl},
        );
      } else {
        view = prevView;
        if (ttl !== prevTtl) {
          view.updateTTL(ttl);
        }
      }

      return [view, clientID, query, queryHash, ttl, currentRefetchKey];
    },
    [undefined, undefined, undefined, undefined, undefined, initialRefetchKey],
  );

  onCleanup(() => {
    view?.destroy();
  });

  return [() => state[0][''] as HumanReadable<TReturn>, () => state[1]];
}

function normalize<T>(options?: T | Accessor<T | undefined>): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
