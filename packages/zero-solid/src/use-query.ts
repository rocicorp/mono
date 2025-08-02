import {createComputed, onCleanup, type Accessor} from 'solid-js';
import {createStore} from 'solid-js/store';
import {
  type ClientID,
  type HumanReadable,
  type Query,
  type Schema,
  type TTL,
} from '../../zero/src/zero.ts';
import {DEFAULT_TTL_MS} from '../../zql/src/query/ttl.ts';
import {
  createSolidViewFactory,
  UNKNOWN,
  type QueryResultDetails,
  type SolidView,
  type State,
} from './solid-view.ts';
import {useZero} from './use-zero.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
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

  let view: SolidView | undefined = undefined;
  // Wrap in in createComputed to ensure a new view is created if the querySignal changes.
  createComputed<
    [
      SolidView | undefined,
      ClientID | undefined,
      Query<TSchema, TTable, TReturn> | undefined,
      string | undefined,
      TTL | undefined,
    ]
  >(
    ([prevView, prevClientID, prevQuery, prevQueryHash, prevTtl]) => {
      const {clientID} = useZero()();
      const query = querySignal();
      const queryHash = query.hash();
      const ttl = normalize(options)?.ttl ?? DEFAULT_TTL_MS;
      if (
        !prevView ||
        clientID !== prevClientID ||
        (query !== prevQuery &&
          (clientID === undefined || query.hash() !== prevQueryHash))
      ) {
        if (prevView) {
          prevView.destroy();
        }
        view = query.materialize(createSolidViewFactory(setState), ttl);
      } else {
        view = prevView;
        if (ttl !== prevTtl) {
          view.updateTTL(ttl);
        }
      }

      return [view, clientID, query, queryHash, ttl];
    },
    [undefined, undefined, undefined, undefined, undefined],
  );

  onCleanup(() => {
    view?.destroy();
  });

  return [() => state[0][''] as HumanReadable<TReturn>, () => state[1]];
}

function normalize<T>(
  options?: T | Accessor<T | undefined> | undefined,
): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
