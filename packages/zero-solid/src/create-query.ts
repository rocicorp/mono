import {createComputed, onCleanup, type Accessor} from 'solid-js';
import {RefCount} from '../../shared/src/ref-count.ts';
import {
  DEFAULT_TTL,
  type HumanReadable,
  type Query,
  type Schema,
  type TTL,
} from '../../zero/src/zero.ts';
import {
  createSolidView,
  unknown,
  type State,
  type QueryResultDetails,
  type SolidView,
} from './solid-view.ts';
import {createStore, type SetStoreFunction} from 'solid-js/store';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
];

export type CreateQueryOptions = {
  ttl?: TTL | undefined;
};

// Deprecated in 0.19
/**
 * @deprecated Use {@linkcode CreateQueryOptions} instead.
 */
export type UseQueryOptions = {
  ttl?: TTL | undefined;
};

export function createQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  querySignal: () => Query<TSchema, TTable, TReturn>,
  options?: CreateQueryOptions | Accessor<CreateQueryOptions>,
): QueryResult<TReturn> {
  const [state, setState] = createStore<State>([
    {
      '': undefined,
    },
    unknown,
  ]);

  // Wrap in in createMemo to ensure a new view is created if the querySignal changes.
  createComputed(() => {
    const query = querySignal();
    const ttl = normalize(options)?.ttl ?? DEFAULT_TTL;
    const view = getView(query, ttl, setState);

    // Use queueMicrotask to allow cleanup/create in the current microtask to
    // reuse the view.
    onCleanup(() => queueMicrotask(() => releaseView(query, view)));
    return view;
  });

  return [() => state[0][''] as HumanReadable<TReturn>, () => state[1]];
}

// Deprecated in 0.19
/**
 * @deprecated Use {@linkcode createQuery} instead.
 */
export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  querySignal: () => Query<TSchema, TTable, TReturn>,
  options?: CreateQueryOptions | Accessor<CreateQueryOptions>,
): QueryResult<TReturn> {
  return createQuery(querySignal, options);
}

type UnknownSolidView = SolidView;

const views = new WeakMap<WeakKey, UnknownSolidView>();

const viewRefCount = new RefCount<UnknownSolidView>();

function getView<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: Query<TSchema, TTable, TReturn>,
  ttl: TTL,
  setState: SetStoreFunction<State>,
): SolidView {
  // TODO(arv): Use the hash of the query instead of the query object itself... but
  // we need the clientID to do that in a reasonable way.
  let view = views.get(query);
  if (!view) {
    view = query.materialize(createSolidView(setState), ttl);
    views.set(query, view);
  } else {
    view.updateTTL(ttl);
  }
  viewRefCount.inc(view);
  return view;
}

function releaseView(query: WeakKey, view: UnknownSolidView) {
  if (viewRefCount.dec(view)) {
    views.delete(query);
    view.destroy();
  }
}

function normalize<T>(
  options?: T | Accessor<T | undefined> | undefined,
): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
