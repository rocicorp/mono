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
import {useZero} from './use-zero.tsx';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
];

export type CreateQueryOptions = {
  ttl?: TTL | undefined;
};

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

  const z = useZero();

  // Wrap in in createMemo to ensure a new view is created if the querySignal changes.
  createComputed(() => {
    const query = querySignal();
    const ttl = normalize(options)?.ttl ?? DEFAULT_TTL;
    getView(z().clientID, query, ttl, setState);
  });

  return [() => state[0][''] as HumanReadable<TReturn>, () => state[1]];
}

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

const views = new Map<string, UnknownSolidView>();

const viewRefCount = new RefCount<UnknownSolidView>();

function getView<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  clientID: string,
  query: Query<TSchema, TTable, TReturn>,
  ttl: TTL,
  setState: SetStoreFunction<State>,
): SolidView {
  const hash = query.hash() + clientID;
  let view = views.get(hash);
  if (!view) {
    view = query.materialize(createSolidView(setState), ttl);
    views.set(hash, view);
  } else {
    view.updateTTL(ttl);
  }
  viewRefCount.inc(view);

  // Use queueMicrotask to allow cleanup/create in the current microtask to
  // reuse the view.
  onCleanup(() =>
    queueMicrotask(() => {
      if (viewRefCount.dec(view)) {
        views.delete(hash);
        view.destroy();
      }
    }),
  );
  return view;
}

function normalize<T>(
  options?: T | Accessor<T | undefined> | undefined,
): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
