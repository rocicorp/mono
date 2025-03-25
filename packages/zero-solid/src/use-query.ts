import {createMemo, onCleanup, type Accessor} from 'solid-js';
import {RefCount} from '../../shared/src/ref-count.ts';
import {
  DEFAULT_TTL,
  type HumanReadable,
  type Query,
  type Schema,
  type TTL,
} from '../../zero/src/zero.ts';
import {
  solidViewFactory,
  type QueryResultDetails,
  type SolidView,
} from './solid-view.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
];

export type UseQueryOptions = {
  ttl?: TTL | undefined;
};

export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  querySignal: () => Query<TSchema, TTable, TReturn>,
  options?: UseQueryOptions | Accessor<UseQueryOptions>,
): QueryResult<TReturn> {
  // Wrap in in createMemo to ensure a new view is created if the querySignal changes.
  const view = createMemo(() => {
    const query = querySignal();
    const ttl = normalize(options)?.ttl ?? DEFAULT_TTL;
    const view = getView(query, ttl);

    // Use queueMicrotask to allow cleanup/create in the current microtask to
    // reuse the view.
    onCleanup(() => queueMicrotask(() => releaseView(query, view)));
    return view;
  });

  return [() => view().data, () => view().resultDetails];
}

type UnknownSolidView = SolidView<HumanReadable<unknown>>;

const views = new WeakMap<WeakKey, UnknownSolidView>();

const viewRefCount = new RefCount<UnknownSolidView>();

function getView<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: Query<TSchema, TTable, TReturn>,
  ttl: TTL,
): SolidView<HumanReadable<TReturn>> {
  // TODO(arv): Use the hash of the query instead of the query object itself... but
  // we need the clientID to do that in a reasonable way.
  let view = views.get(query);
  if (!view) {
    view = query.materialize(solidViewFactory, ttl);
    views.set(query, view);
  } else {
    query.updateTTL(ttl);
  }
  viewRefCount.inc(view);
  return view as SolidView<HumanReadable<TReturn>>;
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
