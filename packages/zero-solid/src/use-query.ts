import {createMemo, onCleanup, type Accessor} from 'solid-js';
import type {
  AdvancedQuery,
  HumanReadable,
  Query,
} from '../../zero/src/advanced.ts';
import {DEFAULT_TTL, type Schema, type TTL} from '../../zero/src/zero.ts';
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
    const query = querySignal() as AdvancedQuery<TSchema, TTable, TReturn>;
    const ttl = normalize(options)?.ttl ?? DEFAULT_TTL;
    const view = getView(query, ttl);

    onCleanup(() => {
      view.destroy();
    });
    return view;
  });

  return [() => view().data, () => view().resultDetails];
}

const views = new WeakMap<object, SolidView<HumanReadable<unknown>>>();

function getView<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(
  query: AdvancedQuery<TSchema, TTable, TReturn>,
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
  return view as SolidView<HumanReadable<TReturn>>;
}

function normalize<T>(
  options?: T | Accessor<T | undefined> | undefined,
): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
