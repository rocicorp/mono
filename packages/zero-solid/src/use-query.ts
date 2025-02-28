import {createMemo, onCleanup, type Accessor} from 'solid-js';
import type {
  AdvancedQuery,
  HumanReadable,
  Query,
} from '../../zero/src/advanced.ts';
import type {Schema} from '../../zero/src/zero.ts';
import {solidViewFactory, type QueryResultDetails} from './solid-view.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
];

export type UseQueryOptions = {
  ttl?: number | undefined;
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
    const ttl = normalize(options)?.ttl;
    let query = querySignal() as AdvancedQuery<TSchema, TTable, TReturn>;
    if (ttl !== undefined) {
      query = query.withTTL(ttl);
    }

    const view = (query as AdvancedQuery<TSchema, TTable, TReturn>).materialize(
      solidViewFactory,
    );

    onCleanup(() => {
      view.destroy();
    });
    return view;
  });

  return [() => view().data, () => view().resultDetails];
}

function normalize<T>(
  options?: T | Accessor<T | undefined> | undefined,
): T | undefined {
  return typeof options === 'function' ? (options as Accessor<T>)() : options;
}
