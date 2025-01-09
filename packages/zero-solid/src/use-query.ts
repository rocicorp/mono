import {createMemo, onCleanup, type Accessor} from 'solid-js';
import type {
  AdvancedQuery,
  Query,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {solidViewFactory, type QueryResultDetails} from './solid-view.js';

export type QueryResult<TReturn extends QueryType> = readonly [
  Accessor<Smash<TReturn>>,
  Accessor<QueryResultDetails>,
];

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): QueryResult<TReturn> {
  // Wrap in in createMemo to ensure a new view is created if the querySignal changes.
  const view = createMemo(() => {
    const query = querySignal();
    const view = (query as AdvancedQuery<TSchema, TReturn>).materialize(
      solidViewFactory,
    );

    onCleanup(() => {
      view.destroy();
    });
    return view;
  });

  return [() => view().data, () => view().resultDetails];
}
