import {createMemo, onCleanup, type Accessor} from 'solid-js';
import type {
  AdvancedQuery,
  HumanReadable,
  Query,
} from '../../zero-advanced/src/mod.ts';
import type {Schema} from '../../zero-schema/src/mod.ts';
import {solidViewFactory, type QueryResultDetails} from './solid-view.ts';

export type QueryResult<TReturn> = readonly [
  Accessor<HumanReadable<TReturn>>,
  Accessor<QueryResultDetails>,
];

export function useQuery<
  TSchema extends Schema,
  TTable extends keyof TSchema['tables'] & string,
  TReturn,
>(querySignal: () => Query<TSchema, TTable, TReturn>): QueryResult<TReturn> {
  // Wrap in in createMemo to ensure a new view is created if the querySignal changes.
  const view = createMemo(() => {
    const query = querySignal();
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
