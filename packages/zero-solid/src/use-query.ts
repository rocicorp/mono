import {type Accessor, createMemo, onCleanup} from 'solid-js';
import type {
  Query,
  AdvancedQuery,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {solidViewFactory} from './solid-view.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): Accessor<Smash<TReturn>> {
  return createMemo(() => {
    const query = querySignal();
    const view = (query as AdvancedQuery<TSchema, TReturn>).materialize(
      solidViewFactory,
    );

    onCleanup(() => {
      view.destroy();
    });

    return view.data;
  });
}
