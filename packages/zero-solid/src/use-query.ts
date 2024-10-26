import {type Accessor, createMemo, onCleanup} from 'solid-js';
import type {
  Query,
  QueryInternal,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-internal/src/mod.js';
import {solidViewFactory} from './solid-view.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): Accessor<Smash<TReturn>> {
  return createMemo(() => {
    const query = querySignal();
    const view = (query as QueryInternal<TSchema, TReturn>).materialize(
      solidViewFactory,
    );

    onCleanup(() => {
      view.destroy();
    });

    return view.data;
  });
}
