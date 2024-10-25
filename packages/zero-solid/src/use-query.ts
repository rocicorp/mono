import {type Accessor, createMemo, onCleanup} from 'solid-js';
import type {TableSchema} from '../../zql/src/zql/query/schema.js';
import type {Query, QueryType, Smash} from '../../zql/src/zql/query/query.js';
import {solidViewFactory} from './solid-view.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): Accessor<Smash<TReturn>> {
  return createMemo(() => {
    const query = querySignal();
    const view = query.materialize(solidViewFactory);

    onCleanup(() => {
      view.destroy();
    });

    return view.data;
  });
}
