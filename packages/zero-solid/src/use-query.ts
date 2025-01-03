import {type Accessor, createMemo, onCleanup} from 'solid-js';
import type {
  Query,
  AdvancedQuery,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {solidViewFactory} from './solid-view.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';

export type QueryResultDetails = Readonly<{
  type: ResultType;
}>;

export type QueryResult<TReturn extends QueryType> = readonly [
  Smash<TReturn>,
  QueryResultDetails,
];

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): Accessor<QueryResult<TReturn>> {
  return createMemo(() => {
    const query = querySignal();
    const view = (query as AdvancedQuery<TSchema, TReturn>).materialize(
      solidViewFactory,
    );

    onCleanup(() => {
      view.destroy();
    });

    return [view.data, {type: view.resultType}] as const;
  });
}
