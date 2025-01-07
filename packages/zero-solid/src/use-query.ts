import {type Accessor, onCleanup} from 'solid-js';
import type {
  AdvancedQuery,
  Query,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';
import {solidViewFactory} from './solid-view.js';

export type QueryResultDetails = Readonly<{
  type: ResultType;
}>;

export type QueryResult<TReturn extends QueryType> = readonly [
  Accessor<Smash<TReturn>>,
  Accessor<QueryResultDetails>,
];

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(querySignal: () => Query<TSchema, TReturn>): QueryResult<TReturn> {
  const query = querySignal();
  const view = (query as AdvancedQuery<TSchema, TReturn>).materialize(
    solidViewFactory,
  );

  onCleanup(() => {
    view.destroy();
  });

  return [() => view.data, () => ({type: view.resultType})];
}
