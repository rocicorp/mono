import type {
  Query,
  AdvancedQuery,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {readonly, ref, watch, type DeepReadonly, type Ref} from 'vue';
import {vueViewFactory} from './vue-view.js';
import type {ResultType} from '../../zql/src/query/typed-view.js';

export type QueryResultDetails = Readonly<{
  type: ResultType;
}>;

export type QueryResult<TReturn extends QueryType> = readonly [
  Ref<Smash<TReturn>>,
  Ref<QueryResultDetails>,
];

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  queryGetter: () => Query<TSchema, TReturn>,
): DeepReadonly<QueryResult<TReturn>> {
  const queryResult: Ref<Smash<TReturn> | undefined> = ref();
  const details = ref<QueryResultDetails>({type: 'unknown'});

  watch(
    queryGetter,
    (query, _, onCleanup) => {
      const view = (query as AdvancedQuery<TSchema, TReturn>).materialize(
        vueViewFactory,
      );

      onCleanup(() => {
        view.destroy();
      });

      queryResult.value = view.data;
      details.value = view.details;
    },
    {immediate: true},
  );

  return [readonly(queryResult as Ref<Smash<TReturn>>), details];
}
