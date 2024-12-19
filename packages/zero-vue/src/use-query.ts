import type {
  Query,
  AdvancedQuery,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {ref, watch, type Ref} from 'vue';
import {vueViewFactory} from './vue-view.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(queryGetter: () => Query<TSchema, TReturn>): Ref<Smash<TReturn>> {
  // @ts-expect-error: This is a hack to initialize the ref with an undefined value
  const queryResult: Ref<Smash<TReturn>> = ref<Smash<TReturn>>();

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
    },
    {immediate: true},
  );

  if (!queryResult.value) {
    throw new Error('Query did not return a result');
  }

  return queryResult;
}
