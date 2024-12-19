import type {
  Query,
  AdvancedQuery,
  QueryType,
  Smash,
  TableSchema,
} from '../../zero-advanced/src/mod.js';
import {readonly, ref, watch, type DeepReadonly, type Ref} from 'vue';
import {vueViewFactory} from './vue-view.js';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(
  queryGetter: () => Query<TSchema, TReturn>,
): DeepReadonly<Ref<Smash<TReturn>>> {
  const queryResult: Ref<Smash<TReturn> | undefined> = ref();

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

  return readonly(queryResult as Ref<Smash<TReturn>>);
}
