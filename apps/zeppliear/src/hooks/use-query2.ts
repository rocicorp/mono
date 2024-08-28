import {useLayoutEffect, useState} from 'react';
import type {Schema} from 'zql/src/zql/query2/schema.js';
import type {Query, QueryResultRow, Smash} from 'zql/src/zql/query2/query.js';
import {TypedView} from 'zql/src/zql/query2/typed-view.js';
import {deepClone} from 'shared/src/deep-clone.js';
import {ResultType} from 'zero-client';

export function useQueryWithResultType<
  TSchema extends Schema,
  TReturn extends Array<QueryResultRow>,
>(
  q: Query<TSchema, TReturn>,
  dependencies: readonly unknown[] = [],
  enabled = true,
): {snapshot: Smash<TReturn>; resultType: ResultType} {
  const [snapshot, setSnapshot] = useState<Smash<TReturn>>([]);
  const [resultType, setResultType] = useState<ResultType>('none');
  const [, setView] = useState<TypedView<Smash<TReturn>> | undefined>(
    undefined,
  );

  useLayoutEffect(() => {
    if (enabled) {
      const view = q.materialize();
      setView(view);
      const unsubscribe = view.addListener((snapshot, resultType) => {
        setSnapshot(deepClone(snapshot) as Smash<TReturn>);
        setResultType(resultType);
      });
      view.hydrate();
      return () => {
        unsubscribe();
        view.destroy();
      };
    }
    return () => {
      //
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies);

  return {snapshot, resultType};
}

export function useQuery<
  TSchema extends Schema,
  TReturn extends Array<QueryResultRow>,
>(
  q: Query<TSchema, TReturn>,
  dependencies: readonly unknown[] = [],
  enabled = true,
): Smash<TReturn> {
  return useQueryWithResultType(q, dependencies, enabled).snapshot;
}
