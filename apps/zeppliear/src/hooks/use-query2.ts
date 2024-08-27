import {useLayoutEffect, useState} from 'react';
import type {Schema} from 'zql/src/zql/query2/schema.js';
import type {Query, QueryResultRow, Smash} from 'zql/src/zql/query2/query.js';
import {TypedView} from 'zql/src/zql/query2/typed-view.js';
import {deepClone} from 'shared/src/deep-clone.js';

export function useQuery<
  TSchema extends Schema,
  TReturn extends Array<QueryResultRow>,
>(
  q: Query<TSchema, TReturn>,
  dependencies: readonly unknown[] = [],
  enabled = true,
): Smash<TReturn> {
  const [snapshot, setSnapshot] = useState<Smash<TReturn>>([]);
  const [, setView] = useState<TypedView<Smash<TReturn>> | undefined>(
    undefined,
  );

  useLayoutEffect(() => {
    if (enabled) {
      const view = q.materialize();
      setView(view);
      const unsubscribe = view.addListener(snapshot => {
        setSnapshot(deepClone(snapshot) as Smash<TReturn>);
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

  return snapshot;
}
