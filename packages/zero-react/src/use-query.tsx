import {useLayoutEffect, useState} from 'react';
import type {Schema} from 'zql/src/zql/query/schema.js';
import type {Query, QueryResultRow, Smash} from 'zql/src/zql/query/query.js';
import {TypedView} from 'zql/src/zql/query/typed-view.js';
import {deepClone} from 'shared/src/deep-clone.js';

export function useQuery<
  TSchema extends Schema,
  TReturn extends Array<QueryResultRow>,
>(q: Query<TSchema, TReturn> | undefined | false): Smash<TReturn> {
  const [snapshot, setSnapshot] = useState<Smash<TReturn>>([]);
  const [, setView] = useState<TypedView<Smash<TReturn>> | undefined>(
    undefined,
  );

  useLayoutEffect(() => {
    if (q) {
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
  }, [JSON.stringify(q ? q.ast : null)]);

  return snapshot;
}
