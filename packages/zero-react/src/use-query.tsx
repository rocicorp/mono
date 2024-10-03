import {useLayoutEffect, useState} from 'react';
import {deepClone} from 'shared/src/deep-clone.js';
import type {
  Query,
  QueryImpl,
  QueryType,
  Smash,
  TableSchema,
  TypedView,
} from 'zero-client';

export function useQuery<
  TSchema extends TableSchema,
  TReturn extends QueryType,
>(q: Query<TSchema, TReturn>, enable: boolean = true): Smash<TReturn> {
  // TODO: Consider exposing singular on Query? The motivation is that we do not
  // want to export QueryImpl.
  const queryImpl = q as QueryImpl<TSchema, TReturn>;

  const [snapshot, setSnapshot] = useState<Smash<TReturn>>(
    (queryImpl.singular ? undefined : []) as unknown as Smash<TReturn>,
  );
  const [, setView] = useState<TypedView<Smash<TReturn>> | undefined>(
    undefined,
  );

  useLayoutEffect(() => {
    if (enable) {
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
    setSnapshot(
      (queryImpl.singular ? undefined : []) as unknown as Smash<TReturn>,
    );
    setView(undefined);
    return () => {
      //
    };
  }, [JSON.stringify(enable ? (q as QueryImpl<never, never>).ast : null)]);

  return snapshot;
}
