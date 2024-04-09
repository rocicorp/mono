import {EntityQuery, makeReplicacheContext} from '@rocicorp/zql/src/index.js';
import type {ReplicacheLike} from '@rocicorp/zql/src/replicache-like.js';
import type {Context} from '@rocicorp/zql/src/zql/context/context.js';
import type {FromSet} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {useEffect, useMemo, useRef, useState} from 'react';

const contextsMap = new Map<ReplicacheLike, Context>();

export function getContext(rep: ReplicacheLike): Context {
  const existing = contextsMap.get(rep);
  if (existing) {
    return existing;
  }
  const ctx = makeReplicacheContext(rep);
  contextsMap.set(rep, ctx);
  return ctx;
}

export function useQuery<From extends FromSet, Return>(
  q: EntityQuery<From, Return>,
  dependencies: unknown[] = [],
): Return {
  const [snapshot, setSnapshot] = useState([] as Return);
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const statement = useMemo(() => q.prepare(), dependencies);
  const unsubscribe = useRef<() => void>(() => {});

  useEffect(() => {
    let unloaded = false;
    void statement.exec().then(v => {
      if (!unloaded) {
        setSnapshot(v as Return);
        unsubscribe.current = statement.subscribe(v =>
          setSnapshot(v as Return),
        );
      }
    });
    return () => {
      unloaded = true;
      unsubscribe.current();
      statement.destroy();
    };
  }, [statement]);

  return snapshot;
}

export function getQuery<From extends FromSet>(
  zero: ReplicacheLike,
  name: string,
): EntityQuery<From> {
  const context = getContext(zero);
  return new EntityQuery<From>(context, name);
}
