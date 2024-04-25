import type {FromSet} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {useEffect, useRef, useState} from 'react';
import type {EntityQuery} from 'zero-client';

export function useQuery<From extends FromSet, Return>(
  q: EntityQuery<From, Return>,
  dependencies: readonly unknown[] = [],
): Return {
  const [snapshot, setSnapshot] = useState([] as Return);
  const [lastDeps, setLastDeps] = useState<readonly unknown[] | undefined>();
  const unsubscribeRef = useRef<() => void>();
  const statementRef = useRef<ReturnType<(typeof q)['prepare']>>();

  if (
    lastDeps === undefined ||
    lastDeps.length !== dependencies.length ||
    lastDeps.some((v, i) => v !== dependencies[i])
  ) {
    setLastDeps(dependencies);
    const statement = q.prepare();
    if (unsubscribeRef.current && statementRef.current) {
      unsubscribeRef.current();
      statementRef.current.destroy();
    }
    statementRef.current = statement;
    unsubscribeRef.current = statement.subscribe(v => {
      setSnapshot(v as Return);
    });
  }

  useEffect(
    () => () => {
      if (unsubscribeRef.current && statementRef.current) {
        unsubscribeRef.current();
        statementRef.current.destroy();
      }
    },
    [],
  );

  return snapshot;
}
