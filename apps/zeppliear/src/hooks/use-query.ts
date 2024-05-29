import type {FromSet} from '@rocicorp/zql/src/zql/query/entity-query.js';
import type {ResultType} from '@rocicorp/zql/src/zql/query/statement.js';
import {useEffect, useRef, useState} from 'react';
import type {EntityQuery} from 'zero-client';
export type {ResultType} from '@rocicorp/zql/src/zql/query/statement.js';

export function useQuery<From extends FromSet, Return>(
  q: EntityQuery<From, Return>,
  dependencies: readonly unknown[] = [],
  enabled = true,
): Return {
  return useQueryWithMakeSnapshot(q, dependencies, enabled, v => v);
}

export function useQueryWithResultType<From extends FromSet, Return>(
  q: EntityQuery<From, Return>,
  dependencies: readonly unknown[] = [],
  enabled = true,
): {value: Return; resultType: ResultType} {
  return useQueryWithMakeSnapshot(
    q,
    dependencies,
    enabled,
    (value, resultType) => ({value, resultType}),
  );
}

function useQueryWithMakeSnapshot<From extends FromSet, Return, Snapshot>(
  q: EntityQuery<From, Return>,
  dependencies: readonly unknown[],
  enabled: boolean,
  makeSnapshot: (v: Return, resultType: ResultType) => Snapshot,
): Snapshot {
  const [snapshot, setSnapshot] = useState<Snapshot>(
    makeSnapshot([] as Return, 'none'),
  );
  const [lastDeps, setLastDeps] = useState<readonly unknown[] | undefined>();
  const unsubscribeRef = useRef<() => void>();
  const statementRef = useRef<ReturnType<(typeof q)['prepare']>>();

  if (
    enabled &&
    (lastDeps === undefined ||
      lastDeps.length !== dependencies.length ||
      lastDeps.some((v, i) => v !== dependencies[i]))
  ) {
    setLastDeps(dependencies);
    const statement = q.prepare();
    if (unsubscribeRef.current && statementRef.current) {
      unsubscribeRef.current();
      statementRef.current.destroy();
    }
    statementRef.current = statement;
    unsubscribeRef.current = statement.subscribe((v, resultType) => {
      setSnapshot(makeSnapshot(v as Return, resultType));
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
