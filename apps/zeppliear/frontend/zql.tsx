import type {FromSet} from '@rocicorp/zql/src/zql/query/entity-query.js';
import {useEffect, useState} from 'react';
import type {EntityQuery} from 'zero-client';

export function useQuery<From extends FromSet, Return>(
  q: EntityQuery<From, Return>,
  dependencies: readonly unknown[] = [],
): Return {
  const [snapshot, setSnapshot] = useState([] as Return);

  useEffect(() => {
    const statement = q.prepare();
    void statement.exec().then(v => setSnapshot(v as Return));
    const unsubscribe = statement.subscribe(v => setSnapshot(v as Return));
    return () => {
      unsubscribe();
      statement.destroy();
    };

    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, dependencies);

  return snapshot;
}
