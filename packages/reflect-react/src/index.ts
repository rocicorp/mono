import {useEffect, useState} from 'react';
import type {Reflect} from 'reflect-client';
import type {ClientID, MutatorDefs} from 'reflect-shared';

export function usePresence<MD extends MutatorDefs>(
  r: Reflect<MD>,
): ReadonlySet<ClientID> {
  const [presentClientIDs, setPresentClientIDs] = useState(
    new Set() as ReadonlySet<ClientID>,
  );
  useEffect(() => {
    const unsubscribe = r.subscribeToPresence(ids => {
      setPresentClientIDs(ids);
    });

    return () => {
      unsubscribe();
      setPresentClientIDs(new Set() as ReadonlySet<ClientID>);
    };
  }, [r]);

  return presentClientIDs;
}
