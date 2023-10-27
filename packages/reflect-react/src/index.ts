import {useEffect, useState} from 'react';
import type {ClientID} from 'reflect-shared';

export type SubscribeToPresenceCallback = (
  presentClientIDs: ReadonlySet<ClientID>,
) => void;

export type PresenceSubscribable = {
  subscribeToPresence(callback: SubscribeToPresenceCallback): () => void;
};

export function usePresence(
  r: PresenceSubscribable | null | undefined,
): ReadonlySet<ClientID> {
  const [presentClientIDs, setPresentClientIDs] = useState(
    new Set() as ReadonlySet<ClientID>,
  );
  useEffect(() => {
    if (!r) {
      return;
    }
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
