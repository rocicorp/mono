import {useEffect, useState} from 'react';
import type {ClientID} from 'reflect-shared/src/mod.js';

export type SubscribeToPresenceCallback = (
  presentClientIDs: ReadonlyArray<ClientID>,
) => void;

export type PresenceSubscribable = {
  subscribeToPresence(callback: SubscribeToPresenceCallback): () => void;
};

export function usePresence(
  r: PresenceSubscribable | null | undefined,
): ReadonlyArray<ClientID> {
  const [presentClientIDs, setPresentClientIDs] = useState<
    ReadonlyArray<ClientID>
  >([]);
  useEffect(() => {
    if (!r) {
      return;
    }
    const unsubscribe = r.subscribeToPresence(ids => {
      setPresentClientIDs(ids);
    });

    return () => {
      unsubscribe();
      setPresentClientIDs([]);
    };
  }, [r]);

  return presentClientIDs;
}
