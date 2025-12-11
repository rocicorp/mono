import {useZeroConnectionState} from '@rocicorp/zero/react';

export function useIsOffline(): boolean {
  const connectionState = useZeroConnectionState();

  return (
    connectionState.name === 'disconnected' || connectionState.name === 'error'
  );
}
