import {useConnectionState} from '@rocicorp/zero/react';

export function useIsOffline(): boolean {
  const connectionState = useConnectionState();

  return (
    connectionState.name === 'disconnected' || connectionState.name === 'error'
  );
}
