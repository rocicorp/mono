import {useZeroConnectionState} from '@rocicorp/zero/react';

export function useIsOffline(): boolean {
  const connectionState = useZeroConnectionState();
  // Return true for both disconnected and error states to make the UI readonly
  return (
    connectionState.name === 'disconnected' || connectionState.name === 'error'
  );
}
