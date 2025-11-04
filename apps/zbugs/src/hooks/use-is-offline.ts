import {ConnectionStatus, useZeroConnectionState} from '@rocicorp/zero/react';

export function useIsOffline(): boolean {
  const connectionState = useZeroConnectionState();
  // Since we already don't allow changes when in an error state,
  // we can just check if we're in a disconnected state.
  return connectionState.name === ConnectionStatus.Disconnected;
}
