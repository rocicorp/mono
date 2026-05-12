import {useConnectionState} from '../../shared/zero-hooks.ts';

export function useIsOffline(): boolean {
  const connectionState = useConnectionState();

  return (
    connectionState.name === 'disconnected' || connectionState.name === 'error'
  );
}
