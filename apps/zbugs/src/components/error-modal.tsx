import {useZero, useZeroConnectionState} from '@rocicorp/zero/react';
import {useCallback} from 'react';
import {Button} from './button.tsx';
import {Modal, ModalActions, ModalText} from './modal.tsx';

export function ErrorModal() {
  const zero = useZero();
  const connectionState = useZeroConnectionState();
  const isError = connectionState.name === 'error';
  const isClosed = connectionState.name === 'closed';

  const isClientNotFound =
    isError &&
    connectionState.reason.startsWith(
      'Server could not find state needed to synchronize this client.',
    );

  const handleAction = useCallback(() => {
    if (isClosed) {
      window.location.reload();
      return;
    }
    void zero.connection.connect();
  }, [isClosed, zero]);

  return (isError && !isClientNotFound) || isClosed ? (
    <Modal isOpen={true} onDismiss={() => {}}>
      <ModalText>
        {isError
          ? `A fatal error occurred with Zero - please reconnect to the sync server to continue.`
          : `The Zero instance has been closed. This shouldn't happen. Please try refreshing the page and report to the team.`}
      </ModalText>
      <ModalActions>
        <Button className="modal-confirm" onAction={handleAction} autoFocus>
          {isClosed ? 'Refresh Page' : 'Reconnect'}
        </Button>
      </ModalActions>
    </Modal>
  ) : null;
}
