import {useCallback} from 'react';
import {Modal, ModalActions, ModalText} from './modal.tsx';
import {Button} from './button.tsx';
import {useZero} from '../hooks/use-zero.ts';
import {ConnectionStatus} from '@rocicorp/zero';
import {useZeroConnectionState} from '@rocicorp/zero/react';

export function ErrorModal() {
  const zero = useZero();
  const connectionState = useZeroConnectionState();
  const isError = connectionState.name === ConnectionStatus.Error;
  const isClosed = connectionState.name === ConnectionStatus.Closed;
  const handleAction = useCallback(() => {
    if (isClosed) {
      window.location.reload();
      return;
    }
    void zero.connection.connect();
  }, [isClosed, zero]);

  return isError || isClosed ? (
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
