import {useCallback} from 'react';
import {Modal, ModalActions, ModalText} from './modal.tsx';
import {Button} from './button.tsx';
import {useZero} from '../hooks/use-zero.ts';
import {ConnectionStatus, type ConnectionState} from '@rocicorp/zero';

export interface Props {
  connectionState: ConnectionState;
}

export function ErrorModal({connectionState}: Props) {
  const zero = useZero();
  const retry = useCallback(() => {
    void zero.connection.connect();
  }, [zero]);

  return (
    <Modal isOpen={true} onDismiss={() => {}}>
      <ModalText>
        {connectionState.name === ConnectionStatus.Error
          ? `An error occurred while connecting to the Zero server. Please try reconnecting.`
          : `The Zero instance has been closed. This shouldn't happen. Please try refreshing the page and report to the team.`}
      </ModalText>
      <ModalActions>
        <Button className="modal-confirm" onAction={retry} autoFocus>
          Reconnect
        </Button>
      </ModalActions>
    </Modal>
  );
}
