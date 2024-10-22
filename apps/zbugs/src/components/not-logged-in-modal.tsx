import {Modal, ModalActions, ModalText} from './modal.js';

export interface Props {
  onDismiss: () => void;
  isOpen: boolean;
  href?: string;
}

export function NotLoggedInModal({onDismiss, isOpen, href}: Props) {
  return (
    <Modal isOpen={isOpen} onDismiss={onDismiss} title="Not Logged In">
      <ModalText>You need to be logged in to create a new issue.</ModalText>
      <ModalActions>
        <a className="modal-confirm" href={href}>
          Login
        </a>
      </ModalActions>
    </Modal>
  );
}
