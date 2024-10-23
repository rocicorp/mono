import {links} from '../routes.js';
import {Modal, ModalActions, ModalText} from './modal.js';

export interface Props {
  onDismiss: () => void;
  isOpen: boolean;
  text: string;
}

export function NotLoggedInModal({onDismiss, isOpen, text}: Props) {
  const loginHref = links.login(
    window.location.pathname,
    window.location.search,
  );

  return (
    <Modal isOpen={isOpen} onDismiss={onDismiss} title="Not Logged In">
      <ModalText>{text}</ModalText>
      <ModalActions>
        <a className="modal-confirm" href={loginHref}>
          Login
        </a>
      </ModalActions>
    </Modal>
  );
}
