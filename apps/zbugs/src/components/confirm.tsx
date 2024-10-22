import {useCallback} from 'react';
import {Button} from './button.js';
import {Modal, ModalActions, ModalText} from './modal.jsx';

interface Props {
  isOpen: boolean;
  onClose: (ok: boolean) => void;
  okButtonLabel?: string | undefined;
  cancelButtonLabel?: string | undefined;
  title?: string | undefined;
  text: string;
}

export function Confirm({
  isOpen,
  onClose,
  okButtonLabel,
  cancelButtonLabel,
  title,
  text,
}: Props) {
  const onOK = useCallback(() => onClose(true), [onClose]);
  const onCancel = useCallback(() => onClose(false), [onClose]);

  return (
    <Modal title={title} isOpen={isOpen} onDismiss={onCancel}>
      <ModalText>{text}</ModalText>
      <ModalActions>
        <Button className="modal-confirm" onAction={onOK} autoFocus>
          {okButtonLabel || 'OK'}
        </Button>
        <Button onAction={onCancel}>{cancelButtonLabel || 'Cancel'}</Button>
      </ModalActions>
    </Modal>
  );
}
