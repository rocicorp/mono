import {Button} from '../../components/button.js';
import Modal from '../../components/modal.js';

interface DeleteConfirmationModalProps {
  isOpen: boolean;
  onDismiss: () => void;
  onDelete: () => void;
}

export function DeleteConfirmationModal({
  isOpen,
  onDismiss,
  onDelete,
}: DeleteConfirmationModalProps) {
  return (
    <Modal title="Delete Comment" isOpen={isOpen} onDismiss={onDismiss}>
      <div className="flex flex-col w-full py-4 overflow-hidden modal-container">
        <div className="flex flex-col flex-1 pb-3.5 overflow-y-auto">
          <div className="flex items-center w-full mt-1.5 px-4">
            Deleting a comment is permanent. Are you sure you want to delete
            this comment?
          </div>
        </div>
        <div className="flex items-center flex-shrink-0 px-4 pt-3 gap-4">
          <Button
            className="modal-confirm"
            onAction={() => {
              onDelete();
              onDismiss();
            }}
          >
            Delete
          </Button>{' '}
          <Button onAction={onDismiss}>Cancel</Button>
        </div>
      </div>
    </Modal>
  );
}
