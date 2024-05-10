import CloseIcon from './assets/icons/close.svg';
import Modal from './modal';

interface Props {
  isOpen: boolean;
  onDismiss?: () => void;
  onConfirm: () => void;
  title: string;
  message: string;
  action: string;
}

export default function ConfirmationModal({
  isOpen,
  onDismiss,
  onConfirm,
  title,
  message,
  action,
}: Props) {
  const handleClickAction = () => {
    onConfirm();
    if (onDismiss) {
      onDismiss();
    }
  };

  const handleClickCancel = () => {
    if (onDismiss) {
      onDismiss();
    }
  };

  return (
    <Modal isOpen={isOpen} center={true} onDismiss={onDismiss}>
      <div className="flex flex-col w-full p-4">
        {/* Header */}
        <div className="flex items-center justify-between flex-shrink-0">
          <span className="text-md text-white">{title}</span>
          <div
            className="inline-flex items-center justify-center h-7 w-7 rounded hover:bg-gray-850 hover-text-gray-400 text-white"
            onMouseDown={handleClickCancel}
          >
            <CloseIcon className="w-4" />
          </div>
        </div>

        <div className="flex flex-col flex-1 py-6">
          <p className="text-white text-md">{message}</p>
        </div>

        <div className="flex items-center justify-end flex-shrink-0">
          <button
            className="px-3 rounded hover:bg-gray-600 h-7 focus:outline-none bg-gray text-white"
            onMouseDown={handleClickCancel}
          >
            Cancel
          </button>
          <button
            className="px-3 ml-2 rounded hover:bg-gray-600 h-7 focus:outline-none bg-gray text-white"
            onMouseDown={handleClickAction}
          >
            {action}
          </button>
        </div>
      </div>
    </Modal>
  );
}
