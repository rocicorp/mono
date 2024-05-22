import Modal from './modal.jsx';

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
    <Modal isOpen={isOpen} size="small" center={true} onDismiss={onDismiss}>
      <div className="flex flex-col w-full p-6">
        {/* Header */}
        <div className="flex items-center justify-between flex-shrink-0">
          <span className="text-md text-white">{title}</span>
        </div>

        <div className="flex flex-col flex-1 py-2">
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
