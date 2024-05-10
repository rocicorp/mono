import CloseIcon from './assets/icons/close.svg';
import Modal from './modal';

interface Props {
  isOpen: boolean;
  onDismiss?: () => void;
  onConfirm: () => void;
  title: string;
  message: string;
}

export default function ConfirmationModal({
  isOpen,
  onDismiss,
  onConfirm,
  title,
  message,
}: Props) {
  const handleClickYes = () => {
    onConfirm();
    if (onDismiss) onDismiss();
  };

  const handleClickNo = () => {
    if (onDismiss) onDismiss();
  };

  const body = (
    <div className="flex flex-col w-full p-4">
      {/* Header */}
      <div className="flex items-center justify-between flex-shrink-0">
        <span className="text-md text-white">{title}</span>
        <div
          className="inline-flex items-center justify-center h-7 w-7 rounded hover:bg-gray-850 hover-text-gray-400 text-white"
          onMouseDown={handleClickNo}
        >
          <CloseIcon className="w-4" />
        </div>
      </div>

      <div className="flex flex-col flex-1 py-4">
        <p className="text-white text-sm">{message}</p>
      </div>

      <div className="flex items-center justify-end flex-shrink-0">
        <button
          className="px-3 rounded hover:bg-indigo-700 h-7 focus:outline-none bg-gray text-white"
          onMouseDown={handleClickYes}
        >
          Yes
        </button>
        <button
          className="px-3 ml-2 rounded hover:bg-gray-600 h-7 focus:outline-none bg-gray text-white"
          onMouseDown={handleClickNo}
        >
          Cancel
        </button>
      </div>
    </div>
  );

  return (
    <Modal isOpen={isOpen} center={true} onDismiss={onDismiss}>
      {body}
    </Modal>
  );
}
