import CloseIcon from '../assets/icons/close.svg?react';
import Modal from './modal.js';

export interface Props {
  onDismiss?: () => void | undefined;
  isOpen: boolean;
  href?: string;
}

export function NotLoggedInModal({onDismiss, isOpen, href}: Props) {
  return (
    <Modal isOpen={isOpen} onDismiss={onDismiss}>
      <div className="flex flex-col w-full py-4 overflow-hidden modal-container">
        <div className="flex items-center justify-between flex-shrink-0 px-4 issue-composer-header">
          <div className="flex items-center">
            <span className="issue-detail-label">Not Logged In</span>
          </div>
          <div className="flex items-center">
            <button className="inline-flex rounded items-center justify-center text-gray-500 h-7 w-7 rouned hover:text-gray-700">
              <CloseIcon className="w-4" />
            </button>
          </div>
        </div>
        <div className="flex flex-col flex-1 pb-3.5 overflow-y-auto">
          <div className="flex items-center w-full mt-1.5 px-4">
            <p>You need to be logged in to create a new issue.</p>
          </div>
        </div>
        <div className="flex items-center flex-shrink-0 px-4 pt-3">
          <a
            className="px-3 ml-auto text-black bg-primary rounded save-issue"
            href={href}
          >
            Login
          </a>
        </div>
      </div>
    </Modal>
  );
}
