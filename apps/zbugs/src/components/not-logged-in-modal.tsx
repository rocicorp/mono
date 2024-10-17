import Modal from './modal.js';

export interface Props {
  onDismiss?: () => void | undefined;
  isOpen: boolean;
  href?: string;
}

export function NotLoggedInModal({onDismiss, isOpen, href}: Props) {
  return (
    <Modal isOpen={isOpen} onDismiss={onDismiss} title="Not Logged In">
      <div className="flex flex-col w-full py-4 overflow-hidden modal-container">
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
