import {nanoid} from 'nanoid';
import {useState} from 'react';
import CloseIcon from './assets/icons/close.svg?react';
import {IssueCreationPartial, Priority, Status} from './issue.js';
import Modal from './modal.jsx';
import PriorityMenu from './priority-menu.jsx';
import StatusMenu from './status-menu.jsx';

interface Props {
  isOpen: boolean;
  onDismiss?: () => void;
  onCreateIssue: (i: IssueCreationPartial) => void;
}

export default function IssueModal({isOpen, onDismiss, onCreateIssue}: Props) {
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState('');
  const [priority, setPriority] = useState(Priority.None);
  const [status, setStatus] = useState(Status.Backlog);

  const handleSubmit = () => {
    if (title === '') {
      //   showWarning("Please enter a title before submiting", "Title required");
      return;
    }
    onCreateIssue({
      id: nanoid(),
      title,
      priority,
      status,
      description,
    });
    if (onDismiss) onDismiss();
    resetModalState();
  };

  const resetModalState = () => {
    setTitle('');
    setDescription('');
    setPriority(Priority.None);
    setStatus(Status.Backlog);
  };

  const handleClickCloseBtn = () => {
    if (onDismiss) onDismiss();
  };

  const body = (
    <div className="flex flex-col w-full py-4">
      {/* header */}
      <div className="flex items-center justify-between flex-shrink-0 px-4">
        <div className="flex items-center">
          <span className="ml-2 font-normal text-white">› New Issue</span>
        </div>
        <div className="flex items-center">
          <div
            className="inline-flex items-center justify-center ml-2 h-7 w-7 rounded hover:bg-gray-850 hover-text-gray-400 text-white"
            onMouseDown={handleClickCloseBtn}
          >
            <CloseIcon className="w-4" />
          </div>
        </div>
      </div>
      <div className="flex flex-col flex-1 pb-3.5 overflow-y-auto">
        {/* Issue title */}
        <div className="flex items-center w-full mt-1.5 px-4">
          <StatusMenu onSelect={setStatus} status={status} />
          <input
            className="w-full ml-1.5 text-lg font-semibold placeholder-white  border-none h-7 focus:outline-none bg-gray-900 text-white"
            placeholder="Issue title"
            value={title}
            onChange={e => setTitle(e.target.value)}
          />
        </div>

        {/* Issue description editor */}
        <div className="flex w-full px-4">
          <textarea
            rows={5}
            onChange={e => setDescription(e.target.value)}
            value={description}
            className="w-full mt-4 ml-5 font-normal border-none appearance-none min-h-12 text-md focus:outline-none placeholder-white bg-gray-900 text-white"
            placeholder="Add description..."
          />
        </div>
      </div>

      {/* Issue labels & priority */}
      <div className="flex items-center px-4 pb-3 mt-1 border-b border-gray-500">
        <PriorityMenu
          onSelect={setPriority}
          labelVisible={true}
          priority={priority}
        />
      </div>

      {/* Footer */}
      <div className="flex items-center justify-end flex-shrink-0 px-4 pt-3">
        <button
          className="px-3 ml-2 rounded hover:bg-indigo-700 h-7 focus:outline-none bg-gray text-white"
          onMouseDown={handleSubmit}
        >
          Save Issue
        </button>
      </div>
    </div>
  );

  return (
    <Modal isOpen={isOpen} center={false} size="large" onDismiss={onDismiss}>
      {body}
    </Modal>
  );
}
