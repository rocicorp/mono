import {nanoid} from 'nanoid';
import {useCallback, useRef, useState} from 'react';
import Modal from '../../components/modal.js';
import {useZero} from '../../hooks/use-zero.js';

interface Props {
  /** If id is defined the issue created by the composer. */
  onDismiss: (id?: string | undefined) => void;
  isOpen: boolean;
}

export default function IssueComposer({isOpen, onDismiss}: Props) {
  const ref = useRef<HTMLInputElement>(null);
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState<string>('');
  const z = useZero();

  const handleSubmit = () => {
    const id = nanoid();
    z.mutate.issue.create({
      id,
      shortID: undefined,
      title,
      description: description ?? '',
      created: Date.now(),
      creatorID: z.userID,
      // TODO: Should be able to skip passing optional fields.
      assigneeID: undefined,
      modified: Date.now(),
      open: true,
      labelIDs: '',
    });
    reset();
    onDismiss(id);
  };

  const reset = () => {
    setTitle('');
    setDescription('');
  };

  const canSave = () =>
    title.trim().length > 0 && description.trim().length > 0;

  const isDirty = useCallback(
    () => title.trim().length > 0 || description.trim().length > 0,
    [title, description],
  );

  const body = (
    <div className="flex flex-col w-full py-4 overflow-hidden modal-container">
      <div className="flex flex-col flex-1 pb-3.5 overflow-y-auto">
        <div className="flex items-center w-full mt-1.5 px-4">
          <input
            className="bg-modal w-full text-lg font-semibold placeholder-gray-400 border-none h-7 focus:border-none focus:outline-none focus:ring-0"
            placeholder="Issue title"
            value={title}
            ref={ref}
            onChange={e => setTitle(e.target.value)}
          />
        </div>
        <div className="w-full px-4">
          <textarea
            className="bg-modal prose w-full max-w-full mt-2 font-normal appearance-none min-h-12 text-md editor border border-transparent focus:outline-none focus:ring-0"
            value={description || ''}
            onChange={e => setDescription(e.target.value)}
            placeholder="Add description..."
          ></textarea>
        </div>
      </div>
      <div className="flex items-center flex-shrink-0 px-4 pt-3">
        <button
          className="px-3 ml-auto text-black bg-primary rounded save-issue"
          onMouseDown={handleSubmit}
          disabled={!canSave()}
        >
          Save Issue
        </button>
      </div>
    </div>
  );

  return (
    <Modal
      title="New Issue"
      isOpen={isOpen}
      center={false}
      size="large"
      onDismiss={() => {
        reset();
        onDismiss();
      }}
      isDirty={isDirty}
    >
      {body}
    </Modal>
  );
}
