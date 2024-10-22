import {nanoid} from 'nanoid';
import {useCallback, useEffect, useRef, useState} from 'react';
import {Button} from '../../components/button.js';
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

  // Function to handle textarea resizing
  function autoResizeTextarea(textarea: HTMLTextAreaElement) {
    textarea.style.height = 'auto';
    textarea.style.height = textarea.scrollHeight + 'px';
  }

  // Use the useEffect hook to handle the auto-resize logic
  useEffect(() => {
    const textareas = document.querySelectorAll(
      '.autoResize',
    ) as NodeListOf<HTMLTextAreaElement>;

    // Add the input event listener to all textareas
    textareas.forEach(textarea => {
      const handleInput = () => autoResizeTextarea(textarea);
      textarea.addEventListener('input', handleInput);
      // Perform initial resize
      autoResizeTextarea(textarea);

      // Clean up the event listener when the component unmounts
      return () => {
        textarea.removeEventListener('input', handleInput);
      };
    });
  }, [description]); // Add the description state to the dependency array

  const handleSubmit = () => {
    const id = nanoid();
    z.mutate.issue.create({
      id,
      shortID: undefined,
      title,
      description: description ?? '',
      created: Date.now(),
      creatorID: z.userID,
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
            className="new-issue-title"
            placeholder="Issue title"
            value={title}
            ref={ref}
            onChange={e => setTitle(e.target.value)}
          />
        </div>
        <div className="w-full px-4">
          <textarea
            className="new-issue-description autoResize"
            value={description || ''}
            onChange={e => setDescription(e.target.value)}
            placeholder="Add description..."
          ></textarea>
        </div>
      </div>
      <div className="flex items-center flex-shrink-0 px-4 pt-3">
        <Button
          className="modal-confirm save-issue"
          onAction={handleSubmit}
          disabled={!canSave()}
        >
          Save Issue
        </Button>
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
