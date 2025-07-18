import {nanoid} from 'nanoid';
import {useCallback, useEffect, useState} from 'react';
import {Button} from '../../components/button.tsx';
import {Modal, ModalActions, ModalBody} from '../../components/modal.tsx';
import {useZero} from '../../hooks/use-zero.ts';
import {
  MAX_ISSUE_DESCRIPTION_LENGTH,
  MAX_ISSUE_TITLE_LENGTH,
} from '../../limits.ts';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {promiseRace} from '../../../../../packages/shared/src/promise.ts';
import {sleep} from '../../../../../packages/shared/src/sleep.ts';

interface Props {
  /** If id is defined the issue created by the composer. */
  onDismiss: (id?: string | undefined) => void;
  isOpen: boolean;
}

const focusInput = (input: HTMLInputElement | null) => {
  if (input) {
    input.focus();
  }
};

export function IssueComposer({isOpen, onDismiss}: Props) {
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState<string>('');
  const z = useZero();

  // Function to handle textarea resizing
  function autoResizeTextarea(textarea: HTMLTextAreaElement) {
    textarea.style.height = 'auto';
    textarea.style.height = textarea.scrollHeight + 'px';
  }

  // Use the useEffect hook to handle the auto-resize logic for textarea
  useEffect(() => {
    const textareas = document.querySelectorAll(
      '.autoResize',
    ) as NodeListOf<HTMLTextAreaElement>;

    textareas.forEach(textarea => {
      const handleInput = () => autoResizeTextarea(textarea);
      textarea.addEventListener('input', handleInput);
      autoResizeTextarea(textarea);

      return () => {
        textarea.removeEventListener('input', handleInput);
      };
    });
  }, [description]);

  const handleSubmit = async () => {
    const id = nanoid();

    const result = z.mutate.issue.create({
      id,
      title,
      description: description ?? '',
      created: Date.now(),
      modified: Date.now(),
    });

    reset();
    onDismiss(id);

    const raceResult = await promiseRace([sleep(5000), result.server]);
    if (raceResult === 0) {
      // TODO show toast
      console.log('timed out');
    }
  };

  const reset = () => {
    setTitle('');
    setDescription('');
  };

  const canSave = () => title.trim().length > 0;

  const isDirty = useCallback(
    () => title.trim().length > 0 || description.trim().length > 0,
    [title, description],
  );

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (canSave() && isCtrlEnter(e)) {
      e.preventDefault();
      handleSubmit();
    }
  };

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
      <ModalBody>
        <div className="flex items-center w-full mt-1.5 px-4">
          <input
            className="new-issue-title"
            placeholder="Issue title"
            value={title}
            ref={focusInput} // Attach the inputRef to this input field
            onChange={e => setTitle(e.target.value)}
            onKeyDown={handleKeyDown}
            maxLength={MAX_ISSUE_TITLE_LENGTH}
          />
        </div>
        <div className="w-full px-4">
          <textarea
            className="new-issue-description autoResize"
            value={description || ''}
            onChange={e => setDescription(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Add description..."
            maxLength={MAX_ISSUE_DESCRIPTION_LENGTH}
          ></textarea>
        </div>
      </ModalBody>
      <ModalActions>
        <Button
          className="modal-confirm"
          eventName="New issue confirm"
          onAction={handleSubmit}
          disabled={!canSave()}
        >
          Save Issue
        </Button>
      </ModalActions>
    </Modal>
  );
}
