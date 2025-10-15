import {nanoid} from 'nanoid';
import {useCallback, useEffect, useRef, useState} from 'react';
import {Button} from '../../components/button.tsx';
import {
  ImageUploadArea,
  type TextAreaPatch,
} from '../../components/image-upload-area.tsx';
import {Modal, ModalActions, ModalBody} from '../../components/modal.tsx';
import {useZero} from '../../hooks/use-zero.ts';
import {
  MAX_ISSUE_DESCRIPTION_LENGTH,
  MAX_ISSUE_TITLE_LENGTH,
} from '../../limits.ts';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {type ProjectRow} from '../../../shared/schema.ts';
import {ProjectPicker} from '../../components/project-picker.tsx';

interface Props {
  /** If id is defined the issue created by the composer. */
  onDismiss: (created?: {projectName: string; id: string} | undefined) => void;
  isOpen: boolean;
  projects: ProjectRow[];
  projectName: string;
}

const focusInput = (input: HTMLInputElement | null) => {
  if (input) {
    input.focus();
  }
};

export function IssueComposer({
  isOpen,
  onDismiss,
  projects,
  projectName,
}: Props) {
  const [project, setProject] = useState(
    projects.find(p => p.lowerCaseName === projectName.toLocaleLowerCase()),
  );
  useEffect(() => {
    if (project === undefined) {
      setProject(
        projects.find(p => p.lowerCaseName === projectName.toLocaleLowerCase()),
      );
    }
  }, [projects, project]);
  const [title, setTitle] = useState('');
  const [description, setDescription] = useState<string>('');
  const textareaRef = useRef<HTMLTextAreaElement>(null);
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

  const handleSubmit = () => {
    if (!project) {
      return;
    }
    const id = nanoid();

    z.mutate.issue.create({
      id,
      projectID: project?.id,
      title,
      description: description ?? '',
      created: Date.now(),
      modified: Date.now(),
    });
    reset();
    onDismiss({id, projectName: project?.name ?? projectName});
  };

  const reset = () => {
    setTitle('');
    setDescription('');
  };

  const canSave = () => title.trim().length > 0 && project !== undefined;

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

  const onInsert = (patch: TextAreaPatch) => {
    setDescription(prev => patch.apply(prev));
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
        <div
          className="w-full px-4"
          style={{width: 'fit-content', marginBottom: '1rem'}}
        >
          <ProjectPicker
            projects={projects}
            selectedProjectName={project?.name}
            onChange={value => setProject(value)}
          ></ProjectPicker>
        </div>
        <div className="flex items-center w-full px-4">
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
          <ImageUploadArea textAreaRef={textareaRef} onInsert={onInsert}>
            <textarea
              className="new-issue-description autoResize"
              value={description || ''}
              onChange={e => setDescription(e.target.value)}
              onKeyDown={handleKeyDown}
              placeholder="Add description..."
              maxLength={MAX_ISSUE_DESCRIPTION_LENGTH}
              ref={textareaRef}
            ></textarea>
          </ImageUploadArea>
        </div>
        <div className="w-full px-4 mt-4">
          <p className="aside">
            Testing Zero? Please make sure to delete your issue after.
            <br />
            Want a faster response?{' '}
            <a href="https://discord.rocicorp.dev/">
              Join us on Discord &rarr;
            </a>
          </p>
        </div>{' '}
      </ModalBody>
      <ModalActions>
        <Button
          className="modal-confirm"
          eventName="New issue confirm"
          onAction={handleSubmit}
          disabled={!canSave()}
        >
          Save Issue
        </Button>{' '}
      </ModalActions>
    </Modal>
  );
}
