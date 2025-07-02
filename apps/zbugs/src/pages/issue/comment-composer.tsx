import {useEffect, useState} from 'react';
import {Button} from '../../components/button.tsx';
import {useLogin} from '../../hooks/use-login.tsx';
import {useZero} from '../../hooks/use-zero.ts';
import {maxCommentLength} from '../../limits.ts';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {nanoid} from 'nanoid';
import {addComment, editComment} from '../../../shared/mutators.ts';

export function CommentComposer({
  id,
  body,
  issueID,
  onDone,
}: {
  issueID: string;
  id?: string | undefined;
  body?: string | undefined;
  onDone?: (() => void) | undefined;
}) {
  const z = useZero();
  const login = useLogin();
  const [currentBody, setCurrentBody] = useState(body ?? '');
  const save = () => {
    setCurrentBody(body ?? '');
    if (!id) {
      addComment(login.loginState?.decoded)(z, {
        id: nanoid(),
        issueID,
        body: currentBody,
        created: Date.now(),
      });
      onDone?.();
      return;
    }

    editComment(login.loginState?.decoded)(z, {id, body: currentBody});
    onDone?.();
  };

  // Handle textarea resizing
  function autoResizeTextarea(textarea: HTMLTextAreaElement) {
    textarea.style.height = 'auto';
    textarea.style.height = textarea.scrollHeight + 'px';
  }

  useEffect(() => {
    const textareas = document.querySelectorAll(
      '.autoResize',
    ) as NodeListOf<HTMLTextAreaElement>;

    const handleResize = (textarea: HTMLTextAreaElement) => {
      autoResizeTextarea(textarea);
      const handleInput = () => autoResizeTextarea(textarea);
      textarea.addEventListener('input', handleInput);

      return () => textarea.removeEventListener('input', handleInput);
    };

    const cleanupFns = Array.from(textareas, handleResize);

    return () => cleanupFns.forEach(fn => fn());
  }, [currentBody]);

  const handleChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setCurrentBody(e.target.value);
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (isCtrlEnter(e)) {
      e.preventDefault();
      save();
    }
  };

  if (!login.loginState) {
    return null;
  }

  return (
    <>
      <textarea
        value={currentBody}
        onChange={handleChange}
        onKeyDown={handleKeyDown}
        className="comment-input autoResize"
        /* The launch post has a speical maxLength because trolls */
        maxLength={maxCommentLength(issueID)}
      />
      <Button
        className="secondary-button"
        eventName={id ? 'Save comment edits' : 'Add new comment'}
        onAction={save}
        disabled={currentBody.trim().length === 0}
      >
        {id ? 'Save' : 'Add comment'}
      </Button>{' '}
      {id ? (
        <Button
          className="edit-comment-cancel"
          eventName="Cancel comment edits"
          onAction={onDone}
        >
          Cancel
        </Button>
      ) : null}
    </>
  );
}
