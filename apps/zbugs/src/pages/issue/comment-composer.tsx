import {nanoid} from 'nanoid';
import {useState, useEffect} from 'react';
import {Button} from '../../components/button.js';
import {useLogin} from '../../hooks/use-login.js';
import {useZero} from '../../hooks/use-zero.js';

export default function CommentComposer({
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
      z.mutate.comment.create({
        id: nanoid(),
        issueID,
        creatorID: z.userID,
        body: currentBody,
        created: Date.now(),
      });
      onDone?.();
      return;
    }

    z.mutate.comment.update({id, body: currentBody});
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

    const cleanupFns = Array.from(textareas).map(handleResize);

    return () => cleanupFns.forEach(fn => fn());
  }, [currentBody]);

  const textAreaChange = (e: React.ChangeEvent<HTMLTextAreaElement>) => {
    setCurrentBody(e.target.value);
  };

  if (!login.loginState) {
    return null;
  }

  return (
    <>
      <textarea
        value={currentBody}
        onChange={textAreaChange}
        className="comment-input autoResize"
      />
      <Button
        className="secondary-button"
        onAction={save}
        disabled={currentBody.trim().length === 0}
      >
        {id ? 'Save' : 'Add comment'}
      </Button>{' '}
      {id ? (
        <Button className="edit-comment-cancel" onAction={onDone}>
          Cancel
        </Button>
      ) : null}
    </>
  );
}
