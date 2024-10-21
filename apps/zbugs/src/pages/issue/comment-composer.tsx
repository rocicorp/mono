import {nanoid} from 'nanoid';
import {useState} from 'react';
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
        className="comment-input"
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
