import {useState} from 'react';
import {useZero} from '../../hooks/use-zero.js';
import {nanoid} from 'zero-client/src/util/nanoid.js';
import {useLogin} from '../../hooks/use-login.js';

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
        style={{
          width: '100%',
          height: '100%',
          background: 'white',
          color: 'black',
          borderRadius: '0.3rem',
        }}
      />
      <button onMouseDown={save}>{id ? 'Save' : 'Comment'}</button>{' '}
      {id ? <button onMouseDown={onDone}>Cancel</button> : null}
    </>
  );
}
