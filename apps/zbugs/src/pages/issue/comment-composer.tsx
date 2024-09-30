import {useState} from 'react';
import {useZero} from '../../domain/schema.js';
import {nanoid} from 'zero-client/src/util/nanoid.js';

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

  return (
    <>
      <textarea
        value={currentBody}
        onChange={textAreaChange}
        style={{width: '100%', height: '100%', background: 'grey'}}
      />
      <button onClick={save}>{id ? 'Save' : 'Comment'}</button>{' '}
      {id ? <button onClick={onDone}>Cancel</button> : null}
    </>
  );
}
