import {useZero} from '../../domain/schema.js';
import {useQuery} from 'zero-react/src/use-query.js';
import Markdown from '../../components/markdown.js';
import style from './comment.module.css';
import {useState} from 'react';
import CommentComposer from './comment-composer.js';

export default function Comment({id, issueID}: {id: string; issueID: string}) {
  const z = useZero();
  const q = z.query.comment
    .where('id', id)
    .related('creator', creator => creator.one())
    .one();
  const comment = useQuery(q);
  const [editing, setEditing] = useState(false);

  if (!comment) {
    return null;
  }

  const edit = () => {
    setEditing(true);
  };

  return (
    <div className={style.commentItem}>
      <p className={style.commentAuthor}>
        <img
          src={comment.creator.avatar}
          width="40"
          height="40"
          style={{borderRadius: '50%', display: 'inline-block'}}
          alt={comment.creator.name}
        />{' '}
        {comment.creator.login}
      </p>
      {editing ? (
        <CommentComposer
          id={id}
          body={comment.body}
          issueID={issueID}
          onDone={() => setEditing(false)}
        />
      ) : (
        <Markdown>{comment.body}</Markdown>
      )}
      <div>{editing ? null : <a onClick={edit}>Edit</a>}</div>
    </div>
  );
}
