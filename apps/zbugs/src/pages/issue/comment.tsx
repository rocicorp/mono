import {useZero} from '../../domain/schema.js';
import {useQuery} from 'zero-react/src/use-query.js';
import Markdown from '../../components/markdown.js';
import style from './comment.module.css';

export default function Comment({id}: {id: string}) {
  const z = useZero();
  const q = z.query.comment
    .where('id', id)
    .related('creator', creator => creator.one())
    .one();
  const comment = useQuery(q);

  if (!comment) {
    return null;
  }

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
      <Markdown>{comment.body}</Markdown>
    </div>
  );
}
