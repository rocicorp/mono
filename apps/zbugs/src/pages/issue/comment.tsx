import {useQuery} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {memo, useState} from 'react';
import {Button} from '../../components/button.js';
import {CanEdit} from '../../components/can-edit.js';
import {Confirm} from '../../components/confirm.js';
import {EmojiPanel} from '../../components/emoji-panel.js';
import {Link} from '../../components/link.js';
import Markdown from '../../components/markdown.js';
import RelativeTime from '../../components/relative-time.js';
import {useHash} from '../../hooks/use-hash.js';
import {useLogin} from '../../hooks/use-login.js';
import {useZero} from '../../hooks/use-zero.js';
import CommentComposer from './comment-composer.js';
import style from './comment.module.css';

type Props = {
  id: string;
  issueID: string;
  /**
   * Height of the comment. Used to keep the layout stable when comments are
   * being "loaded".
   */
  height?: number | undefined;
};

// eslint-disable-next-line react-refresh/only-export-components
export function parsePermalink(hash: string): string | undefined {
  const match = hash.match(/^comment-(.+)$/);
  return match ? match?.[1] : undefined;
}

const Comment = memo(({id, issueID, height}: Props) => {
  const z = useZero();
  const q = z.query.comment
    .where('id', id)
    .related('creator', creator => creator.one())
    .one();
  const comment = useQuery(q);
  const [editing, setEditing] = useState(false);
  const login = useLogin();
  const [deleteConfirmationShown, setDeleteConfirmationShown] = useState(false);

  const hash = useHash();
  const permalink = comment ? `comment-${comment.id}` : undefined;
  const isPermalinked = hash === permalink;

  const edit = () => setEditing(true);
  const remove = () => z.mutate.comment.delete({id});

  if (!comment) {
    return <div style={{height}}></div>;
  }
  return (
    <div
      className={classNames({
        [style.commentItem]: true,
        [style.authorComment]:
          comment.creatorID == login.loginState?.decoded.sub,
        [style.permalinked]: isPermalinked,
      })}
    >
      <p className={style.commentAuthor}>
        <img
          src={comment.creator?.avatar}
          style={{
            width: '2rem',
            height: '2rem',
            borderRadius: '50%',
            display: 'inline-block',
            marginRight: '0.3rem',
          }}
          alt={comment.creator?.name}
        />{' '}
        {comment.creator?.login}
      </p>
      <span id={permalink} className={style.commentTimestamp}>
        <Link href={`#${permalink}`}>
          <RelativeTime timestamp={comment.created} />
        </Link>
      </span>
      {editing ? (
        <CommentComposer
          id={id}
          body={comment.body}
          issueID={issueID}
          onDone={() => setEditing(false)}
        />
      ) : (
        <div className="markdown-container">
          <Markdown>{comment.body}</Markdown>
          <EmojiPanel issueID={issueID} commentID={comment.id} />
        </div>
      )}
      {editing ? null : (
        <CanEdit ownerID={comment.creatorID}>
          <div className={style.commentActions}>
            <Button eventName="Edit comment" onAction={edit}>
              Edit
            </Button>
            <Button
              eventName="Delete comment"
              onAction={() => setDeleteConfirmationShown(true)}
            >
              Delete
            </Button>
          </div>
        </CanEdit>
      )}
      <Confirm
        title="Delete Comment"
        text="Deleting a comment is permanent. Are you sure you want to delete this comment?"
        okButtonLabel="Delete"
        isOpen={deleteConfirmationShown}
        onClose={b => {
          if (b) {
            remove();
          }
          setDeleteConfirmationShown(false);
        }}
      />
    </div>
  );
});

export {Comment as default};
