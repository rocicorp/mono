import type {Row} from '@rocicorp/zero';
import {useZero} from '@rocicorp/zero/react';
import classNames from 'classnames';
import {memo, useState} from 'react';
import {mutators} from '../../../shared/mutators.ts';
import {makePermalink} from '../../comment-permalink.ts';
import type {commentQuery} from '../../comment-query.ts';
import {AvatarImage} from '../../components/avatar-image.tsx';
import {Button} from '../../components/button.tsx';
import {CanEdit} from '../../components/can-edit.tsx';
import {Confirm} from '../../components/confirm.tsx';
import {EmojiPanel} from '../../components/emoji-panel.tsx';
import {Link} from '../../components/link.tsx';
import {Markdown} from '../../components/markdown.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {useHash} from '../../hooks/use-hash.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {CommentComposer} from './comment-composer.tsx';
import style from './comment.module.css';

type Props = {
  id: string;
  issueID: string;
  comment: Row<ReturnType<typeof commentQuery>>;
  /**
   * Height of the comment. Used to keep the layout stable when comments are
   * being "loaded".
   */
  height?: number | undefined;
  highlight?: boolean | undefined;
};

export const Comment = memo(
  ({id, issueID, comment, height, highlight}: Props) => {
    const z = useZero();
    const [editing, setEditing] = useState(false);
    const login = useLogin();

    const [deleteConfirmationShown, setDeleteConfirmationShown] =
      useState(false);

    const hash = useHash();
    const permalink = comment && makePermalink(comment);
    const isPermalinked = highlight || hash === permalink;

    const edit = () => setEditing(true);
    const remove = () => z.mutate(mutators.comment.remove(id));

    if (!comment) {
      return <div style={{height}}></div>;
    }
    return (
      <div
        className={classNames({
          [style.commentItem]: true,
          [style.authorComment]:
            comment.creatorID === login.loginState?.decoded.sub,
          [style.permalinked]: isPermalinked,
        })}
      >
        {comment.creator && (
          <p className={style.commentAuthor}>
            <AvatarImage
              user={comment.creator}
              style={{
                width: '2rem',
                height: '2rem',
                borderRadius: '50%',
                display: 'inline-block',
                marginRight: '0.3rem',
              }}
            />{' '}
            {comment.creator.login}
          </p>
        )}
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
          <>
            <div className="markdown-container">
              <Markdown>{comment.body}</Markdown>
            </div>
            <EmojiPanel
              issueID={issueID}
              commentID={comment.id}
              emojis={comment.emoji}
            />
          </>
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
  },
);
