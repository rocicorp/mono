import {useQuery} from '@rocicorp/zero/react';
import {useWindowVirtualizer, Virtualizer} from '@tanstack/react-virtual';
import {nanoid} from 'nanoid';
import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import {toast, ToastContainer} from 'react-toastify';
import {assert} from 'shared/src/asserts.js';
import {useParams} from 'wouter';
import {navigate, useHistoryState} from 'wouter/use-browser-location';
import {findLastIndex} from '../../../../../packages/shared/src/find-last-index.ts';
import {must} from '../../../../../packages/shared/src/must.ts';
import {difference} from '../../../../../packages/shared/src/set-utils.ts';
import {
  type CommentRow,
  type IssueRow,
  type UserRow,
} from '../../../shared/schema.ts';
import statusClosed from '../../assets/icons/issue-closed.svg';
import statusOpen from '../../assets/icons/issue-open.svg';
import circle from '../../assets/icons/circle.svg';
import {commentQuery} from '../../comment-query.ts';
import {AvatarImage} from '../../components/avatar-image.tsx';
import {Button} from '../../components/button.tsx';
import {CanEdit} from '../../components/can-edit.tsx';
import {Combobox} from '../../components/combobox.tsx';
import {Confirm} from '../../components/confirm.tsx';
import {EmojiPanel} from '../../components/emoji-panel.tsx';
import {LabelPicker} from '../../components/label-picker.tsx';
import {Link} from '../../components/link.tsx';
import {Markdown} from '../../components/markdown.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {UserPicker} from '../../components/user-picker.tsx';
import {type Emoji} from '../../emoji-utils.ts';
import {useCanEdit} from '../../hooks/use-can-edit.ts';
import {useDocumentHasFocus} from '../../hooks/use-document-has-focus.ts';
import {useEmojiDataSourcePreload} from '../../hooks/use-emoji-data-source-preload.ts';
import {useIsScrolling} from '../../hooks/use-is-scrolling.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {useZero} from '../../hooks/use-zero.ts';
import {
  MAX_ISSUE_DESCRIPTION_LENGTH,
  MAX_ISSUE_TITLE_LENGTH,
} from '../../limits.ts';
import {LRUCache} from '../../lru-cache.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {CACHE_NAV} from '../../query-cache-policy.ts';
import {links, type ZbugsHistoryState} from '../../routes.ts';
import {CommentComposer} from './comment-composer.tsx';
import {Comment} from './comment.tsx';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {queries} from '../../../shared/queries.ts';
import {INITIAL_COMMENT_LIMIT} from '../../../shared/consts.ts';
import {preload} from '../../zero-preload.ts';
import type {NotificationType} from '../../../shared/mutators.ts';

const {emojiChange, issueDetail, prevNext} = queries;

function softNavigate(path: string, state?: ZbugsHistoryState) {
  navigate(path, {state});
  requestAnimationFrame(() => {
    window.scrollTo(0, 0);
  });
}

const emojiToastShowDuration = 3_000;

export function IssuePage({onReady}: {onReady: () => void}) {
  const z = useZero();
  const params = useParams();

  const idStr = must(params.id);
  const idField = /[^\d]/.test(idStr) ? 'id' : 'shortID';
  const id = idField === 'shortID' ? parseInt(idStr) : idStr;
  const login = useLogin();

  const zbugsHistoryState = useHistoryState<ZbugsHistoryState | undefined>();
  const listContext = zbugsHistoryState?.zbugsListContext;

  const [issue, issueResult] = useQuery(
    issueDetail(login.loginState?.decoded, idField, id, z.userID),
    CACHE_NAV,
  );
  useEffect(() => {
    if (issue || issueResult.type === 'complete') {
      onReady();
    }
  }, [issue, onReady, issueResult.type]);

  const isScrolling = useIsScrolling();
  const [displayed, setDisplayed] = useState(issue);
  useLayoutEffect(() => {
    if (!isScrolling) {
      setDisplayed(issue);
    }
  }, [issue, isScrolling, displayed]);

  if (import.meta.env.DEV) {
    // exposes a function to dev console to create comments.
    // useful for testing displayed above, and other things.
    // eslint-disable-next-line react-hooks/rules-of-hooks
    useEffect(() => {
      (window as unknown as Record<string, unknown>).autocomment = (
        count = 1,
        repeat = true,
      ) => {
        const mut = () => {
          z.mutateBatch(m => {
            for (let i = 0; i < count; i++) {
              const id = nanoid();
              m.comment.insert({
                id,
                issueID: displayed?.id ?? '',
                body: `autocomment ${id}`,
                created: Date.now(),
                creatorID: z.userID,
              });
            }
          });
        };

        if (repeat) {
          setInterval(mut, 5_000);
        } else {
          mut();
        }
      };
    });
  }

  useEffect(() => {
    if (issueResult.type === 'complete') {
      recordPageLoad('issue-page');
      preload(login.loginState?.decoded, z);
    }
  }, [issueResult.type, login.loginState?.decoded, z]);

  useEffect(() => {
    // only push viewed forward if the issue has been modified since the last viewing
    if (
      z.userID !== 'anon' &&
      displayed &&
      displayed.modified > (displayed?.viewState?.viewed ?? 0)
    ) {
      // only set to viewed if the user has looked at it for > 1 second
      const handle = setTimeout(() => {
        z.mutate.viewState.set({
          issueID: displayed.id,
          viewed: Date.now(),
        });
      }, 1000);
      return () => clearTimeout(handle);
    }
    return;
  }, [displayed, z]);

  const [editing, setEditing] = useState<typeof displayed | null>(null);
  const [edits, setEdits] = useState<Partial<typeof displayed>>({});
  useEffect(() => {
    if (displayed?.shortID != null && idField !== 'shortID') {
      navigate(links.issue(displayed), {
        replace: true,
        state: zbugsHistoryState,
      });
    }
  }, [displayed, idField, zbugsHistoryState]);

  const save = () => {
    if (!editing) {
      return;
    }
    z.mutate.issue.update({id: editing.id, ...edits, modified: Date.now()});
    setEditing(null);
    setEdits({});
  };

  const cancel = () => {
    setEditing(null);
    setEdits({});
  };

  // A snapshot before any edits/comments added to the issue in this view is
  // used for finding the next/prev items so that a user can open an item
  // modify it and then navigate to the next/prev item in the list as it was
  // when they were viewing it.
  const [issueSnapshot, setIssueSnapshot] = useState(displayed);
  if (
    displayed !== undefined &&
    (issueSnapshot === undefined || issueSnapshot.id !== displayed.id)
  ) {
    setIssueSnapshot(displayed);
  }
  const prevNextOptions = {
    enabled: listContext !== undefined && issueSnapshot !== undefined,
    ...CACHE_NAV,
  } as const;
  // Don't need to send entire issue to server, just the sort columns plus PK.
  const start = displayed
    ? {
        id: displayed.id,
        created: displayed.created,
        modified: displayed.modified,
      }
    : null;
  const [next] = useQuery(
    prevNext(
      login.loginState?.decoded,
      listContext?.params ?? null,
      start,
      'next',
    ),
    prevNextOptions,
  );
  useKeypress('j', () => {
    if (next) {
      softNavigate(links.issue(next), zbugsHistoryState);
    }
  });

  const [prev] = useQuery(
    prevNext(
      login.loginState?.decoded,
      listContext?.params ?? null,
      start,
      'prev',
    ),
    prevNextOptions,
  );
  useKeypress('k', () => {
    if (prev) {
      softNavigate(links.issue(prev), zbugsHistoryState);
    }
  });

  const labelSet = useMemo(
    () => new Set(displayed?.labels?.map(l => l.id)),
    [displayed?.labels],
  );

  const [displayAllComments, setDisplayAllComments] = useState(false);

  const [allComments, allCommentsResult] = useQuery(
    commentQuery(z, displayed),
    {enabled: displayAllComments && displayed !== undefined, ...CACHE_NAV},
  );

  const [comments, hasOlderComments] = useMemo(() => {
    if (displayed?.comments === undefined) {
      return [undefined, false];
    }
    if (allCommentsResult.type === 'complete') {
      return [allComments, false];
    }
    return [
      displayed.comments.slice(0, INITIAL_COMMENT_LIMIT).reverse(),
      displayed.comments.length > INITIAL_COMMENT_LIMIT,
    ];
  }, [displayed?.comments, allCommentsResult.type, allComments]);

  const issueDescriptionRef = useRef<HTMLDivElement | null>(null);
  const restoreScrollRef = useRef<() => void>();
  const {listRef, virtualizer} = useVirtualComments(comments ?? []);

  // Restore scroll on changes to comments.
  useEffect(() => {
    restoreScrollRef.current?.();
  }, [comments]);

  useEffect(() => {
    if (comments === undefined || comments.length === 0) {
      restoreScrollRef.current = undefined;
      return;
    }

    restoreScrollRef.current = getScrollRestore(
      issueDescriptionRef.current,
      virtualizer,
      comments,
    );
  }, [virtualizer.scrollOffset, comments, virtualizer]);

  // Permalink scrolling behavior
  const [highlightedCommentID, setHighlightedCommentID] = useState<
    string | null
  >(null);

  const highlightComment = (commentID: string) => {
    if (comments === undefined) {
      return;
    }
    const commentIndex = comments.findIndex(c => c.id === commentID);
    if (commentIndex !== -1) {
      setHighlightedCommentID(commentID);
      virtualizer.scrollToIndex(commentIndex, {
        // auto for minimal amount of scrolling.
        align: 'auto',
        // The `smooth` scroll behavior is not fully supported with dynamic size.
        // behavior: 'smooth',
      });
    }
  };

  const [deleteConfirmationShown, setDeleteConfirmationShown] = useState(false);

  const canEdit = useCanEdit(displayed?.creatorID);

  const issueEmojiRef = useRef<HTMLDivElement>(null);

  const [recentEmojis, setRecentEmojis] = useState<Emoji[]>([]);

  const handleEmojiChange = useCallback(
    (added: readonly Emoji[], removed: readonly Emoji[]) => {
      const newRecentEmojis = new Map(recentEmojis.map(e => [e.id, e]));

      for (const emoji of added) {
        if (displayed && emoji.creatorID !== z.userID) {
          maybeShowToastForEmoji(
            emoji,
            displayed,
            virtualizer,
            issueEmojiRef.current,
            setRecentEmojis,
          );
          newRecentEmojis.set(emoji.id, emoji);
        }
      }
      for (const emoji of removed) {
        // toast.dismiss is fine to call with non existing toast IDs
        toast.dismiss(emoji.id);
        newRecentEmojis.delete(emoji.id);
      }

      setRecentEmojis([...newRecentEmojis.values()]);
    },
    [displayed, recentEmojis, virtualizer, z.userID],
  );

  const removeRecentEmoji = useCallback((id: string) => {
    toast.dismiss(id);
    setRecentEmojis(recentEmojis => recentEmojis.filter(e => e.id !== id));
  }, []);

  useEmojiChangeListener(displayed, handleEmojiChange);
  useEmojiDataSourcePreload();
  useShowToastForNewComment(comments, virtualizer, highlightComment);

  if (!displayed && issueResult.type === 'complete') {
    return (
      <div>
        <div>
          <b>Error 404</b>
        </div>
        <div>zarro boogs found</div>
      </div>
    );
  }

  if (!displayed || !comments) {
    return null;
  }

  const remove = () => {
    // TODO: Implement undo - https://github.com/rocicorp/undo
    z.mutate.issue.delete(displayed.id);
    navigate(listContext?.href ?? links.home());
  };

  // TODO: This check goes away once Zero's consistency model is implemented.
  // The query above should not be able to return an incomplete result.
  if (!displayed.creator) {
    return null;
  }

  const rendering = editing ? {...editing, ...edits} : displayed;

  const isSubscribed = issue?.notificationState?.subscribed;
  const currentState: NotificationType = isSubscribed
    ? 'subscribe'
    : 'unsubscribe';

  return (
    <div className="issue-detail-container">
      <MyToastContainer position="bottom" />
      <MyToastContainer position="top" />
      {/* Center column of info */}
      <div className="issue-detail">
        <div className="issue-topbar">
          <div className="issue-breadcrumb">
            {listContext ? (
              <>
                <Link className="breadcrumb-item" href={listContext.href}>
                  {listContext.title}
                </Link>
                <span className="breadcrumb-item">&rarr;</span>
              </>
            ) : null}
            <span className="breadcrumb-item">Issue {displayed.shortID}</span>
          </div>
          <CanEdit ownerID={displayed.creatorID}>
            <div className="edit-buttons">
              {!editing ? (
                <>
                  <Button
                    className="edit-button"
                    eventName="Edit issue"
                    onAction={() => setEditing(displayed)}
                  >
                    Edit
                  </Button>
                  <Button
                    className="delete-button"
                    eventName="Delete issue"
                    onAction={() => setDeleteConfirmationShown(true)}
                  >
                    Delete
                  </Button>
                </>
              ) : (
                <>
                  <Button
                    className="save-button"
                    eventName="Save issue edits"
                    onAction={save}
                    disabled={
                      !edits || edits.title === '' || edits.description === ''
                    }
                  >
                    Save
                  </Button>
                  <Button
                    className="cancel-button"
                    eventName="Cancel issue edits"
                    onAction={cancel}
                  >
                    Cancel
                  </Button>
                </>
              )}
            </div>
          </CanEdit>
        </div>

        <div ref={issueDescriptionRef}>
          {!editing ? (
            <h1 className="issue-detail-title">{rendering.title}</h1>
          ) : (
            <div className="edit-title-container">
              <p className="issue-detail-label">Edit title</p>
              <TextareaAutosize
                value={rendering.title}
                className="edit-title"
                autoFocus
                onChange={e => setEdits({...edits, title: e.target.value})}
                onKeyDown={e => isCtrlEnter(e) && save()}
                maxLength={MAX_ISSUE_TITLE_LENGTH}
              />
            </div>
          )}
          {/* These comments are actually github markdown which unfortunately has
           HTML mixed in. We need to find some way to render them, or convert to
           standard markdown? break-spaces makes it render a little better */}
          {!editing ? (
            <>
              <div className="description-container markdown-container">
                <Markdown>{rendering.description}</Markdown>
              </div>
              <EmojiPanel
                issueID={displayed.id}
                ref={issueEmojiRef}
                emojis={displayed.emoji}
                recentEmojis={recentEmojis}
                removeRecentEmoji={removeRecentEmoji}
              />
            </>
          ) : (
            <div className="edit-description-container">
              <p className="issue-detail-label">Edit description</p>
              <TextareaAutosize
                className="edit-description"
                value={rendering.description}
                onChange={e =>
                  setEdits({...edits, description: e.target.value})
                }
                onKeyDown={e => isCtrlEnter(e) && save()}
                maxLength={MAX_ISSUE_DESCRIPTION_LENGTH}
              />
            </div>
          )}
        </div>

        {/* Right sidebar */}
        <div className="issue-sidebar">
          <div className="sidebar-item">
            <p className="issue-detail-label">Status</p>
            <Combobox
              editable={false}
              disabled={!canEdit}
              items={[
                {
                  text: 'Open',
                  value: true,
                  icon: statusOpen,
                },
                {
                  text: 'Closed',
                  value: false,
                  icon: statusClosed,
                },
              ]}
              selectedValue={displayed.open}
              onChange={value =>
                z.mutate.issue.update({
                  id: displayed.id,
                  open: value,
                  modified: Date.now(),
                })
              }
            />
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Assignee</p>
            <UserPicker
              disabled={!canEdit}
              selected={{login: displayed.assignee?.login}}
              placeholder="Assign to..."
              unselectedLabel="Nobody"
              filter="crew"
              onSelect={user => {
                z.mutate.issue.update({
                  id: displayed.id,
                  assigneeID: user?.id ?? null,
                  modified: Date.now(),
                });
              }}
            />
          </div>

          {login.loginState?.decoded.role === 'crew' ? (
            <div className="sidebar-item">
              <p className="issue-detail-label">Visibility</p>
              <Combobox<'public' | 'internal'>
                editable={false}
                disabled={!canEdit}
                items={[
                  {
                    text: 'Public',
                    value: 'public',
                    icon: statusOpen,
                  },
                  {
                    text: 'Internal',
                    value: 'internal',
                    icon: statusClosed,
                  },
                ]}
                selectedValue={displayed.visibility}
                onChange={value =>
                  z.mutate.issue.update({
                    id: displayed.id,
                    visibility: value,
                    modified: Date.now(),
                  })
                }
              />
            </div>
          ) : null}

          <div className="sidebar-item">
            <p className="issue-detail-label">Notifications</p>
            <Combobox<NotificationType>
              disabled={!login.loginState?.decoded?.sub}
              items={[
                {
                  text: 'Subscribed',
                  value: 'subscribe',
                  icon: statusClosed,
                },
                {
                  text: 'Unsubscribed',
                  value: 'unsubscribe',
                  icon: circle,
                },
              ]}
              selectedValue={currentState}
              onChange={value =>
                z.mutate.notification.update({
                  issueID: displayed.id,
                  subscribed: value,
                  created: Date.now(),
                })
              }
            />
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Creator</p>
            <div className="issue-creator">
              <AvatarImage
                user={displayed.creator}
                className="issue-creator-avatar"
              />
              {displayed.creator.login}
            </div>
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Labels</p>
            <div className="issue-detail-label-container">
              {displayed.labels.map(label => (
                <span className="pill label" key={label.id}>
                  {label.name}
                </span>
              ))}
            </div>
            <CanEdit ownerID={displayed.creatorID}>
              <LabelPicker
                selected={labelSet}
                onAssociateLabel={labelID =>
                  z.mutate.issue.addLabel({
                    issueID: displayed.id,
                    labelID,
                  })
                }
                onDisassociateLabel={labelID =>
                  z.mutate.issue.removeLabel({
                    issueID: displayed.id,
                    labelID,
                  })
                }
                onCreateNewLabel={labelName => {
                  const labelID = nanoid();
                  z.mutate.label.createAndAddToIssue({
                    labelID,
                    labelName,
                    issueID: displayed.id,
                  });
                }}
              />
            </CanEdit>
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Last updated</p>
            <div className="timestamp-container">
              <RelativeTime timestamp={displayed.modified} />
            </div>
          </div>
        </div>

        <h2 className="issue-detail-label">Comments</h2>
        <Button
          className="show-older-comments"
          style={{
            visibility: hasOlderComments ? 'visible' : 'hidden',
          }}
          onAction={() => setDisplayAllComments(true)}
        >
          Show Older
        </Button>

        <div className="comments-container" ref={listRef}>
          <div
            className="virtual-list"
            style={{height: virtualizer.getTotalSize()}}
          >
            {virtualizer.getVirtualItems().map(item => (
              <div
                key={item.key as string}
                ref={virtualizer.measureElement}
                data-index={item.index}
                style={{
                  transform: `translateY(${
                    item.start - virtualizer.options.scrollMargin
                  }px)`,
                }}
              >
                <Comment
                  id={comments[item.index].id}
                  issueID={displayed.id}
                  comment={comments[item.index]}
                  height={item.size}
                  highlight={highlightedCommentID === comments[item.index].id}
                />
              </div>
            ))}
          </div>
        </div>

        {z.userID === 'anon' ? (
          <a href="/api/login/github" className="login-to-comment">
            Login to comment
          </a>
        ) : (
          <CommentComposer issueID={displayed.id} />
        )}
      </div>
      <Confirm
        isOpen={deleteConfirmationShown}
        title="Delete Issue"
        text="Really delete?"
        okButtonLabel="Delete"
        onClose={b => {
          if (b) {
            remove();
          }
          setDeleteConfirmationShown(false);
        }}
      />
    </div>
  );
}

const MyToastContainer = memo(({position}: {position: 'top' | 'bottom'}) => {
  return (
    <ToastContainer
      hideProgressBar={true}
      theme="dark"
      containerId={position}
      newestOnTop={position === 'bottom'}
      closeButton={false}
      position={`${position}-center`}
      closeOnClick={true}
      limit={3}
      // Auto close is broken. So we will manage it ourselves.
      autoClose={false}
    />
  );
});

// This cache is stored outside the state so that it can be used between renders.
const commentSizeCache = new LRUCache<string, number>(2000);

function maybeShowToastForEmoji(
  emoji: Emoji,
  issue: IssueRow & {readonly comments: readonly CommentRow[]},
  virtualizer: Virtualizer<Window, HTMLElement>,
  emojiElement: HTMLDivElement | null,
  setRecentEmojis: Dispatch<SetStateAction<Emoji[]>>,
) {
  const toastID = emoji.id;
  const {creator} = emoji;
  assert(creator);

  // We ony show toasts for emojis in the issue itself. Not for emojis in comments.
  if (emoji.subjectID !== issue.id || !emojiElement) {
    return;
  }

  // Determine if we should show a toast:
  // - at the top (the emoji is above the viewport)
  // - at the bottom (the emoji is below the viewport)
  // - no toast. Just the tooltip (which is always shown)
  let containerID: 'top' | 'bottom' | undefined;
  const rect = emojiElement.getBoundingClientRect();
  const {scrollRect} = virtualizer;
  if (scrollRect) {
    if (rect.bottom < 0) {
      containerID = 'top';
    } else if (rect.top > scrollRect.height) {
      containerID = 'bottom';
    }
  }

  if (containerID === undefined) {
    return;
  }

  toast(
    <ToastContent toastID={toastID}>
      <AvatarImage className="toast-avatar-icon" user={creator} />
      {creator.login + ' reacted on this issue: ' + emoji.value}
    </ToastContent>,
    {
      toastId: toastID,
      containerId: containerID,
      onClick: () => {
        // Put the emoji that was clicked first in the recent emojis list.
        // This is so that the emoji that was clicked first is the one that is
        // shown in the tooltip.
        setRecentEmojis(emojis => [
          emoji,
          ...emojis.filter(e => e.id !== emoji.id),
        ]);

        emojiElement?.scrollIntoView({
          block: 'end',
          behavior: 'smooth',
        });
      },
    },
  );
}

function ToastContent({
  children,
  toastID,
}: {
  children: ReactNode;
  toastID: string;
}) {
  const docFocused = useDocumentHasFocus();
  const [hover, setHover] = useState(false);

  useEffect(() => {
    if (docFocused && !hover) {
      const id = setTimeout(() => {
        toast.dismiss(toastID);
      }, emojiToastShowDuration);
      return () => clearTimeout(id);
    }
    return () => void 0;
  }, [docFocused, hover, toastID]);

  return (
    <div
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
    >
      {children}
    </div>
  );
}

const sampleSize = 100;
function average(numbers: number[]) {
  return numbers.reduce((a, b) => a + b, 0) / numbers.length;
}
function sample(bucket: number[], sample: number, size: number) {
  if (bucket.length < size) {
    bucket.push(sample);
    return;
  }

  bucket.shift();
  bucket.push(sample);
}

function useVirtualComments<T extends {id: string}>(comments: readonly T[]) {
  const listRef = useRef<HTMLDivElement | null>(null);

  const measurements = useRef<number[]>([200]);

  const virtualizer = useWindowVirtualizer({
    count: comments.length,
    estimateSize: index => {
      const {id} = comments[index];
      const cached = commentSizeCache.get(id);
      if (cached) {
        return cached;
      }
      return average(measurements.current);
    },
    overscan: 3,
    scrollMargin: listRef.current?.offsetTop ?? 0,
    measureElement: (el: HTMLElement) => {
      const height = el.offsetHeight;
      const {index} = el.dataset;
      if (index && height) {
        const {id} = comments[parseInt(index)];
        commentSizeCache.set(id, height);
      }
      sample(measurements.current, height, sampleSize);
      return height;
    },
    getItemKey: index => comments[index].id,
    gap: 16,
  });
  return {listRef, virtualizer};
}

function getScrollRestore(
  issueDescriptionElement: HTMLDivElement | null,
  virtualizer: Virtualizer<Window, HTMLElement>,
  comments: readonly {id: string}[],
): () => void {
  const getScrollHeight = () =>
    virtualizer.options.scrollMargin + virtualizer.getTotalSize();
  const getClientHeight = () => virtualizer.scrollRect?.height ?? 0;
  const getScrollTop = () => virtualizer.scrollOffset ?? 0;

  // If the issue description is visible we keep scroll as is.
  if (issueDescriptionElement && issueDescriptionElement.isConnected) {
    const rect = issueDescriptionElement.getBoundingClientRect();
    const inView = rect.bottom > 0 && rect.top < getClientHeight();
    if (inView) {
      return noop;
    }
  }

  // If almost at the bottom of the page, maintain the scrollBottom.
  const bottomMargin = 175;
  const scrollBottom = getScrollHeight() - getScrollTop() - getClientHeight();

  if (scrollBottom <= bottomMargin) {
    return () => {
      virtualizer.scrollToOffset(
        getScrollHeight() - getClientHeight() - scrollBottom,
      );
    };
  }

  // Npw we use the first comment that is visible in the viewport as the anchor.
  const scrollTop = getScrollTop();
  const topVirtualItem = virtualizer.getVirtualItemForOffset(scrollTop);
  if (topVirtualItem) {
    const top = topVirtualItem.start - scrollTop;
    const {key, index} = topVirtualItem;
    return () => {
      let newIndex = -1;
      // First search the virtual items for the comment.
      const newVirtualItem = virtualizer
        .getVirtualItems()
        .find(vi => vi.key === key);
      if (newVirtualItem) {
        newIndex = newVirtualItem.index;
      } else {
        // The comment is not in the virtual items. Let's try to find it in the
        // comments list
        newIndex = comments.findIndex(c => c.id === key);
        if (newIndex === -1) {
          // The comment was removed. Use the old index instead.
          newIndex = index;
        }
      }

      const offsetForIndex = virtualizer.getOffsetForIndex(newIndex, 'start');
      if (offsetForIndex === undefined) {
        return;
      }
      virtualizer.scrollToOffset(offsetForIndex[0] - top, {
        align: 'start',
      });
    };
  }

  return noop;
}

function noop() {
  // no op
}

type Issue = IssueRow & {
  readonly comments: readonly CommentRow[];
};

function useEmojiChangeListener(
  issue: Issue | undefined,
  cb: (added: readonly Emoji[], removed: readonly Emoji[]) => void,
) {
  const login = useLogin();
  const enabled = issue !== undefined;
  const issueID = issue?.id;
  const [emojis, result] = useQuery(
    emojiChange(login.loginState?.decoded, issueID ?? ''),
    {enabled},
  );

  const lastEmojis = useRef<Map<string, Emoji> | undefined>();

  useEffect(() => {
    const newEmojis = new Map(emojis.map(emoji => [emoji.id, emoji]));

    // First time we see the complete emojis for this issue.
    if (result.type === 'complete' && !lastEmojis.current) {
      lastEmojis.current = newEmojis;
      // First time should not trigger the callback.
      return;
    }

    if (lastEmojis.current) {
      const added: Emoji[] = [];
      const removed: Emoji[] = [];

      for (const [id, emoji] of newEmojis) {
        if (!lastEmojis.current.has(id)) {
          added.push(emoji);
        }
      }

      for (const [id, emoji] of lastEmojis.current) {
        if (!newEmojis.has(id)) {
          removed.push(emoji);
        }
      }

      if (added.length !== 0 || removed.length !== 0) {
        cb(added, removed);
      }

      lastEmojis.current = newEmojis;
    }
  }, [cb, emojis, issueID, result.type]);
}

function useShowToastForNewComment(
  comments:
    | ReadonlyArray<CommentRow & {readonly creator: UserRow | undefined}>
    | undefined,
  virtualizer: Virtualizer<Window, HTMLElement>,
  highlightComment: (id: string) => void,
) {
  // Keep track of the last comment IDs so we can compare them to the current
  // comment IDs and show a toast for new comments.
  const lastCommentIDs = useRef<Set<string> | undefined>();
  const {userID} = useZero();

  useEffect(() => {
    if (comments === undefined || comments.length === 0) {
      return;
    }
    if (lastCommentIDs.current === undefined) {
      lastCommentIDs.current = new Set(comments.map(c => c.id));
      return;
    }

    const currentCommentIDs = new Set(comments.map(c => c.id));

    const lCommentIDs = lastCommentIDs.current;
    const removedCommentIDs = difference(lCommentIDs, currentCommentIDs);

    const newCommentIDs = [];
    for (let i = comments.length - 1; i >= 0; i--) {
      const commentID = comments[i].id;
      if (lCommentIDs.has(commentID)) {
        break;
      }
      newCommentIDs.push(commentID);
    }

    for (const commentID of newCommentIDs) {
      const index = findLastIndex(comments, c => c.id === commentID);
      if (index === -1) {
        continue;
      }

      // Don't show a toast if the user is the one who posted the comment.
      const comment = comments[index];
      if (comment.creatorID === userID) {
        continue;
      }

      const scrollTop = virtualizer.scrollOffset ?? 0;
      const clientHeight = virtualizer.scrollRect?.height ?? 0;
      const isCommentBelowViewport =
        virtualizer.measurementsCache[index].start > scrollTop + clientHeight;

      if (!isCommentBelowViewport || !comment.creator) {
        continue;
      }

      toast(
        <ToastContent toastID={commentID}>
          <AvatarImage className="toast-avatar-icon" user={comment.creator} />
          {comment.creator?.login + ' posted a new comment'}
        </ToastContent>,

        {
          toastId: commentID,
          containerId: 'bottom',
          onClick: () => {
            highlightComment(comment.id);
          },
        },
      );
    }

    for (const commentID of removedCommentIDs) {
      toast.dismiss(commentID);
    }

    lastCommentIDs.current = currentCommentIDs;
  }, [comments, virtualizer, userID, highlightComment]);
}
