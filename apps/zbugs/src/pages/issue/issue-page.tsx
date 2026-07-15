import type {Row} from '@rocicorp/zero';
import {useQuery, useZero} from '@rocicorp/zero/react';
import {nanoid} from 'nanoid';
import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type Dispatch,
  type SetStateAction,
} from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import {toast} from 'react-toastify';
import {useParams} from 'wouter';
import {navigate, useHistoryState} from 'wouter/use-browser-location';
import {assert} from '../../../../../packages/shared/src/asserts.js';
import {must} from '../../../../../packages/shared/src/must.ts';
import {mutators, type NotificationType} from '../../../shared/mutators.ts';
import {queries, type ListContextParams} from '../../../shared/queries.ts';
import circle from '../../assets/icons/circle.svg';
import statusClosed from '../../assets/icons/issue-closed.svg';
import statusOpen from '../../assets/icons/issue-open.svg';
import {parsePermalink} from '../../comment-permalink.ts';
import {AvatarImage} from '../../components/avatar-image.tsx';
import {Button} from '../../components/button.tsx';
import {CanEdit} from '../../components/can-edit.tsx';
import {Combobox} from '../../components/combobox.tsx';
import {Confirm} from '../../components/confirm.tsx';
import {EmojiPanel} from '../../components/emoji-panel.tsx';
import {
  ImageUploadArea,
  type TextAreaPatch,
} from '../../components/image-upload-area.tsx';
import {LabelPicker} from '../../components/label-picker.tsx';
import {Link} from '../../components/link.tsx';
import {Markdown} from '../../components/markdown.tsx';
import {RelativeTime} from '../../components/relative-time.tsx';
import {UserPicker} from '../../components/user-picker.tsx';
import {type Emoji} from '../../emoji-utils.ts';
import {useCanEdit} from '../../hooks/use-can-edit.ts';
import {useEmojiDataSourcePreload} from '../../hooks/use-emoji-data-source-preload.ts';
import {useHash} from '../../hooks/use-hash.ts';
import {useIsOffline} from '../../hooks/use-is-offline.ts';
import {useIsScrolling} from '../../hooks/use-is-scrolling.ts';
import {useKeypress} from '../../hooks/use-keypress.ts';
import {useLogin} from '../../hooks/use-login.tsx';
import {
  MAX_ISSUE_DESCRIPTION_LENGTH,
  MAX_ISSUE_TITLE_LENGTH,
} from '../../limits.ts';
import {recordPageLoad} from '../../page-load-stats.ts';
import {CACHE_NAV} from '../../query-cache-policy.ts';
import {links, useListContext, type ZbugsHistoryState} from '../../routes.tsx';
import {preload} from '../../zero-preload.ts';
import {CommentComposer} from './comment-composer.tsx';
import {Comments} from './comments.tsx';
import {getID} from './get-id.tsx';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {ToastContainer, ToastContent} from './toast-content.tsx';

function softNavigate(path: string, state?: ZbugsHistoryState) {
  navigate(path, {state});
  requestAnimationFrame(() => {
    window.scrollTo(0, 0);
  });
}

export function IssuePage({onReady}: {onReady: () => void}) {
  const z = useZero();
  const params = useParams();

  const {idField, idValue} = getID(params);
  const projectName = must(params.projectName);
  const login = useLogin();

  const isOffline = useIsOffline();

  const zbugsHistoryState = useHistoryState<ZbugsHistoryState | undefined>();
  const listContext = zbugsHistoryState?.zbugsListContext;

  const {setListContext} = useListContext();
  useEffect(() => {
    setListContext(listContext);
  }, [listContext, setListContext]);

  const [issue, issueResult] = useQuery(
    queries.issueDetail({idField, id: idValue}),
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

  useEffect(() => {
    document.title = 'Zero Bugs → ' + (displayed?.title ?? 'Issue Page');
  }, [displayed?.title]);

  useEffect(() => {
    if (issueResult.type === 'complete') {
      recordPageLoad('issue-page');
      preload(z, projectName);
    }
  }, [issueResult.type, z, projectName]);

  useEffect(() => {
    // only push viewed forward if the issue has been modified since the last viewing
    if (
      z.userID !== undefined &&
      displayed &&
      displayed.modified > (displayed?.viewState?.viewed ?? 0)
    ) {
      // only set to viewed if the user has looked at it for > 1 second
      const handle = setTimeout(() => {
        z.mutate(
          mutators.viewState.set({
            issueID: displayed.id,
            viewed: Date.now(),
          }),
        );
      }, 1000);
      return () => clearTimeout(handle);
    }
    return;
  }, [displayed, z]);

  const [editing, setEditing] = useState<typeof displayed | null>(null);
  const [edits, setEdits] = useState<Partial<typeof displayed>>({});
  const editDescriptionRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    if (
      displayed?.shortID !== null &&
      displayed !== undefined &&
      displayed.project !== undefined &&
      idField !== 'shortID'
    ) {
      navigate(
        links.issue({
          projectName: displayed.project.name,
          shortID: displayed.shortID,
          id: displayed.id,
        }),
        {
          replace: true,
          state: zbugsHistoryState,
        },
      );
    }
  }, [displayed, idField, zbugsHistoryState]);

  const save = () => {
    if (!editing) {
      return;
    }
    z.mutate(
      mutators.issue.update({id: editing.id, ...edits, modified: Date.now()}),
    );
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

  const listContextParams: ListContextParams = listContext?.params ?? {
    open: null,
    assignee: null,
    creator: null,
    labels: null,
    textFilter: null,
    sortField: 'modified',
    sortDirection: 'asc',
  };

  const [[next]] = useQuery(
    queries.issueListV2({
      listContext: listContextParams,
      limit: 1,
      start,
      dir: 'forward',
    }),
    prevNextOptions,
  );
  useKeypress('j', () => {
    if (next) {
      softNavigate(
        links.issue({projectName, shortID: next.shortID, id: next.id}),
        zbugsHistoryState,
      );
    }
  });

  const [[prev]] = useQuery(
    queries.issueListV2({
      listContext: listContextParams,
      limit: 1,
      start,
      dir: 'backward',
    }),
    prevNextOptions,
  );
  useKeypress('k', () => {
    if (prev) {
      softNavigate(
        links.issue({projectName, shortID: prev.shortID, id: prev.id}),
        zbugsHistoryState,
      );
    }
  });

  const labelSet = useMemo(
    () => new Set(displayed?.labels?.map(l => l.id)),
    [displayed?.labels],
  );

  // A `#comment-<id>` permalink: the comments virtualizer loads the window
  // around the target and scrolls it into view; the Comment highlights itself
  // by comparing the hash.
  const hash = useHash();
  const commentPermalinkID = parsePermalink(hash) ?? null;

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
    [displayed, recentEmojis, z.userID],
  );

  const removeRecentEmoji = useCallback((id: string) => {
    toast.dismiss(id);
    setRecentEmojis(recentEmojis => recentEmojis.filter(e => e.id !== id));
  }, []);

  const onInsert = (patch: TextAreaPatch) => {
    setEdits(prev => ({
      ...prev,
      description: patch.apply(prev?.description ?? ''),
    }));
  };

  useEmojiChangeListener(displayed, handleEmojiChange);
  useEmojiDataSourcePreload();

  if (!displayed && issueResult.type === 'complete') {
    return <NotFound></NotFound>;
  }

  if (!displayed) {
    return null;
  }

  const remove = async () => {
    // TODO: Implement undo - https://github.com/rocicorp/undo
    const result = z.mutate(mutators.issue.delete(displayed.id));

    // we wait for the client result to redirect to the list page
    const clientResult = await result.client;
    if (clientResult.type === 'error') {
      return;
    }
    navigate(listContext?.href ?? links.list({projectName}));
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
      <ToastContainer position="bottom" />
      <ToastContainer position="top" />
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

        <div>
          {!editing ? (
            <h1 className="issue-detail-title">{rendering.title}</h1>
          ) : (
            <div className="edit-title-container">
              <p className="issue-detail-label">Edit title</p>
              <TextareaAutosize
                disabled={isOffline}
                value={rendering.title}
                className="edit-title"
                autoFocus
                onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
                  setEdits({...edits, title: e.target.value})
                }
                onKeyDown={(e: React.KeyboardEvent<HTMLTextAreaElement>) =>
                  isCtrlEnter(e) && save()
                }
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
              <ImageUploadArea
                textAreaRef={editDescriptionRef}
                onInsert={onInsert}
              >
                <TextareaAutosize
                  disabled={isOffline}
                  className="edit-description"
                  value={rendering.description}
                  onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) =>
                    setEdits({...edits, description: e.target.value})
                  }
                  onKeyDown={(e: React.KeyboardEvent<HTMLTextAreaElement>) =>
                    isCtrlEnter(e) && save()
                  }
                  maxLength={MAX_ISSUE_DESCRIPTION_LENGTH}
                  ref={editDescriptionRef}
                />
              </ImageUploadArea>
            </div>
          )}
        </div>

        {/* Right sidebar */}
        <div className="issue-sidebar">
          <div className="sidebar-item">
            <p className="issue-detail-label">Status</p>
            <Combobox
              editable={false}
              disabled={!canEdit || isOffline}
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
                z.mutate(
                  mutators.issue.update({
                    id: displayed.id,
                    open: value,
                    modified: Date.now(),
                  }),
                )
              }
            />
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Assignee</p>
            <UserPicker
              projectName={projectName}
              disabled={!canEdit || isOffline}
              selected={{login: displayed.assignee?.login}}
              placeholder="Assign to..."
              unselectedLabel="Nobody"
              filter="crew"
              onSelect={user => {
                z.mutate(
                  mutators.issue.update({
                    id: displayed.id,
                    assigneeID: user?.id ?? null,
                    modified: Date.now(),
                  }),
                );
              }}
            />
          </div>

          {login.loginState?.decoded.role === 'crew' ? (
            <div className="sidebar-item">
              <p className="issue-detail-label">Visibility</p>
              <Combobox<'public' | 'internal'>
                editable={false}
                disabled={!canEdit || isOffline}
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
                  z.mutate(
                    mutators.issue.update({
                      id: displayed.id,
                      visibility: value,
                      modified: Date.now(),
                    }),
                  )
                }
              />
            </div>
          ) : null}

          <div className="sidebar-item">
            <p className="issue-detail-label">Notifications</p>
            <Combobox<NotificationType>
              disabled={!login.loginState?.decoded?.sub || isOffline}
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
                z.mutate(
                  mutators.notification.update({
                    issueID: displayed.id,
                    subscribed: value,
                    created: Date.now(),
                  }),
                )
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
                projectName={projectName}
                onAssociateLabel={labelID =>
                  z.mutate(
                    mutators.issue.addLabel({
                      issueID: displayed.id,
                      labelID,
                    }),
                  )
                }
                onDisassociateLabel={labelID =>
                  z.mutate(
                    mutators.issue.removeLabel({
                      issueID: displayed.id,
                      labelID,
                    }),
                  )
                }
                onCreateNewLabel={labelName => {
                  const labelID = nanoid();
                  z.mutate(
                    mutators.label.createAndAddToIssue({
                      labelID,
                      labelName,
                      issueID: displayed.id,
                    }),
                  );
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

        <Comments issueID={displayed.id} permalinkID={commentPermalinkID} />

        {z.userID === undefined ? (
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
            void remove();
          }
          setDeleteConfirmationShown(false);
        }}
      />
    </div>
  );
}

function NotFound() {
  return (
    <div>
      <div>
        <b>Error 404</b>
      </div>
      <div>zarro boogs found</div>
    </div>
  );
}

function maybeShowToastForEmoji(
  emoji: Emoji,
  issue: Row['issue'],
  emojiElement: HTMLDivElement | null,
  setRecentEmojis: Dispatch<SetStateAction<Emoji[]>>,
) {
  const toastID = emoji.id;
  const {creator} = emoji;
  assert(creator, 'Expected emoji creator to be defined');

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
  if (rect.bottom < 0) {
    containerID = 'top';
  } else if (rect.top > window.innerHeight) {
    containerID = 'bottom';
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

type Issue = NonNullable<Row<ReturnType<typeof queries.issueDetail>>>;

function useEmojiChangeListener(
  issue: Issue | undefined,
  cb: (added: readonly Emoji[], removed: readonly Emoji[]) => void,
) {
  const issueID = issue?.id;
  const [emojis, result] = useQuery(issueID && queries.emojiChange(issueID));

  const lastEmojis = useRef<Map<string, Emoji>>(undefined);

  useEffect(() => {
    if (!emojis) return;
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

export function IssueRedirect({onReady}: {onReady: () => void}) {
  const params = useParams();

  const {idField, idValue} = getID(params);

  const [issue, issueResult] = useQuery(
    queries.issueDetail({idField, id: idValue}),
    CACHE_NAV,
  );

  useEffect(() => {
    if (issue && issue.project) {
      navigate(
        links.issue({
          projectName: issue.project.name,
          shortID: issue.shortID,
          id: issue.id,
        }) + window.location.search,
      );
    }
  }, [issue]);

  if (!issue && issueResult.type === 'complete') {
    onReady();
    return <NotFound></NotFound>;
  }
  return null;
}
