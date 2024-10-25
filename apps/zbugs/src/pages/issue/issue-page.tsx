import type {SchemaToRow, Zero} from '@rocicorp/zero';
import {useQuery} from '@rocicorp/zero/react';
import {nanoid} from 'nanoid';
import {useEffect, useMemo, useState} from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import {useParams} from 'wouter';
import {navigate, useHistoryState} from 'wouter/use-browser-location';
import {must} from '../../../../../packages/shared/src/must.js';
import statusClosed from '../../assets/icons/issue-closed.svg';
import statusOpen from '../../assets/icons/issue-open.svg';
import {Button} from '../../components/button.js';
import {CanEdit} from '../../components/can-edit.js';
import {Confirm} from '../../components/confirm.js';
import {EmojiPanel} from '../../components/emoji-panel.js';
import LabelPicker from '../../components/label-picker.js';
import {Link} from '../../components/link.js';
import Markdown from '../../components/markdown.js';
import RelativeTime from '../../components/relative-time.js';
import Selector from '../../components/selector.js';
import UserPicker from '../../components/user-picker.js';
import type {Schema} from '../../domain/schema.js';
import {useCanEdit} from '../../hooks/use-can-edit.js';
import {useKeypress} from '../../hooks/use-keypress.js';
import {useZero} from '../../hooks/use-zero.js';
import {links, type ListContext, type ZbugsHistoryState} from '../../routes.js';
import CommentComposer from './comment-composer.js';
import Comment from './comment.js';

export default function IssuePage() {
  const z = useZero();
  const params = useParams();

  const idStr = must(params.id);
  const idField = /[^\d]/.test(idStr) ? 'id' : 'shortID';
  const id = idField === 'shortID' ? parseInt(idStr) : idStr;

  const zbugsHistoryState = useHistoryState<ZbugsHistoryState | undefined>();
  const listContext = zbugsHistoryState?.zbugsListContext;
  // todo: one should be in the schema
  const q = z.query.issue
    .where(idField, id)
    .related('creator', creator => creator.one())
    .related('assignee', assignee => assignee.one())
    .related('labels')
    .related('viewState', q => q.where('userID', z.userID).one())
    .related('comments', q => q.orderBy('created', 'asc'))
    .one();
  const issue = useQuery(q);

  useEffect(() => {
    // only push viewed forward if the issue has been modified since the last viewing
    if (
      z.userID !== 'anon' &&
      issue &&
      issue.modified > (issue?.viewState?.viewed ?? 0)
    ) {
      // only set to viewed if the user has looked at it for > 1 second
      const handle = setTimeout(() => {
        z.mutate.viewState.set({
          issueID: issue.id,
          userID: z.userID,
          viewed: Date.now(),
        });
      }, 1000);
      return () => clearTimeout(handle);
    }
    return;
  }, [issue, z]);

  const [editing, setEditing] = useState<typeof issue | null>(null);
  const [edits, setEdits] = useState<Partial<typeof issue>>({});
  useEffect(() => {
    if (issue?.shortID !== undefined && idField !== 'shortID') {
      navigate(links.issue(issue), {
        replace: true,
        state: zbugsHistoryState,
      });
    }
  }, [issue, idField, zbugsHistoryState]);

  const save = () => {
    if (!editing) {
      return;
    }
    z.mutate.issue.update({id: editing.id, ...edits});
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
  const [issueSnapshot, setIssueSnapshot] = useState(issue);
  if (
    issue !== undefined &&
    (issueSnapshot === undefined || issueSnapshot.id !== issue.id)
  ) {
    setIssueSnapshot(issue);
  }
  const next = useQuery(
    buildListQuery(z, listContext, issue, 'next'),
    listContext !== undefined && issueSnapshot !== undefined,
  );
  useKeypress('j', () => {
    if (next) {
      navigate(links.issue(next), {state: zbugsHistoryState});
    }
  });

  const prev = useQuery(
    buildListQuery(z, listContext, issue, 'prev'),
    listContext !== undefined && issueSnapshot !== undefined,
  );
  useKeypress('k', () => {
    if (prev) {
      navigate(links.issue(prev), {state: zbugsHistoryState});
    }
  });

  const labelSet = useMemo(
    () => new Set(issue?.labels?.map(l => l.id)),
    [issue?.labels],
  );

  const [deleteConfirmationShown, setDeleteConfirmationShown] = useState(false);

  const canEdit = useCanEdit(issue?.creatorID);

  // TODO: We need the notion of the 'partial' result type to correctly render
  // a 404 here. We can't put the 404 here now because it would flash until we
  // get data.
  if (!issue) {
    return null;
  }

  const remove = () => {
    // TODO: Implement undo - https://github.com/rocicorp/undo
    z.mutate.issue.delete({id: issue.id});
    navigate(listContext?.href ?? links.home());
  };

  // TODO: This check goes away once Zero's consistency model is implemented.
  // The query above should not be able to return an incomplete result.
  if (!issue.creator) {
    return null;
  }

  const rendering = editing ? {...editing, ...edits} : issue;

  return (
    <div className="issue-detail-container">
      {/* Center column of info */}
      <div className="issue-detail">
        <div className="issue-topbar">
          <div className="issue-breadcrumb">
            {listContext ? (
              <>
                <Link
                  className="breadcrumb-item"
                  href={listContext.href}
                  state={{
                    zbugsListScrollOffset:
                      zbugsHistoryState?.zbugsListScrollOffset,
                  }}
                >
                  {listContext.title}
                </Link>
                <span className="breadcrumb-item">&rarr;</span>
              </>
            ) : null}
            <span className="breadcrumb-item">Issue {issue.shortID}</span>
          </div>
          <CanEdit ownerID={issue.creatorID}>
            <div className="edit-buttons">
              {!editing ? (
                <>
                  <Button
                    className="edit-button"
                    onAction={() => setEditing(issue)}
                  >
                    Edit
                  </Button>
                  <Button
                    className="delete-button"
                    onAction={() => setDeleteConfirmationShown(true)}
                  >
                    Delete
                  </Button>
                </>
              ) : (
                <>
                  <Button
                    className="save-button"
                    onAction={save}
                    disabled={
                      !edits || edits.title === '' || edits.description === ''
                    }
                  >
                    Save
                  </Button>
                  <Button className="cancel-button" onAction={cancel}>
                    Cancel
                  </Button>
                </>
              )}
            </div>
          </CanEdit>
        </div>

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
            />
          </div>
        )}
        {/* These comments are actually github markdown which unfortunately has
         HTML mixed in. We need to find some way to render them, or convert to
         standard markdown? break-spaces makes it render a little better */}
        {!editing ? (
          <div className="description-container markdown-container">
            <Markdown>{rendering.description}</Markdown>
            <EmojiPanel issueID={issue.id} />
          </div>
        ) : (
          <div className="edit-description-container">
            <p className="issue-detail-label">Edit description</p>
            <TextareaAutosize
              className="edit-description"
              value={rendering.description}
              onChange={e => setEdits({...edits, description: e.target.value})}
            />
          </div>
        )}

        {/* Right sidebar */}
        <div className="issue-sidebar">
          <div className="sidebar-item">
            <p className="issue-detail-label">Status</p>
            <Selector
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
              selectedValue={issue.open}
              onChange={value =>
                z.mutate.issue.update({id: issue.id, open: value})
              }
            />
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Creator</p>
            <Button className="sidebar-button issue-creator">
              <img
                src={issue.creator?.avatar}
                className="issue-creator-avatar"
                alt={issue.creator?.name}
              />
              {issue.creator.login}
            </Button>
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Assignee</p>
            <UserPicker
              disabled={!canEdit}
              selected={{login: issue.assignee?.login}}
              onSelect={user => {
                z.mutate.issue.update({id: issue.id, assigneeID: user.id});
              }}
            />
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Labels</p>
            <div className="issue-detail-label-container">
              {issue.labels.map(label => (
                <span className="pill label" key={label.id}>
                  {label.name}
                </span>
              ))}
            </div>
            <CanEdit ownerID={issue.creatorID}>
              <LabelPicker
                selected={labelSet}
                onAssociateLabel={labelID =>
                  z.mutate.issueLabel.create({
                    issueID: issue.id,
                    labelID,
                  })
                }
                onDisassociateLabel={labelID =>
                  z.mutate.issueLabel.delete({issueID: issue.id, labelID})
                }
                onCreateNewLabel={labelName => {
                  const labelID = nanoid();
                  z.mutate(tx => {
                    tx.label.create({id: labelID, name: labelName});
                    tx.issueLabel.create({issueID: issue.id, labelID});
                  });
                }}
              />
            </CanEdit>
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Last updated</p>
            <div className="timestamp-container">
              <RelativeTime timestamp={issue.modified} />
            </div>
          </div>
        </div>

        <h2 className="issue-detail-label">Comments</h2>
        {issue.comments.length > 0 ? (
          <div className="comments-container">
            {issue.comments.map(comment => (
              <Comment key={comment.id} id={comment.id} issueID={issue.id} />
            ))}
          </div>
        ) : null}
        {z.userID === 'anon' ? (
          <a href="/api/login/github" className="login-to-comment">
            Login to comment
          </a>
        ) : (
          <CommentComposer issueID={issue.id} />
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

function buildListQuery(
  z: Zero<Schema>,
  listContext: ListContext | undefined,
  issue: SchemaToRow<Schema['tables']['issue']> | undefined,
  dir: 'next' | 'prev',
) {
  if (!listContext || !issue) {
    return z.query.issue.one();
  }
  const {
    open,
    creatorID,
    assigneeID,
    labelIDs,
    textFilter,
    sortField,
    sortDirection,
  } = listContext.params;
  const orderByDir =
    dir === 'next' ? sortDirection : sortDirection === 'asc' ? 'desc' : 'asc';
  let q = z.query.issue
    .orderBy(sortField, orderByDir)
    .orderBy('id', orderByDir)
    .start(issue)
    .one();
  if (open !== undefined) {
    q = q.where('open', open);
  }

  if (creatorID) {
    q = q.where('creatorID', creatorID);
  }

  if (assigneeID) {
    q = q.where('assigneeID', assigneeID);
  }
  if (labelIDs) {
    for (const labelID of labelIDs) {
      q = q.where('labelIDs', 'LIKE', `%${labelID}%`);
    }
  }
  if (textFilter) {
    q = q.where('title', 'ILIKE', `%${textFilter}%`);
  }
  return q;
}
