import {useQuery} from '@rocicorp/zero/react';
import {nanoid} from 'nanoid';
import {useEffect, useMemo, useState} from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import {useParams, useSearch} from 'wouter';
import {navigate} from 'wouter/use-browser-location';
import {must} from '../../../../../packages/shared/src/must.js';
import statusClosed from '../../assets/icons/issue-closed.svg';
import statusOpen from '../../assets/icons/issue-open.svg';
import LabelPicker from '../../components/label-picker.js';
import Markdown from '../../components/markdown.js';
import Selector from '../../components/selector.js';
import UserPicker from '../../components/user-picker.js';
import {useKeypress} from '../../hooks/use-keypress.js';
import {useZero} from '../../hooks/use-zero.js';
import {links} from '../../routes.js';
import CommentComposer from './comment-composer.js';
import Comment from './comment.js';

export default function IssuePage() {
  const z = useZero();
  const params = useParams();
  const qs = new URLSearchParams(useSearch());
  const idField: 'id' | 'shortID' = params.id ? 'shortID' : 'id';
  const id = params.id ?? must(qs.get('longID'));

  // todo: one should be in the schema
  const q = z.query.issue
    .where(idField, idField === 'shortID' ? parseInt(id) : id)
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
      history.replaceState(null, '', links.issue(issue));
    }
  }, [issue, idField]);

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

  const next = useQuery(
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    z.query.issue.orderBy('modified', 'desc').start(issue!).one(),
    issue !== undefined,
  );
  useKeypress('j', () => {
    if (next) {
      navigate(links.issue(next));
    }
  });

  const labelSet = useMemo(
    () => new Set(issue?.labels?.map(l => l.id)),
    [issue?.labels],
  );

  // TODO: We need the notion of the 'partial' result type to correctly render
  // a 404 here. We can't put the 404 here now because it would flash until we
  // get data.
  if (!issue) {
    return null;
  }

  const remove = () => {
    // TODO: Implement undo - https://github.com/rocicorp/undo
    if (confirm('Really delete?')) {
      z.mutate.issue.delete({id: issue.id});
    }
    navigate('/');
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
            <span className="breadcrumb-item">Open issues</span>
            <span className="breadcrumb-item">&rarr;</span>
            <span className="breadcrumb-item">ZB-15</span>
          </div>
          <div className="edit-buttons">
            {!editing ? (
              <>
                <button
                  className="edit-button"
                  onMouseDown={() => setEditing(issue)}
                >
                  Edit
                </button>
                <button className="delete-button" onMouseDown={() => remove()}>
                  Delete
                </button>
              </>
            ) : (
              <>
                <button className="save-button" onMouseDown={save}>
                  Save
                </button>
                <button className="cancel-button" onMouseDown={cancel}>
                  Cancel
                </button>
              </>
            )}
          </div>
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
            <button className="sidebar-button issue-creator">
              <img
                src={issue.creator?.avatar}
                className="issue-creator-avatar"
                alt={issue.creator?.name}
              />
              {issue.creator.login}
            </button>
          </div>

          <div className="sidebar-item">
            <p className="issue-detail-label">Assignee</p>
            <UserPicker
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
    </div>
  );
}
