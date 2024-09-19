import {useRoute} from 'wouter';
import {useZero} from '../../domain/schema.js';
import {useQuery} from 'zero-react/src/use-query.js';
import {useState} from 'react';
import TextareaAutosize from 'react-textarea-autosize';

export default function IssuePage() {
  const z = useZero();

  const [match, params] = useRoute('/issue/:id');

  // todo: one
  const issue = useQuery(
    match &&
      z.query.issue
        .where('id', params?.id ?? '')
        .related('creator')
        .related('labels')
        .related('comments', c => c.related('creator')),
  )[0];

  const [editing, setEditing] = useState<typeof issue | null>(null);
  const [edits, setEdits] = useState<Partial<typeof issue>>({});

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

  // TODO: We need the notion of the 'partial' result type to correctly render
  // a 404 here. We can't put the 404 here now because it would flash until we
  // get data.
  if (!issue) {
    return null;
  }

  const rendering = editing ? {...editing, ...edits} : issue;

  return (
    <div className="issue-detail-container">
      {/* Center column of info */}
      <div className="issue-detail">
        <div className="issue-breadcrumb">
          <span className="breadcrumb-item">Open issues</span>
          <span className="breadcrumb-item">&rarr;</span>
          <span className="breadcrumb-item">ZB-15</span>
        </div>
        {!editing ? (
          <h1>{rendering.title}</h1>
        ) : (
          <TextareaAutosize
            value={rendering.title}
            style={{color: 'black', width: '600px'}}
            onChange={e => setEdits({...edits, title: e.target.value})}
          />
        )}
        <div>
          {!editing ? (
            <button
              style={{border: '1px outset white'}}
              onMouseDown={() => setEditing(issue)}
            >
              Edit
            </button>
          ) : (
            <>
              <button style={{border: '1px outset white'}} onMouseDown={save}>
                Save
              </button>
              <button style={{border: '1px outset white'}} onMouseDown={cancel}>
                Cancel
              </button>
            </>
          )}
        </div>
        {/* These comments are actually github markdown which unfortunately has
         HTML mixed in. We need to find some way to render them, or convert to
         standard markdown? break-spaces makes it render a little better */}
        {!editing ? (
          <div style={{whiteSpace: 'break-spaces'}}>
            {rendering.description}
          </div>
        ) : (
          <TextareaAutosize
            style={{color: 'black', width: '600px'}}
            value={rendering.description}
            onChange={e => setEdits({...edits, description: e.target.value})}
          />
        )}
        {issue.comments.length > 0 ? (
          <div>
            <h2 style={{fontSize: '1.5em', marginTop: '1em'}}>Comments</h2>
            {issue.comments.map(comment => (
              <div key={comment.id} style={{marginBottom: '1em'}}>
                {comment.body} – {comment.creator[0].name}
              </div>
            ))}
          </div>
        ) : null}
      </div>

      {/* Right sidebar */}
      <div className="issue-sidebar">
        <p>
          <b className="mr-2">Creator:</b>
          {issue.creator[0].name}
        </p>
        <p>
          <b className="mr-2">Labels:</b>
          {issue.labels.map(label => (
            <span key={label.id}>{label.name}</span>
          ))}
        </p>
      </div>
    </div>
  );
}
