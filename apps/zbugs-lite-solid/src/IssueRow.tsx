import type {Row} from '@rocicorp/zero';
import {useZero} from '@rocicorp/zero/solid';
import {createEffect, createSignal, on, Show} from 'solid-js';
import {mutators, type schema} from './zero.ts';

type Issue = Row<typeof schema.tables.issue>;

export function IssueRow(props: {issue: Issue}) {
  // Track reactive updates to the issue (Solid components only run once,
  // but this effect runs each time issue data changes)
  const [updateCount, setUpdateCount] = createSignal(1);

  createEffect(
    on(
      () => [props.issue.title, props.issue.modified],
      () => {
        setUpdateCount((c) => c + 1);
        const id = props.issue.id.slice(0, 8);
        console.log(`[update #${updateCount()}] IssueRow: ${id} - "${props.issue.title.slice(0, 30)}"`);
      },
      {defer: true},
    ),
  );

  const z = useZero();
  const [editing, setEditing] = createSignal(false);
  const [editValue, setEditValue] = createSignal('');

  const formatDate = (timestamp: number) => {
    return new Date(timestamp).toLocaleDateString();
  };

  const startEditing = () => {
    setEditValue(props.issue.title);
    setEditing(true);
  };

  const saveEdit = () => {
    const newTitle = editValue().trim();
    if (newTitle && newTitle !== props.issue.title) {
      z().mutate(
        mutators.issue.update({
          id: props.issue.id,
          title: newTitle,
          modified: Date.now(),
        }),
      );
    }
    setEditing(false);
  };

  const handleKeyDown = (e: KeyboardEvent) => {
    if (e.key === 'Enter') {
      saveEdit();
    } else if (e.key === 'Escape') {
      setEditing(false);
    }
  };

  const deleteIssue = () => {
    z().mutate(mutators.issue.delete(props.issue.id));
  };

  return (
    <div class="issue-row">
      <span class="render-badge" title="Update count">{updateCount()}</span>
      <Show
        when={editing()}
        fallback={
          <span class="issue-title" onClick={startEditing} style={{cursor: 'pointer'}}>
            {props.issue.title}
          </span>
        }
      >
        <input
          class="issue-title-input"
          type="text"
          value={editValue()}
          onInput={(e) => setEditValue(e.currentTarget.value)}
          onBlur={saveEdit}
          onKeyDown={handleKeyDown}
          ref={(el) => setTimeout(() => el.focus(), 0)}
        />
      </Show>
      <span class="issue-modified">{formatDate(props.issue.modified)}</span>
      <button class="delete-button" onClick={deleteIssue} title="Delete issue">
        ×
      </button>
    </div>
  );
}
