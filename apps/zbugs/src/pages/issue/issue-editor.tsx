import {useEffect, useRef, useCallback, useState} from 'react';
import TextareaAutosize from 'react-textarea-autosize';
import {isCtrlEnter} from './is-ctrl-enter.ts';
import {
  MAX_ISSUE_DESCRIPTION_LENGTH,
  MAX_ISSUE_TITLE_LENGTH,
} from '../../limits.ts';

import * as Y from 'yjs';
import { useZero } from '../../hooks/use-zero.ts';
import { useQuery } from '@rocicorp/zero/react';

interface IssueEditorProps {
  issue: { id: string; title: string; description: string };
  onSave: (fields: { title: string; description: string }) => void;
  disabled?: boolean;
}

function base64ToUint8Array(base64: string) {
  const binaryString = atob(base64);
  const length = binaryString.length;
  const uint8Array = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    uint8Array[i] = binaryString.charCodeAt(i);
  }
  return uint8Array;
}

function uint8ArrayToBase64(uint8Array: Uint8Array) {
  const binaryString = String.fromCharCode(...uint8Array);
  return btoa(binaryString);
}

export default function IssueEditor({
  issue,
  onSave,
  disabled = false,
}: IssueEditorProps) {
  const [title, setTitle] = useState(issue.title);
  const [description, setDescription] = useState(issue.description);

  const z = useZero();
  const [document, documentResult] = useQuery(z.query.document.where('id', issue.id).one());

  const ydocRef = useRef<Y.Doc>();
  const yTitleRef = useRef<Y.Text>();
  const yDescriptionRef = useRef<Y.Text>();

  useEffect(() => {
    const ydoc = new Y.Doc();
    ydocRef.current = ydoc;

    ydoc.on('update', (update, origin) => {
      if (origin === 'ydoc') return;
      const updateString = uint8ArrayToBase64(update);
      z.mutate.document.applyUpdate({documentId: issue.id, update: updateString});
    });

    // Observe Title
    const yTitle = ydoc.getText('title');
    yTitleRef.current = yTitle;
    const updateTitle = () => setTitle(yTitle.toString());
    yTitle.observe(updateTitle);

    // Observe Description
    const yDescription = ydoc.getText('description');
    yDescriptionRef.current = yDescription;
    const updateDescription = () => setDescription(yDescription.toString());
    yDescription.observe(updateDescription);

    return () => {
      yTitle.unobserve(updateTitle);
      yDescription.unobserve(updateDescription);
      ydoc.destroy();
      ydocRef.current = undefined;
      yTitleRef.current = undefined;
      yDescriptionRef.current = undefined;
    };
  }, [issue.id]);

  useEffect(() => {
    const ydoc = ydocRef.current;
    if (!ydoc) return;

    if (!document && documentResult.type === 'complete') {
      const state = Y.encodeStateAsUpdate(ydoc);
      const stateString = uint8ArrayToBase64(state);
      z.mutate.document.insert({id: issue.id, snapshot: stateString});
      return;
    }

    // If the document is not loaded, don't do anything
    if (!document) return;

    // If the document is loaded, apply the latest snapshot
    const snapshot = base64ToUint8Array(document.snapshot);
    const currentState = Y.encodeStateAsUpdate(ydoc);
    const diff = Y.diffUpdate(snapshot, currentState);
    Y.applyUpdate(ydoc, diff);
  }, [document, documentResult.type]);

  // Update the text of a collaborative text field
  const setCollaborativeText = useCallback((newValue: string, yText?: Y.Text) => {
    if (!yText) return;
    if (yText.toString() === newValue) return;
    // Very naive implementation
    ydocRef.current?.transact(() => {
      yText.delete(0, yText.length);
      yText.insert(0, newValue);
    });
  }, []);

  const canSave = title.trim() !== '' && description.trim() !== '';

  const handleSave = () => {
    if (!canSave || disabled) return;
    onSave({ title, description });
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (isCtrlEnter(e)) {
      e.preventDefault();
      handleSave();
    }
  };

  return (
    <div className="issue-editor">
      <div className="edit-title-container">
        <p className="issue-detail-label">Edit title</p>
        <TextareaAutosize
          value={title}
          className="edit-title"
          autoFocus
          onChange={e => setCollaborativeText(e.target.value, yTitleRef.current)}
          onKeyDown={handleKeyDown}
          maxLength={MAX_ISSUE_TITLE_LENGTH}
          disabled={disabled}
        />
      </div>
      <div className="edit-description-container">
        <p className="issue-detail-label">Edit description</p>
        <TextareaAutosize
          className="edit-description"
          value={description}
          onChange={e => setCollaborativeText(e.target.value, yDescriptionRef.current)}
          onKeyDown={handleKeyDown}
          maxLength={MAX_ISSUE_DESCRIPTION_LENGTH}
          disabled={disabled}
        />
      </div>
    </div>
  );
} 