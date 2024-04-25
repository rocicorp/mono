import {UndoManager} from '@rocicorp/undo';
import {useEffect, useRef, useState} from 'react';
import {Zero} from 'zero-client';
import App, {Collections} from '../../frontend/app';
import {ZeroProvider} from '../../frontend/hooks/useZero';
import type {Comment, Issue, IssueLabel, Label} from '../../frontend/issue.js';
import {M, mutators} from '../../frontend/mutators';

export default function Home() {
  const [zero, setZero] = useState<Zero<M, Collections> | null>(null);
  const undoManagerRef = useRef(new UndoManager());
  useEffect(() => {
    // disabled eslint await requirement
    // eslint-disable-next-line
    (async () => {
      if (zero) {
        return;
      }

      const [, , spaceID] = location.pathname.split('/');
      const z = new Zero({
        server: process.env.NEXT_PUBLIC_SERVER,
        userID: 'anon',
        roomID: spaceID,
        mutators,
        kvStore: 'idb',
        queries: {
          issue: v => v as Issue,
          comment: v => v as Comment,
          label: v => v as Label,
          issueLabel: v => v as IssueLabel,
        },
      });

      setZero(z);
    })();
  }, [zero]);

  if (!zero) {
    return null;
  }
  return (
    <div className="repliear">
      <ZeroProvider zero={zero}>
        <App undoManager={undoManagerRef.current} />
      </ZeroProvider>
    </div>
  );
}
