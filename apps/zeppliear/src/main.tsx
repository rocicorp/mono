import {UndoManager} from '@rocicorp/undo';
import {useEffect, useRef, useState} from 'react';
import ReactDOM from 'react-dom/client';
import {Zero} from 'zero-client';
import App, {Collections} from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import type {Comment, Issue, IssueLabel, Label, Member} from './issue.js';

async function init() {
  function Home() {
    const [zero, setZero] = useState<Zero<Collections> | null>(null);
    const undoManagerRef = useRef(new UndoManager());
    useEffect(() => {
      // disabled eslint await requirement
      // eslint-disable-next-line
      (async () => {
        if (zero) {
          return;
        }

        const z = new Zero({
          server: import.meta.env.VITE_PUBLIC_SERVER,
          userID: 'anon',
          kvStore: 'idb',
          queries: {
            issue: v => v as Issue,
            comment: v => v as Comment,
            label: v => v as Label,
            issueLabel: v => v as IssueLabel,
            member: v => v as Member,
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
  ReactDOM.createRoot(document.getElementById('root')!).render(<Home />);
}

await init();
