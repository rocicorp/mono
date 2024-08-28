import {UndoManager} from '@rocicorp/undo';
import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {Zero} from 'zero-client';
import App from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import {Schema, schema} from './schema.js';
import {getIssuePreloadQuery} from './queries.js';

function init() {
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: 'anon',
    schemas: schema,
  });
  const undoManager = new UndoManager();

  const sorts = ['modified', 'created', 'priority', 'status'] as const;
  for (const sort of sorts) {
    const query = getIssuePreloadQuery(z, sort);
    query.preload();
  }

  function Home({
    zero,
    undoManager,
  }: {
    zero: Zero<Schema>;
    undoManager: UndoManager;
  }) {
    return (
      <div className="repliear">
        <ZeroProvider zero={zero}>
          <App undoManager={undoManager}></App>
        </ZeroProvider>
      </div>
    );
  }

  const root = must(document.getElementById('root'));
  createRoot(root).render(<Home zero={z} undoManager={undoManager} />);
}

init();
