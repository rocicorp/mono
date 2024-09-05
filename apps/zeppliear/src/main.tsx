import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {Zero} from 'zero-client';
import App from './app.jsx';
import {ZeroProvider} from './hooks/use-zero.jsx';
import './index.css';
import {getIssuePreloadQuery} from './queries.js';
import {Schema, schema} from './schema.js';

function init() {
  const z = new Zero({
    logLevel: 'info',
    server: import.meta.env.VITE_PUBLIC_SERVER,
    userID: 'anon',
    schemas: schema,
  });

  const sorts = ['modified', 'created', 'priority', 'status'] as const;
  for (const sort of sorts) {
    const query = getIssuePreloadQuery(z, sort);
    query.preload();
  }

  // Exposed so we can mess around in the terminal and add/remove issues
  (window as {z?: Zero<Schema>}).z = z;

  function Home({zero}: {zero: Zero<Schema>}) {
    return (
      <div>
        <ZeroProvider zero={zero}>
          <App />
        </ZeroProvider>
      </div>
    );
  }

  const root = must(document.getElementById('root'));
  createRoot(root).render(<Home zero={z} />);
}

init();
