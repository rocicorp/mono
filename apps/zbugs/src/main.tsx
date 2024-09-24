import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {Zero} from 'zero-client';
import {ZeroProvider} from 'zero-react/src/use-zero.js';
import {Schema, schema} from './domain/schema-shared.js';
import './index.css';
import Root from './root.js';

const z = new Zero({
  logLevel: 'info',
  server: import.meta.env.VITE_PUBLIC_SERVER,
  userID: 'anon',
  schemas: schema,
});

z.query.user.preload();
z.query.label.preload();

z.query.issue
  .related('creator')
  .related('labels')
  .related('comments', c => c.limit(10).related('creator'))
  .orderBy('modified', 'desc')
  .preload();

// Exposed so we can mess around in the terminal and add/remove issues
(window as {z?: Zero<Schema>}).z = z;

createRoot(must(document.getElementById('root'))).render(
  <StrictMode>
    <ZeroProvider zero={z}>
      <Root />
    </ZeroProvider>
  </StrictMode>,
);
