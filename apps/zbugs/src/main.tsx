import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {Zero} from 'zero-client';
import {ZeroProvider} from 'zero-react/src/use-zero.js';
import {type Schema, schema} from './domain/schema.js';
import './index.css';
import Root from './root.js';

const qs = new URLSearchParams(location.search);
const hiddenTabDisconnectDelayMinutes = qs.get('keepalive') ? 60 : 5;
console.info(
  `Hidden tab disconnect delay: ${hiddenTabDisconnectDelayMinutes} minutes`,
);

const z = new Zero({
  logLevel: 'info',
  server: import.meta.env.VITE_PUBLIC_SERVER,
  userID: 'anon',
  schemas: schema,
  hiddenTabDisconnectDelay: hiddenTabDisconnectDelayMinutes * 60 * 1000,
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
