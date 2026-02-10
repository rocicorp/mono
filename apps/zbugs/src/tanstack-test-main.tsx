import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {TanstackTestApp} from './tanstack-test-app.js';

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element not found');
}

createRoot(rootElement).render(
  <StrictMode>
    <TanstackTestApp />
  </StrictMode>,
);
