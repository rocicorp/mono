import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {VirtuaTestApp} from './virtua-test-app.js';

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element not found');
}

createRoot(rootElement).render(
  <StrictMode>
    <VirtuaTestApp />
  </StrictMode>,
);
