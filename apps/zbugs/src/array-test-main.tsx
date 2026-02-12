import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {ArrayTestApp} from './array-test-app.tsx';

const rootElement = document.getElementById('root');
if (!rootElement) {
  throw new Error('Root element not found');
}

createRoot(rootElement).render(
  <StrictMode>
    <ArrayTestApp />
  </StrictMode>,
);
