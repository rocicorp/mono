import './zero-setup.js';
import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import {must} from 'shared/src/must.js';
import {LoginProvider} from './hooks/use-login.js';
import './index.css';
import Root from './root.js';

createRoot(must(document.getElementById('root'))).render(
  <StrictMode>
    <LoginProvider>
      <Root />
    </LoginProvider>
  </StrictMode>,
);
