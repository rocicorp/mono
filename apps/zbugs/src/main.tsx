import './zero-setup.js';

import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import 'react-toastify/dist/ReactToastify.css';
import {must} from 'shared/src/must.js';
import {LoginProvider} from './hooks/use-login.js';
import './index.css';
import Root from './root.js';

createRoot(must(document.getElementById('root'))).render(
  <LoginProvider>
    <StrictMode>
      <Root />
    </StrictMode>
  </LoginProvider>,
);
