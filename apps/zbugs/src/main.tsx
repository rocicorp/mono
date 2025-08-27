import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import 'react-toastify/dist/ReactToastify.css';
import {must} from 'shared/src/must.js';
import {LoginProvider} from './components/login-provider.tsx';
import './index.css';
import {Root} from './root.tsx';
import {ZeroInit} from './zero-init.tsx';

createRoot(must(document.getElementById('root'))).render(
  <LoginProvider>
    <StrictMode>
      <ZeroInit>
        <Root />
      </ZeroInit>
    </StrictMode>
  </LoginProvider>,
);
