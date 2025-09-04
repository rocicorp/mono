import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import 'react-toastify/dist/ReactToastify.css';
import {must} from 'shared/src/must.js';
import '../index.css';
import {ZeroInit} from '../zero-init.tsx';
import {DebugApp} from './debug-app.tsx';
import {LoginProvider} from '../components/login-provider.tsx';

createRoot(must(document.getElementById('debug-root'))).render(
  <LoginProvider>
    <StrictMode>
      <ZeroInit>
        <DebugApp />
      </ZeroInit>
    </StrictMode>
  </LoginProvider>,
);
