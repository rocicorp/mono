import {render} from 'solid-js/web';
import {App} from './App.tsx';
import './index.css';
import {LoginProvider} from './login-provider.tsx';
import {ZeroInit} from './zero-init.tsx';

const root = document.getElementById('root');

if (!root) {
  throw new Error('Root element not found');
}

render(
  () => (
    <LoginProvider>
      <ZeroInit>
        <App />
      </ZeroInit>
    </LoginProvider>
  ),
  root,
);
