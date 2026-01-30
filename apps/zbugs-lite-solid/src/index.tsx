import {render} from 'solid-js/web';
import {App} from './App.tsx';
import {ZeroInit} from './zero-init.tsx';
import './index.css';

const root = document.getElementById('root');

if (!root) {
  throw new Error('Root element not found');
}

render(
  () => (
    <ZeroInit>
      <App />
    </ZeroInit>
  ),
  root,
);
