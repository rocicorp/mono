import {StrictMode} from 'react';
import {createRoot} from 'react-dom/client';
import App from './App.tsx';
import './index.css';
import {z} from './zero.ts';

preload();

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);

function preload() {
  const sorts = ['modified', 'created', 'priority', 'status'] as const;
  for (const sort of sorts) {
    z.query.issue
      .related('labels')
      .related('comments', comments => comments.limit(20))
      .orderBy(sort, 'desc')
      .limit(1_000)
      .preload();
  }
}
