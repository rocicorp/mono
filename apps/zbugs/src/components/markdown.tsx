import {lazy, Suspense} from 'react';

const MarkdownInternal = lazy(() => import('./markdown-internal.js'));

export default function Markdown({children}: {children: string}) {
  return (
    <Suspense fallback={<div>{children}</div>}>
      <MarkdownInternal>{children}</MarkdownInternal>
    </Suspense>
  );
}
