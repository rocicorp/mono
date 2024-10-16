import MarkdownBase from 'react-markdown';
import remarkGfm from 'remark-gfm';

/**
 * Do not import this component directly. Use `Markdown` instead.
 */
export default function Markdown({children}: {children: string}) {
  return <MarkdownBase rehypePlugins={[remarkGfm]}>{children}</MarkdownBase>;
}
