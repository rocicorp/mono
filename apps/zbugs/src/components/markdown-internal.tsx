import MarkdownBase from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import rehypeSanitize from 'rehype-sanitize';

/**
 * Do not import this component directly. Use `Markdown` instead.
 */
export default function Markdown({children}: {children: string}) {
  return (
    <MarkdownBase rehypePlugins={[rehypeRaw, rehypeSanitize]}>
      {children}
    </MarkdownBase>
  );
}
