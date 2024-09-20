import MarkdownBase from 'react-markdown';
import rehypeRaw from 'rehype-raw';
import rehypeSanitize from 'rehype-sanitize';

export default function Markdown({children}: {children: string}) {
  return (
    <MarkdownBase rehypePlugins={[rehypeRaw, rehypeSanitize]}>
      {children}
    </MarkdownBase>
  );
}
