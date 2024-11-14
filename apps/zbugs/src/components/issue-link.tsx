import {links, type ListContext} from '../routes.js';
import {Link, type Props as LinkProps} from './link.js';

export default function IssueLink({
  issue,
  title,
  children,
  className,
  listContext,
  scrollOffset,
}: {
  issue: {id: string; shortID?: number | null};
  listContext: ListContext;
  scrollOffset: number;
} & Omit<LinkProps, 'href' | 'state'>) {
  return (
    <Link
      href={links.issue(issue)}
      title={title}
      className={className}
      state={{
        zbugsListContext: listContext,
        zbugsListScrollOffset: scrollOffset,
      }}
    >
      {children}
    </Link>
  );
}
