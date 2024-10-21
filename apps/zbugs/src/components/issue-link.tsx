import {links, type ListContext} from '../routes.js';
import {Link, type Props as LinkProps} from './link.js';

export default function IssueLink({
  issue,
  title,
  children,
  className,
  listContext,
}: {
  issue: {id: string; shortID?: number | undefined};
  listContext: ListContext;
} & Omit<LinkProps<ListContext>, 'href' | 'state'>) {
  return (
    <Link
      href={links.issue(issue)}
      title={title}
      className={className}
      state={listContext}
    >
      {children}
    </Link>
  );
}
